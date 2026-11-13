from concurrent.futures import ThreadPoolExecutor

from app.config import Config
from app.db import connect, get_db, run_sweep


def _flaky_pipeline(authed):
    """偶数编号管道是确定性抖动源，带失败行。"""
    items = authed.get("/api/pipelines").json()["items"]
    return next(p for p in items if p["id"] % 2 == 0)


def _healthy_pipeline(authed):
    items = authed.get("/api/pipelines").json()["items"]
    return next(p for p in items if p["id"] % 2 == 1)


def _set_max(authed, n, delay=0):
    r = authed.put("/api/retry/settings",
                   json={"max_auto_retries": n, "retry_delay_seconds": delay})
    assert r.status_code == 200
    assert r.json()["max_auto_retries"] == n


def _failed_rows(authed, pid):
    detail = authed.get(f"/api/pipelines/{pid}").json()
    return [r for r in detail["rows"] if r["state"] != "已成功"]


def _row(authed, pid, key):
    detail = authed.get(f"/api/pipelines/{pid}").json()
    return next(r for r in detail["rows"] if r["row_key"] == key)


# ---------- 全量抽取产生失败行，页面数据能分清队列与死信 ----------

def test_extract_fail_rows_enter_queue(authed):
    p = _flaky_pipeline(authed)
    r = authed.post(f"/api/pipelines/{p['id']}/extract")
    assert r.status_code == 200
    detail = r.json()
    assert detail["status"] == "抽取失败"
    failed = [x for x in detail["rows"] if x["state"] != "已成功"]
    assert {x["row_key"] for x in failed} == {"R02", "R05"}
    assert all(x["state"] == "队列中" for x in failed)
    # 队列与死信分得清：刚失败时全在队列，死信筐为空
    assert detail["counts"]["queue"] == 2
    assert detail["counts"]["dead"] == 0
    ov = authed.get("/api/retry/overview").json()
    assert ov["totals"]["queue"] == 2
    assert ov["totals"]["dead"] == 0


def test_healthy_pipeline_extract_clean(authed):
    p = _healthy_pipeline(authed)
    r = authed.post(f"/api/pipelines/{p['id']}/extract")
    assert r.json()["status"] == "已抽取待加载"
    assert all(x["state"] == "已成功" for x in r.json()["rows"])


# ---------- 自动重试次数：上限写在设置，测试钉死次数，满次停自动 ----------

def test_auto_retry_count_capped_and_stops(authed):
    p = _flaky_pipeline(authed)
    authed.post(f"/api/pipelines/{p['id']}/extract")
    _set_max(authed, 2, delay=0)  # 钉死：同一失败行自动最多 2 次

    first = authed.post("/api/retry/sweep").json()
    assert first["retried"] == 2
    # R02 只失败 1 次，第一轮自动重试即成功；R05 持续失败
    r02 = _row(authed, p["id"], "R02")
    r05 = _row(authed, p["id"], "R05")
    assert r02["state"] == "已成功"
    assert r05["state"] == "队列中" and r05["auto_retries"] == 1

    second = authed.post("/api/retry/sweep").json()
    r05 = _row(authed, p["id"], "R05")
    # 满次：自动停，转人工待点，不再被调度扫到
    assert r05["state"] == "人工待点"
    assert r05["mode"] == "manual"
    assert r05["auto_retries"] == 2
    third = authed.post("/api/retry/sweep").json()
    assert third["retried"] == 0
    assert _row(authed, p["id"], "R05")["state"] == "人工待点"
    detail = authed.get(f"/api/pipelines/{p['id']}").json()
    assert any(e["kind"] == "auto_stop" for e in detail["events"])
    assert second["stopped"] == 1


# ---------- 死信与人工触发：满次后按出路分流，出路当日锁定 ----------

def test_manual_retry_exit_to_dead(authed):
    p = _flaky_pipeline(authed)
    authed.post(f"/api/pipelines/{p['id']}/extract")
    _set_max(authed, 1, delay=0)
    authed.post("/api/retry/sweep")  # R05 打满 1 次自动 -> 人工待点
    r05 = _row(authed, p["id"], "R05")
    assert r05["state"] == "人工待点"

    # 满次行人工触发须带出路
    bad = authed.post(f"/api/retry/rows/{r05['id']}/retry", json={})
    assert bad.status_code == 400
    r = authed.post(f"/api/retry/rows/{r05['id']}/retry", json={"exit_path": "dead"})
    assert r.status_code == 200
    out = r.json()
    assert out["state"] == "死信"
    assert out["exit_path"] == "dead"
    assert out["exit_locked_day"]
    assert "转入死信并停止自动重试" in out["exit_note"]
    ov = authed.get("/api/retry/overview").json()
    assert ov["totals"]["dead"] == 1
    detail = authed.get(f"/api/pipelines/{p['id']}").json()
    assert any(e["kind"] == "to_dead" for e in detail["events"])


def test_manual_retry_exit_keep_manual(authed):
    p = _flaky_pipeline(authed)
    authed.post(f"/api/pipelines/{p['id']}/extract")
    _set_max(authed, 1, delay=0)
    authed.post("/api/retry/sweep")
    r05 = _row(authed, p["id"], "R05")
    r = authed.post(f"/api/retry/rows/{r05['id']}/retry", json={"exit_path": "manual"})
    out = r.json()
    assert out["state"] == "人工待点"
    assert out["mode"] == "manual"
    assert out["exit_path"] == "manual"
    assert "只能人工触发" in out["exit_note"]
    # 留在队列：自动调度不会再碰它
    assert authed.post("/api/retry/sweep").json()["retried"] == 0
    ov = authed.get("/api/retry/overview").json()
    assert ov["totals"]["manual"] == 1
    assert ov["totals"]["dead"] == 0


def test_exit_path_skew_rejected_same_day(authed):
    p = _flaky_pipeline(authed)
    authed.post(f"/api/pipelines/{p['id']}/extract")
    _set_max(authed, 1, delay=0)
    authed.post("/api/retry/sweep")
    r05 = _row(authed, p["id"], "R05")
    authed.post(f"/api/retry/rows/{r05['id']}/retry", json={"exit_path": "manual"})
    r05 = _row(authed, p["id"], "R05")
    # 当日出路已锁定为 manual，再点成 dead -> 出路写歪了
    skew = authed.post(f"/api/retry/rows/{r05['id']}/retry", json={"exit_path": "dead"})
    assert skew.status_code == 400
    assert "出路写歪" in skew.json()["detail"]
    # 歪的请求没有改变行状态
    assert _row(authed, p["id"], "R05")["exit_path"] == "manual"
    detail = authed.get(f"/api/pipelines/{p['id']}").json()
    assert any(e["kind"] == "exit_skew" for e in detail["events"])
    # 与锁定出路一致仍可继续人工触发
    ok = authed.post(f"/api/retry/rows/{r05['id']}/retry", json={"exit_path": "manual"})
    assert ok.status_code == 200


def test_invalid_exit_path_rejected(authed):
    p = _flaky_pipeline(authed)
    authed.post(f"/api/pipelines/{p['id']}/extract")
    r02 = _row(authed, p["id"], "R02")
    r = authed.post(f"/api/retry/rows/{r02['id']}/retry", json={"exit_path": "nowhere"})
    assert r.status_code == 400
    assert "出路写歪" in r.json()["detail"]


# ---------- 并发争抢：两人同刻点同一行只成一笔 ----------

def test_atomic_claim_only_one_wins(authed):
    """原子认领守卫：同一行只能被认一次，已在重试中时第二次认领落空。"""
    p = _flaky_pipeline(authed)
    authed.post(f"/api/pipelines/{p['id']}/extract")
    r02 = _row(authed, p["id"], "R02")
    rid = r02["id"]

    from app.db import _claim_row, ROW_RETRYING, connect as _connect

    conn = _connect()
    try:
        a = _claim_row(conn, rid, "user-a")
        assert a is not None
        # 同刻第二人再认：WHERE 守卫拦住，只成一笔
        b = _claim_row(conn, rid, "user-b")
        assert b is None
        held = conn.execute(
            "SELECT state, claim_token FROM extract_rows WHERE id = ?", (rid,)
        ).fetchone()
        assert held["state"] == ROW_RETRYING
        assert held["claim_token"] == "user-a"
        conn.commit()
    finally:
        conn.close()


def test_concurrent_manual_retry_only_one_wins(authed):
    """HTTP 级：第一笔把行置为重试中在途，第二笔同刻点击被打回，只留一条争抢记录。"""
    p = _flaky_pipeline(authed)
    authed.post(f"/api/pipelines/{p['id']}/extract")
    r02 = _row(authed, p["id"], "R02")
    rid = r02["id"]

    def fire():
        return authed.post(f"/api/retry/rows/{rid}/retry", json={})

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(fire)
        r1 = first.result()
        # r1 已完成后行已离开重试中；用独立连接模拟另一人在途时点同一行
        with get_db() as conn:
            conn.execute(
                "UPDATE extract_rows SET state = '重试中', claim_token = 'inflight' WHERE id = ?",
                (rid,),
            )
        r2 = pool.submit(fire).result()
    assert r1.status_code == 200
    assert r2.status_code == 409
    assert "争抢只成一笔" in r2.json()["detail"]
    with get_db() as conn:
        conn.execute(
            "UPDATE extract_rows SET state = '队列中', claim_token = NULL WHERE id = ?", (rid,)
        )
    detail = authed.get(f"/api/pipelines/{p['id']}").json()
    contentions = [e for e in detail["events"] if e["kind"] == "contention"]
    assert len(contentions) == 1
    assert "争抢" in contentions[0]["detail"]


# ---------- 死信回炉须填原因，回炉后仅人工触发 ----------

def test_dead_row_requeue_requires_reason(authed):
    p = _flaky_pipeline(authed)
    authed.post(f"/api/pipelines/{p['id']}/extract")
    _set_max(authed, 1, delay=0)
    authed.post("/api/retry/sweep")
    r05 = _row(authed, p["id"], "R05")
    authed.post(f"/api/retry/rows/{r05['id']}/retry", json={"exit_path": "dead"})
    dead = _row(authed, p["id"], "R05")
    assert dead["state"] == "死信"

    no_reason = authed.post(f"/api/retry/rows/{dead['id']}/requeue", json={"reason": "  "})
    assert no_reason.status_code == 400
    assert "原因" in no_reason.json()["detail"]

    ok = authed.post(f"/api/retry/rows/{dead['id']}/requeue",
                     json={"reason": "源端已恢复，人工复核回炉"})
    out = ok.json()
    assert out["state"] == "队列中"
    assert out["mode"] == "manual"          # 回炉后只能人工触发
    assert out["auto_retries"] == 0
    assert out["dead_reason"] == "源端已恢复，人工复核回炉"
    # 自动调度不扫 manual 行
    assert authed.post("/api/retry/sweep").json()["retried"] == 0
    # 人工再点，R05 仍持续失败，须重新给出路
    again = authed.post(f"/api/retry/rows/{out['id']}/retry", json={"exit_path": "dead"})
    assert again.json()["state"] == "死信"


# ---------- 全量抽取进行中禁止单行重试 ----------

def test_single_row_retry_blocked_during_full_extract(authed):
    p = _flaky_pipeline(authed)
    authed.post(f"/api/pipelines/{p['id']}/extract")
    r02 = _row(authed, p["id"], "R02")
    # 直接置全量进行中标记，模拟全量抽取未结束窗口
    with get_db() as conn:
        conn.execute("UPDATE pipelines SET full_running = 1 WHERE id = ?", (p["id"],))
    blocked = authed.post(f"/api/retry/rows/{r02['id']}/retry", json={})
    assert blocked.status_code == 409
    assert "全量抽取" in blocked.json()["detail"]
    detail = authed.get(f"/api/pipelines/{p['id']}").json()
    assert any(e["kind"] == "blocked_full" for e in detail["events"])
    # 全量结束后恢复可点
    with get_db() as conn:
        conn.execute("UPDATE pipelines SET full_running = 0 WHERE id = ?", (p["id"],))
    ok = authed.post(f"/api/retry/rows/{r02['id']}/retry", json={})
    assert ok.status_code == 200


# ---------- 总览统计与页面可读 ----------

def test_overview_totals_and_spa_renders_sections(authed):
    p = _flaky_pipeline(authed)
    authed.post(f"/api/pipelines/{p['id']}/extract")
    _set_max(authed, 1, delay=0)
    authed.post("/api/retry/sweep")  # R02 成功，R05 人工待点
    ov = authed.get("/api/retry/overview").json()
    assert ov["totals"]["retrying"] == 0
    assert ov["totals"]["manual"] == 1
    assert ov["totals"]["success"] >= 1
    pipe_bucket = next(b for b in ov["pipelines"] if b["pipeline_id"] == p["id"])
    assert pipe_bucket["manual"] == 1

    html = authed.get("/app/").text
    assert "重试队列" in html
    assert "死信筐" in html
    assert "重试设置" in html
    assert "失败摘要" in html and "争抢记录" in html
    # 统计磁贴文案由 Vue 客户端渲染，落在 app.js
    js = authed.get("/static/js/app.js").text
    assert "重试中" in js and "死信数" in js and "人工待点" in js


def test_settings_validation(authed):
    r = authed.put("/api/retry/settings",
                   json={"max_auto_retries": -1, "retry_delay_seconds": 0})
    assert r.status_code == 400
    r2 = authed.put("/api/retry/settings",
                    json={"max_auto_retries": "x", "retry_delay_seconds": 0})
    assert r2.status_code == 400


def test_retry_settings_persisted_in_detail(authed):
    _set_max(authed, 5, delay=3)
    p = _flaky_pipeline(authed)
    detail = authed.get(f"/api/pipelines/{p['id']}").json()
    assert detail["settings"]["max_auto_retries"] == 5
    assert detail["settings"]["retry_delay_seconds"] == 3


def test_manual_retry_before_cap_stays_auto(authed):
    # 自动额度未满时人工点试：不占自动次数，回 auto 队列
    p = _flaky_pipeline(authed)
    authed.post(f"/api/pipelines/{p['id']}/extract")
    _set_max(authed, 3, delay=0)
    r02 = _row(authed, p["id"], "R02")
    r = authed.post(f"/api/retry/rows/{r02['id']}/retry", json={})
    out = r.json()
    assert out["state"] == "已成功"  # R02 第二次尝试即成功
    assert out["auto_retries"] == 0


def test_dead_row_cannot_retry_directly(authed):
    p = _flaky_pipeline(authed)
    authed.post(f"/api/pipelines/{p['id']}/extract")
    _set_max(authed, 1, delay=0)
    authed.post("/api/retry/sweep")
    r05 = _row(authed, p["id"], "R05")
    authed.post(f"/api/retry/rows/{r05['id']}/retry", json={"exit_path": "dead"})
    dead = _row(authed, p["id"], "R05")
    r = authed.post(f"/api/retry/rows/{dead['id']}/retry", json={"exit_path": "dead"})
    assert r.status_code == 400
    assert "回炉" in r.json()["detail"]
