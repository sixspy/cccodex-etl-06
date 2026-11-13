import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

from app.config import Config

SEED_PIPELINES = [
    ("订单明细日抽", "orders_daily", "待抽取", "mock://orders/day"),
    ("用户画像宽表", "user_profile", "待抽取", "mock://users/profile"),
    ("库存快照落地", "inventory_snap", "待抽取", "mock://stock/snap"),
    ("支付流水清洗", "pay_clean", "待抽取", "mock://pay/flow"),
]

# 失败行状态机
ROW_QUEUE = "队列中"        # 在重试队列，自动模式下可被调度扫到
ROW_RETRYING = "重试中"     # 已被认领，重试进行中
ROW_MANUAL = "人工待点"     # 自动次数满，停自动，只能人工触发
ROW_DEAD = "死信"           # 死信筐
ROW_SUCCESS = "已成功"

MODE_AUTO = "auto"
MODE_MANUAL = "manual"

EXIT_DEAD = "dead"          # 满次出路：转入死信并停止自动重试
EXIT_MANUAL = "manual"      # 满次出路：留在队列改为只能人工触发
EXIT_PATHS = (EXIT_DEAD, EXIT_MANUAL)

DEFAULT_MAX_AUTO_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 10

# 全量抽取每个管道产生的 mock 行；抖动源的管道里这两行先失败
MOCK_ROW_KEYS = ["R01", "R02", "R03", "R04", "R05", "R06"]
# row_key -> 连续失败次数（耗尽后成功）；R05 始终失败，用于打满自动次数
FLAKY_FAIL_TIMES = {"R02": 1, "R05": 99}

EVENT_LABELS = {
    "extract_fail": "抽取失败",
    "auto_retry": "自动重试失败",
    "retry_ok": "重试成功",
    "manual_retry": "人工重试",
    "auto_stop": "满次停自动",
    "to_dead": "转入死信",
    "to_manual": "转人工触发",
    "requeue": "死信回炉",
    "contention": "重试争抢",
    "exit_skew": "出路写歪",
    "blocked_full": "全量拦截",
}


class RetryError(Exception):
    """领域错误：携带 HTTP 状态码与面向前端的中文说明。"""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or Config.database()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


@contextmanager
def get_db(db_path: str | None = None):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str | None = None) -> None:
    with get_db(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pipelines (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title TEXT NOT NULL,
              code TEXT NOT NULL UNIQUE,
              status TEXT NOT NULL,
              source_uri TEXT NOT NULL,
              extracted INTEGER NOT NULL DEFAULT 0,
              loaded INTEGER NOT NULL DEFAULT 0,
              full_running INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS extract_rows (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pipeline_id INTEGER NOT NULL REFERENCES pipelines(id),
              row_key TEXT NOT NULL,
              source_ref TEXT NOT NULL,
              state TEXT NOT NULL,
              mode TEXT NOT NULL DEFAULT 'auto',
              attempts INTEGER NOT NULL DEFAULT 0,
              auto_retries INTEGER NOT NULL DEFAULT 0,
              fail_times INTEGER NOT NULL DEFAULT 0,
              last_error TEXT,
              dead_reason TEXT,
              next_retry_at TEXT,
              claim_token TEXT,
              exit_path TEXT,
              exit_note TEXT,
              exit_locked_day TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS retry_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pipeline_id INTEGER NOT NULL,
              row_id INTEGER,
              row_key TEXT,
              kind TEXT NOT NULL,
              actor TEXT NOT NULL,
              detail TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_rows_pipeline ON extract_rows(pipeline_id);
            CREATE INDEX IF NOT EXISTS idx_rows_state ON extract_rows(state);
            CREATE INDEX IF NOT EXISTS idx_events_pipeline ON retry_events(pipeline_id);
            """
        )
        self_init = conn.execute(
            "SELECT name FROM pragma_table_info('pipelines') WHERE name='full_running'"
        ).fetchone()
        if not self_init:
            conn.execute("ALTER TABLE pipelines ADD COLUMN full_running INTEGER NOT NULL DEFAULT 0")
        # 上次进程残留的全量标记，启动时清掉，避免永久拦住单行重试
        conn.execute("UPDATE pipelines SET full_running = 0")
        n = conn.execute("SELECT COUNT(*) AS c FROM pipelines").fetchone()["c"]
        if n == 0:
            conn.executemany(
                "INSERT INTO pipelines (title, code, status, source_uri) VALUES (?,?,?,?)",
                SEED_PIPELINES,
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('lab_name', ?)",
                (Config.LAB_NAME,),
            )
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('max_auto_retries', ?)",
            (str(DEFAULT_MAX_AUTO_RETRIES),),
        )
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('retry_delay_seconds', ?)",
            (str(DEFAULT_RETRY_DELAY_SECONDS),),
        )


# ---------------- 设置 ----------------

def get_settings(conn: sqlite3.Connection) -> dict:
    rows = conn.execute("SELECT key, value FROM meta").fetchall()
    meta = {r["key"]: r["value"] for r in rows}
    return {
        "max_auto_retries": int(meta.get("max_auto_retries", DEFAULT_MAX_AUTO_RETRIES)),
        "retry_delay_seconds": int(meta.get("retry_delay_seconds", DEFAULT_RETRY_DELAY_SECONDS)),
    }


def save_settings(conn: sqlite3.Connection, max_auto_retries: int, retry_delay_seconds: int) -> dict:
    if max_auto_retries < 0 or retry_delay_seconds < 0:
        raise RetryError(400, "重试次数上限与间隔都不能为负数")
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('max_auto_retries', ?)",
        (str(max_auto_retries),),
    )
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('retry_delay_seconds', ?)",
        (str(retry_delay_seconds),),
    )
    return get_settings(conn)


# ---------------- 事件 ----------------

def _log_event(
    conn: sqlite3.Connection,
    kind: str,
    pipeline_id: int,
    row: sqlite3.Row | dict | None = None,
    detail: str = "",
    actor: str = "scheduler",
) -> None:
    row_id = row["id"] if row is not None else None
    row_key = row["row_key"] if row is not None else None
    conn.execute(
        "INSERT INTO retry_events (pipeline_id, row_id, row_key, kind, actor, detail, created_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (pipeline_id, row_id, row_key, kind, actor, detail or EVENT_LABELS.get(kind, kind), _fmt(_now())),
    )


def list_events(conn: sqlite3.Connection, pipeline_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT id, pipeline_id, row_id, row_key, kind, actor, detail, created_at "
        "FROM retry_events WHERE pipeline_id = ? ORDER BY id DESC LIMIT 200",
        (pipeline_id,),
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------- 管道 ----------------

def list_pipelines(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT id, title, code, status, source_uri, extracted, loaded, full_running, created_at "
        "FROM pipelines ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def _pipeline_row_factory(row: sqlite3.Row) -> dict:
    item = dict(row)
    item["full_running"] = bool(item.get("full_running", 0))
    return item


def get_pipeline(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    row = conn.execute(
        "SELECT id, title, code, status, source_uri, extracted, loaded, full_running, created_at "
        "FROM pipelines WHERE id = ?",
        (pipeline_id,),
    ).fetchone()
    if not row:
        return None
    item = _pipeline_row_factory(row)
    item["rows"] = list_rows(conn, pipeline_id)
    item["events"] = list_events(conn, pipeline_id)
    item["settings"] = get_settings(conn)
    item["counts"] = _pipeline_counts(conn, pipeline_id)
    return item


def list_rows(conn: sqlite3.Connection, pipeline_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM extract_rows WHERE pipeline_id = ? ORDER BY row_key",
        (pipeline_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _pipeline_counts(conn: sqlite3.Connection, pipeline_id: int) -> dict:
    rows = conn.execute(
        "SELECT state, COUNT(*) AS c FROM extract_rows WHERE pipeline_id = ? GROUP BY state",
        (pipeline_id,),
    ).fetchall()
    counts = {r["state"]: r["c"] for r in rows}
    return {
        "queue": counts.get(ROW_QUEUE, 0),
        "retrying": counts.get(ROW_RETRYING, 0),
        "manual": counts.get(ROW_MANUAL, 0),
        "dead": counts.get(ROW_DEAD, 0),
        "success": counts.get(ROW_SUCCESS, 0),
    }


def _refresh_pipeline_status(conn: sqlite3.Connection, pipeline_id: int) -> None:
    """失败行全部救回 -> 已抽取待加载；仍有未成功失败行 -> 抽取失败。"""
    pipe = conn.execute(
        "SELECT status FROM pipelines WHERE id = ?", (pipeline_id,)
    ).fetchone()
    if not pipe:
        return
    stuck = conn.execute(
        "SELECT COUNT(*) AS c FROM extract_rows "
        "WHERE pipeline_id = ? AND state IN (?, ?, ?, ?)",
        (pipeline_id, ROW_QUEUE, ROW_RETRYING, ROW_MANUAL, ROW_DEAD),
    ).fetchone()["c"]
    if stuck == 0 and pipe["status"] == "抽取失败":
        conn.execute("UPDATE pipelines SET status = ? WHERE id = ?", ("已抽取待加载", pipeline_id))
    elif stuck > 0 and pipe["status"] != "抽取失败":
        conn.execute(
            "UPDATE pipelines SET status = ?, loaded = 0 WHERE id = ?",
            ("抽取失败", pipeline_id),
        )


def _is_flaky(pipeline_id: int) -> bool:
    """确定性抖动源：偶数编号管道带失败行，奇数编号管道全程健康。"""
    return pipeline_id % 2 == 0


def run_extract(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    item = conn.execute("SELECT * FROM pipelines WHERE id = ?", (pipeline_id,)).fetchone()
    if not item:
        return None
    if item["status"] not in ("待抽取", "抽取失败"):
        return get_pipeline(conn, pipeline_id)
    # 全量抽取进行中：单行重试在此期间一律禁止
    conn.execute("UPDATE pipelines SET full_running = 1 WHERE id = ?", (pipeline_id,))
    now = _fmt(_now())
    # 新一轮全量抽取：清掉上一轮的失败行与事件
    conn.execute("DELETE FROM retry_events WHERE pipeline_id = ?", (pipeline_id,))
    conn.execute("DELETE FROM extract_rows WHERE pipeline_id = ?", (pipeline_id,))
    flaky = _is_flaky(pipeline_id)
    failed = 0
    for key in MOCK_ROW_KEYS:
        total_fails = FLAKY_FAIL_TIMES.get(key, 0) if flaky else 0
        if total_fails > 0:
            failed += 1
            # 初次抽取失败已消耗一次失败额度，剩余次数留给后续重试扣减
            remaining = total_fails - 1
            conn.execute(
                "INSERT INTO extract_rows (pipeline_id, row_key, source_ref, state, mode, attempts, "
                "fail_times, last_error, next_retry_at, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    pipeline_id, key, f"{item['source_uri']}#{key}", ROW_QUEUE, MODE_AUTO,
                    1, remaining, f"mock 抽取失败：{key} 源端返回空批", _fmt(_now()), now, now,
                ),
            )
            _log_event(
                conn, "extract_fail", pipeline_id,
                {"id": None, "row_key": key}, f"全量抽取失败，进入重试队列：{key}",
            )
        else:
            conn.execute(
                "INSERT INTO extract_rows (pipeline_id, row_key, source_ref, state, mode, attempts, "
                "fail_times, next_retry_at, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    pipeline_id, key, f"{item['source_uri']}#{key}", ROW_SUCCESS, MODE_AUTO,
                    1, 0, None, now, now,
                ),
            )
    status = "抽取失败" if failed else "已抽取待加载"
    conn.execute(
        "UPDATE pipelines SET status = ?, extracted = ?, loaded = 0, full_running = 0 WHERE id = ?",
        (status, len(MOCK_ROW_KEYS), pipeline_id),
    )
    return get_pipeline(conn, pipeline_id)


def run_load(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    item = conn.execute("SELECT * FROM pipelines WHERE id = ?", (pipeline_id,)).fetchone()
    if not item:
        return None
    if item["status"] != "已抽取待加载":
        return get_pipeline(conn, pipeline_id)
    conn.execute(
        "UPDATE pipelines SET status = ?, loaded = extracted WHERE id = ?",
        ("已落库", pipeline_id),
    )
    return get_pipeline(conn, pipeline_id)


# ---------------- 单行抽取 mock 与认领 ----------------

def _attempt_mock_extract(conn: sqlite3.Connection, row: sqlite3.Row) -> tuple[bool, str]:
    """确定性 mock：fail_times 未尽则失败并扣减，耗尽则成功。"""
    if row["fail_times"] > 0:
        conn.execute(
            "UPDATE extract_rows SET fail_times = fail_times - 1 WHERE id = ?", (row["id"],)
        )
        return False, f"mock 重试仍失败：{row['row_key']} 源端连接超时"
    return True, ""


def _get_row(conn: sqlite3.Connection, row_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM extract_rows WHERE id = ?", (row_id,)).fetchone()


def _claim_row(conn: sqlite3.Connection, row_id: int, token: str) -> sqlite3.Row | None:
    """原子认领：只有非重试中/非死信/非成功的行能被抢到，并发下只成一笔。"""
    cur = conn.execute(
        "UPDATE extract_rows SET state = ?, claim_token = ?, updated_at = ? "
        "WHERE id = ? AND state NOT IN (?, ?, ?)",
        (ROW_RETRYING, token, _fmt(_now()), row_id, ROW_RETRYING, ROW_DEAD, ROW_SUCCESS),
    )
    if cur.rowcount == 0:
        return None
    return _get_row(conn, row_id)


def _release_claim(conn: sqlite3.Connection, row_id: int, state: str) -> None:
    conn.execute(
        "UPDATE extract_rows SET state = ?, claim_token = NULL, updated_at = ? WHERE id = ?",
        (state, _fmt(_now()), row_id),
    )


# ---------------- 服务端重试调度 ----------------

def run_sweep(conn: sqlite3.Connection) -> dict:
    """一轮服务端自动调度：扫到点的 auto 队列行，认领后重试，满次停自动。"""
    settings = get_settings(conn)
    max_retries = settings["max_auto_retries"]
    delay = settings["retry_delay_seconds"]
    now = _now()
    due = conn.execute(
        "SELECT r.* FROM extract_rows r JOIN pipelines p ON p.id = r.pipeline_id "
        "WHERE r.state = ? AND r.mode = ? AND p.full_running = 0 "
        "AND (r.next_retry_at IS NULL OR r.next_retry_at <= ?) ORDER BY r.id",
        (ROW_QUEUE, MODE_AUTO, _fmt(now)),
    ).fetchall()
    swept = {"retried": 0, "succeeded": 0, "failed": 0, "stopped": 0}
    touched_pipelines: set[int] = set()
    for due_row in due:
        token = f"auto-{due_row['id']}-{_fmt(now)}"
        row = _claim_row(conn, due_row["id"], token)
        if row is None:  # 被人工/另一轮调度抢先
            continue
        touched_pipelines.add(row["pipeline_id"])
        swept["retried"] += 1
        ok, err = _attempt_mock_extract(conn, row)
        if ok:
            conn.execute(
                "UPDATE extract_rows SET state = ?, claim_token = NULL, attempts = attempts + 1, "
                "last_error = NULL, updated_at = ? WHERE id = ?",
                (ROW_SUCCESS, _fmt(_now()), row["id"]),
            )
            _log_event(conn, "retry_ok", row["pipeline_id"], row,
                       f"自动重试成功：{row['row_key']}（第 {row['attempts'] + 1} 次尝试）")
            swept["succeeded"] += 1
            continue
        next_auto = row["auto_retries"] + 1
        swept["failed"] += 1
        if next_auto >= max_retries:
            # 满次：停止自动重试，转人工待点，出路等人工触发时选定
            conn.execute(
                "UPDATE extract_rows SET state = ?, mode = ?, claim_token = NULL, "
                "auto_retries = ?, attempts = attempts + 1, last_error = ?, next_retry_at = NULL, "
                "updated_at = ? WHERE id = ?",
                (ROW_MANUAL, MODE_MANUAL, next_auto, err, _fmt(_now()), row["id"]),
            )
            _log_event(conn, "auto_stop", row["pipeline_id"], row,
                       f"自动重试已达上限 {max_retries} 次，停止自动，转人工待点")
            swept["stopped"] += 1
        else:
            next_at = _fmt(now + timedelta(seconds=delay))
            conn.execute(
                "UPDATE extract_rows SET state = ?, mode = ?, claim_token = NULL, "
                "auto_retries = ?, attempts = attempts + 1, last_error = ?, next_retry_at = ?, "
                "updated_at = ? WHERE id = ?",
                (ROW_QUEUE, MODE_AUTO, next_auto, err, next_at, _fmt(_now()), row["id"]),
            )
            _log_event(conn, "auto_retry", row["pipeline_id"], row,
                       f"自动重试失败（{next_auto}/{max_retries}），下次 {next_at}：{err}")
    for pid in touched_pipelines:
        _refresh_pipeline_status(conn, pid)
    return swept


# ---------------- 人工重试与出路 ----------------

def trigger_manual_retry(
    conn: sqlite3.Connection, row_id: int, exit_path: str | None, actor: str
) -> dict:
    row = _get_row(conn, row_id)
    if row is None:
        raise RetryError(404, "失败行不存在")
    pipe = conn.execute(
        "SELECT full_running FROM pipelines WHERE id = ?", (row["pipeline_id"],)
    ).fetchone()
    if pipe and pipe["full_running"]:
        _log_event(conn, "blocked_full", row["pipeline_id"], row,
                   "全量抽取进行中，单行重试被禁止", actor)
        raise RetryError(409, "管道正在跑全量抽取，禁止单行重试")
    if row["state"] == ROW_DEAD:
        raise RetryError(400, "死信行须填原因回炉后才能重试")
    if row["state"] == ROW_SUCCESS:
        raise RetryError(400, "该行已抽取成功，无需重试")
    if exit_path is not None and exit_path not in EXIT_PATHS:
        raise RetryError(400, "出路只能是 dead（转死信）或 manual（留队列人工触发）")

    # 出路当日锁定：同天再触发若出路偏了，直接打回
    locked = row["exit_path"]
    if locked and row["exit_locked_day"] == _today() and exit_path and exit_path != locked:
        _log_event(conn, "exit_skew", row["pipeline_id"], row,
                   f"出路写歪：当日已锁定 {locked}，本次传入 {exit_path}", actor)
        raise RetryError(400, "重试出路写歪了：当日出路已锁定，不能改")

    token = f"manual-{row_id}-{_fmt(_now())}-{actor}"
    claimed = _claim_row(conn, row_id, token)
    if claimed is None:
        # 两人同刻点同一行：只成一笔，落一条争抢记录
        _log_event(conn, "contention", row["pipeline_id"], row,
                   f"并发重试争抢，仅一笔生效（{actor} 未抢到）", actor)
        raise RetryError(409, "该行重试已在处理中，争抢只成一笔")
    row = _get_row(conn, row_id)

    ok, err = _attempt_mock_extract(conn, row)
    if ok:
        conn.execute(
            "UPDATE extract_rows SET state = ?, claim_token = NULL, attempts = attempts + 1, "
            "last_error = NULL, updated_at = ? WHERE id = ?",
            (ROW_SUCCESS, _fmt(_now()), row_id),
        )
        _log_event(conn, "retry_ok", row["pipeline_id"], row,
                   f"人工重试成功：{row['row_key']}", actor)
        _refresh_pipeline_status(conn, row["pipeline_id"])
        return dict(_get_row(conn, row_id))

    # 仍失败
    if row["mode"] == MODE_MANUAL:
        effective_exit = exit_path or locked
        if effective_exit not in EXIT_PATHS:
            _release_claim(conn, row_id, ROW_MANUAL)
            raise RetryError(400, "满次行须先选定出路：dead（转死信）或 manual（留队列人工触发）")
        today = _today()
        if effective_exit == EXIT_DEAD:
            note = f"满次出路：转入死信并停止自动重试（{today}）"
            conn.execute(
                "UPDATE extract_rows SET state = ?, mode = ?, claim_token = NULL, "
                "attempts = attempts + 1, last_error = ?, dead_reason = ?, exit_path = ?, "
                "exit_note = ?, exit_locked_day = ?, next_retry_at = NULL, updated_at = ? WHERE id = ?",
                (ROW_DEAD, MODE_MANUAL, err, err, EXIT_DEAD, note, today, _fmt(_now()), row_id),
            )
            _log_event(conn, "to_dead", row["pipeline_id"], row, note, actor)
        else:
            note = f"满次出路：留在队列改为只能人工触发（{today}）"
            conn.execute(
                "UPDATE extract_rows SET state = ?, mode = ?, claim_token = NULL, "
                "attempts = attempts + 1, last_error = ?, exit_path = ?, exit_note = ?, "
                "exit_locked_day = ?, next_retry_at = NULL, updated_at = ? WHERE id = ?",
                (ROW_MANUAL, MODE_MANUAL, err, EXIT_MANUAL, note, today, _fmt(_now()), row_id),
            )
            _log_event(conn, "to_manual", row["pipeline_id"], row, note, actor)
    else:
        # 自动额度未满时的人工点试：不占自动次数，仍留在 auto 队列等调度
        settings = get_settings(conn)
        next_at = _fmt(_now() + timedelta(seconds=settings["retry_delay_seconds"]))
        conn.execute(
            "UPDATE extract_rows SET state = ?, claim_token = NULL, attempts = attempts + 1, "
            "last_error = ?, next_retry_at = ?, updated_at = ? WHERE id = ?",
            (ROW_QUEUE, err, next_at, _fmt(_now()), row_id),
        )
        _log_event(conn, "manual_retry", row["pipeline_id"], row,
                   f"人工重试失败，回到自动队列：{err}", actor)
    _refresh_pipeline_status(conn, row["pipeline_id"])
    return dict(_get_row(conn, row_id))


def requeue_dead_row(conn: sqlite3.Connection, row_id: int, reason: str, actor: str) -> dict:
    row = _get_row(conn, row_id)
    if row is None:
        raise RetryError(404, "失败行不存在")
    if row["state"] != ROW_DEAD:
        raise RetryError(400, "只有死信行才能回炉")
    reason = (reason or "").strip()
    if not reason:
        raise RetryError(400, "死信行须填原因才能回炉")
    now = _fmt(_now())
    # 回炉：回队列但只接受人工触发，自动次数清零，出路锁定解除（当日说明留在事件里）
    conn.execute(
        "UPDATE extract_rows SET state = ?, mode = ?, claim_token = NULL, auto_retries = 0, "
        "exit_path = NULL, exit_note = NULL, exit_locked_day = NULL, dead_reason = ?, "
        "next_retry_at = ?, updated_at = ? WHERE id = ?",
        (ROW_QUEUE, MODE_MANUAL, reason, now, now, row_id),
    )
    _log_event(conn, "requeue", row["pipeline_id"], row, f"死信回炉，原因：{reason}", actor)
    _refresh_pipeline_status(conn, row["pipeline_id"])
    return dict(_get_row(conn, row_id))


# ---------------- 总览 ----------------

def retry_overview(conn: sqlite3.Connection) -> dict:
    rows = conn.execute(
        "SELECT p.id AS pipeline_id, p.title, r.state, COUNT(*) AS c "
        "FROM extract_rows r JOIN pipelines p ON p.id = r.pipeline_id "
        "GROUP BY p.id, r.state ORDER BY p.id, r.state"
    ).fetchall()
    per_pipeline: dict[int, dict] = {}
    totals = {"queue": 0, "retrying": 0, "manual": 0, "dead": 0, "success": 0}
    state_to_key = {
        ROW_QUEUE: "queue", ROW_RETRYING: "retrying", ROW_MANUAL: "manual",
        ROW_DEAD: "dead", ROW_SUCCESS: "success",
    }
    for r in rows:
        bucket = per_pipeline.setdefault(
            r["pipeline_id"],
            {"pipeline_id": r["pipeline_id"], "title": r["title"],
             "queue": 0, "retrying": 0, "manual": 0, "dead": 0, "success": 0},
        )
        key = state_to_key[r["state"]]
        bucket[key] = r["c"]
        totals[key] += r["c"]
    return {
        "totals": totals,
        "pipelines": list(per_pipeline.values()),
        "settings": get_settings(conn),
    }
