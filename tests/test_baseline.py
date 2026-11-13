def test_login_and_list(authed):
    me = authed.get("/api/me")
    assert me.status_code == 200
    assert me.json()["user"] == "admin"
    assert me.json()["domain"] == "数据工程"
    pipes = authed.get("/api/pipelines")
    assert pipes.status_code == 200
    items = pipes.json()["items"]
    assert len(items) >= 3
    detail = authed.get(f"/api/pipelines/{items[0]['id']}")
    assert detail.status_code == 200
    assert detail.json()["source_uri"].startswith("mock://")


def test_extract_then_load(authed):
    items = authed.get("/api/pipelines").json()["items"]
    pid = items[0]["id"]
    ex = authed.post(f"/api/pipelines/{pid}/extract")
    assert ex.status_code == 200
    assert ex.json()["status"] == "已抽取待加载"
    assert ex.json()["extracted"] > 0
    ld = authed.post(f"/api/pipelines/{pid}/load")
    assert ld.status_code == 200
    assert ld.json()["status"] == "已落库"
    assert ld.json()["loaded"] == ld.json()["extracted"]


def test_unauthed_blocked(client):
    assert client.get("/api/pipelines").status_code == 401


def test_login_page(client):
    r = client.get("/login")
    assert r.status_code == 200
    assert "抽取转换加载台" in r.text
    assert "密码" in r.text or "password" in r.text.lower()
