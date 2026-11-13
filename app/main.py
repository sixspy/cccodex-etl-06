from contextlib import contextmanager
from pathlib import Path
import sqlite3

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from itsdangerous import BadSignature, URLSafeSerializer

from app.config import Config
from app.db import (
    EXIT_PATHS,
    RetryError,
    connect,
    get_db,
    get_pipeline,
    get_settings,
    init_db,
    list_pipelines,
    requeue_dead_row,
    retry_overview,
    run_extract,
    run_load,
    run_sweep,
    save_settings,
    trigger_manual_retry,
)
from app.scheduler import scheduler, start_scheduler


@contextmanager
def db_session():
    """领域错误（RetryError）抛出前先提交，保留争抢/出路写歪/全量拦截等事件记录；
    其余异常照常回滚。直接持有单连接，避免与 get_db 嵌套导致二次提交。
    注意：RetryError 必须穿透本上下文，在 with 块外再转成 HTTPException。"""
    conn = connect()
    try:
        yield conn
    except RetryError:
        conn.commit()
        raise
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()

ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "static"
serializer = URLSafeSerializer(Config.SECRET, salt="cc-etl-session")

app = FastAPI(title="cc-etl-lab")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


def _read_session(request: Request) -> dict:
    raw = request.cookies.get("etl_session")
    if not raw:
        return {}
    try:
        data = serializer.loads(raw)
        return data if isinstance(data, dict) else {}
    except BadSignature:
        return {}


def require_user(request: Request) -> str:
    user = _read_session(request).get("user")
    if user != Config.ADMIN_USER:
        raise HTTPException(status_code=401, detail="未登录")
    return user


@app.on_event("startup")
def _startup() -> None:
    init_db()
    start_scheduler()


@app.on_event("shutdown")
def _shutdown() -> None:
    scheduler.stop()


@app.get("/")
def root(request: Request):
    if _read_session(request).get("user") == Config.ADMIN_USER:
        return RedirectResponse("/app/", status_code=302)
    return RedirectResponse("/login", status_code=302)


@app.get("/login")
def login_page():
    return FileResponse(STATIC / "login.html")


@app.get("/app/")
def spa_page(request: Request):
    if _read_session(request).get("user") != Config.ADMIN_USER:
        return RedirectResponse("/login", status_code=302)
    return FileResponse(STATIC / "index.html")


@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    user = str(body.get("username", "")).strip()
    pwd = str(body.get("password", ""))
    if user != Config.ADMIN_USER or pwd != Config.ADMIN_PASSWORD:
        return JSONResponse({"ok": False, "error": "账号或密码错误"}, status_code=400)
    resp = JSONResponse({"ok": True, "user": user})
    resp.set_cookie(
        "etl_session",
        serializer.dumps({"user": user}),
        httponly=True,
        samesite="lax",
    )
    return resp


@app.post("/api/logout")
def api_logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("etl_session")
    return resp


@app.get("/api/me")
def api_me(user: str = Depends(require_user)):
    return {"user": user, "lab": Config.LAB_NAME, "domain": "数据工程"}


@app.get("/api/pipelines")
def api_pipelines(user: str = Depends(require_user)):
    with get_db() as conn:
        return {"items": list_pipelines(conn)}


@app.get("/api/retry/overview")
def api_retry_overview(user: str = Depends(require_user)):
    with get_db() as conn:
        return retry_overview(conn)


@app.get("/api/retry/settings")
def api_get_settings(user: str = Depends(require_user)):
    with get_db() as conn:
        return get_settings(conn)


@app.put("/api/retry/settings")
async def api_put_settings(request: Request, user: str = Depends(require_user)):
    body = await request.json()
    try:
        max_retries = int(body.get("max_auto_retries"))
        delay = int(body.get("retry_delay_seconds"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="重试设置须为整数")
    try:
        with db_session() as conn:
            return save_settings(conn, max_retries, delay)
    except RetryError as exc:
        raise HTTPException(status_code=exc.status, detail=exc.message)


@app.post("/api/retry/sweep")
def api_sweep(user: str = Depends(require_user)):
    """跑一轮服务端自动重试调度（后台线程也会自动跑，此接口便于钉死时序）。"""
    with get_db() as conn:
        return run_sweep(conn)


@app.post("/api/retry/rows/{row_id}/retry")
async def api_manual_retry(row_id: int, request: Request, user: str = Depends(require_user)):
    body = {}
    try:
        raw = await request.body()
        if raw:
            body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="请求体须为 JSON")
    exit_path = body.get("exit_path")
    if exit_path is not None and exit_path not in EXIT_PATHS:
        raise HTTPException(status_code=400, detail="重试出路写歪了：只能选 dead 或 manual")
    try:
        with db_session() as conn:
            return trigger_manual_retry(conn, row_id, exit_path, user)
    except RetryError as exc:
        raise HTTPException(status_code=exc.status, detail=exc.message)
    except sqlite3.OperationalError as exc:
        # 并发下写锁竞争：归一为争抢只成一笔，而不是 500
        if "locked" in str(exc).lower():
            raise HTTPException(status_code=409, detail="该行重试已在处理中，争抢只成一笔")
        raise


@app.post("/api/retry/rows/{row_id}/requeue")
async def api_requeue(row_id: int, request: Request, user: str = Depends(require_user)):
    body = {}
    raw = await request.body()
    if raw:
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="请求体须为 JSON")
    reason = str(body.get("reason", "")).strip()
    try:
        with db_session() as conn:
            return requeue_dead_row(conn, row_id, reason, user)
    except RetryError as exc:
        raise HTTPException(status_code=exc.status, detail=exc.message)


@app.get("/api/pipelines/{pipeline_id}")
def api_pipeline_detail(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        item = get_pipeline(conn, pipeline_id)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


@app.post("/api/pipelines/{pipeline_id}/extract")
def api_extract(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        item = run_extract(conn, pipeline_id)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


@app.post("/api/pipelines/{pipeline_id}/load")
def api_load(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        item = run_load(conn, pipeline_id)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


def create_app() -> FastAPI:
    return app
