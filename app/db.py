import sqlite3
from contextlib import contextmanager
from pathlib import Path

from app.config import Config

SEED_PIPELINES = [
    ("订单明细日抽", "orders_daily", "待抽取", "mock://orders/day"),
    ("用户画像宽表", "user_profile", "待抽取", "mock://users/profile"),
    ("库存快照落地", "inventory_snap", "待抽取", "mock://stock/snap"),
    ("支付流水清洗", "pay_clean", "待抽取", "mock://pay/flow"),
]


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or Config.database()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
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
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            """
        )
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


def list_pipelines(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT id, title, code, status, source_uri, extracted, loaded, created_at "
        "FROM pipelines ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def get_pipeline(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    row = conn.execute(
        "SELECT id, title, code, status, source_uri, extracted, loaded, created_at "
        "FROM pipelines WHERE id = ?",
        (pipeline_id,),
    ).fetchone()
    return dict(row) if row else None


def run_extract(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None
    if item["status"] not in ("待抽取", "抽取失败"):
        return item
    extracted = 12 + (pipeline_id % 5)
    conn.execute(
        "UPDATE pipelines SET status = ?, extracted = ?, loaded = 0 WHERE id = ?",
        ("已抽取待加载", extracted, pipeline_id),
    )
    return get_pipeline(conn, pipeline_id)


def run_load(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None
    if item["status"] != "已抽取待加载":
        return item
    conn.execute(
        "UPDATE pipelines SET status = ?, loaded = extracted WHERE id = ?",
        ("已落库", pipeline_id),
    )
    return get_pipeline(conn, pipeline_id)
