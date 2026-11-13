"""服务端重试调度：后台线程周期性扫到点的自动重试队列。

测试环境通过 RETRY_SCHEDULER=off 关闭，改用显式的 sweep API 钉死时序。
"""
import os
import threading
import time

from app.config import Config
from app.db import get_db, run_sweep


class RetryScheduler:
    def __init__(self, interval_seconds: float = 2.0):
        self.interval = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="retry-scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    def _loop(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                with get_db() as conn:
                    run_sweep(conn)
            except Exception as exc:  # 调度失败不能打死线程
                print(f"[retry-scheduler] sweep 失败: {exc}")

    def sweep_once(self) -> dict:
        with get_db() as conn:
            return run_sweep(conn)


scheduler = RetryScheduler(interval_seconds=float(os.environ.get("RETRY_SWEEP_INTERVAL", "2")))


def scheduler_enabled() -> bool:
    return os.environ.get("RETRY_SCHEDULER", "on").lower() not in ("off", "0", "false")


def start_scheduler() -> None:
    if scheduler_enabled() and Config.database():
        scheduler.start()
