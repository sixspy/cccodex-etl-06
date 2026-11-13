# cc-etl-lab

CC_Codex 二期（26.11）**数据工程**产品线：**抽取转换加载台**（FastAPI + Vue 3 SPA + SQLite）。

与 `cc-data-lab`（Flask + HTMX 采集控制台）错开：本线面向 **ETL 管道编排 / 抽取落库**，前端为 Vue 单页，暖色仓运风格。

## 领域

- **所属领域**：数据工程  
- **子方向**：抽取-转换-加载 / 数据采集与落库  

## 技术栈

| 层 | 选型 |
|----|------|
| 后端 | Python 3.12、FastAPI、Uvicorn、SQLite |
| 前端 | Vue 3（CDN Composition）、双栏管道台 |
| 风格 | 暖琥珀仓运底、圆角强调、等宽源地址 |
| 测试 | pytest + httpx |

## 基线能力

登录、管道列表、管道详情、执行抽取、执行落库。后续单轮题在此叠加增量水位、清洗口径、并发争用、跨日重算等。

## 抽取失败重试与死信筐（etl-06）

- **失败行**：全量抽取按行落 `extract_rows`，抖动源管道（偶数 id）的 R02/R05 确定性失败，进入**重试队列**；页面顶部总览给出重试中、死信数、人工待点、队列待重试。
- **服务端调度**：后台线程 `RetryScheduler` 周期性 `run_sweep`，只扫到点的 `auto` 队列行，认领（原子 `UPDATE ... WHERE state NOT IN (...)`）后重试；测试用 `RETRY_SCHEDULER=off` 关闭，改调 `POST /api/retry/sweep` 钉死时序。
- **自动次数上限**：写在重试设置（`max_auto_retries`），间隔 `retry_delay_seconds` 可调，测试可钉死次数；打满即停自动、转「人工待点」。
- **满次出路（当日锁定）**：人工触发满次行须选 `dead`（转死信并停止自动重试）或 `manual`（留队列仅人工触发），说明写入 `exit_note`，当日不可改；同天再点成别的出路返回「重试出路写歪了」并留 `exit_skew` 事件。
- **并发争抢**：同一行同刻两次重试只成一笔，败方落 `contention` 事件并收到 409。
- **死信筐**：死信行不能直接重试，回炉须填原因，回炉后清零自动次数、仅人工触发。
- **全量拦截**：管道 `full_running=1` 期间禁止单行重试，落 `blocked_full` 事件。
- **详情可展开**：失败摘要与重试争抢记录（`retry_events`），出路写歪/争抢高亮。

### 重试相关接口

| 方法 | 路径 | 说明 |
|----|------|------|
| GET | `/api/retry/overview` | 置顶总览（重试中/死信/人工待点/队列） |
| GET/PUT | `/api/retry/settings` | 自动次数上限与重试间隔 |
| POST | `/api/retry/sweep` | 跑一轮服务端自动调度（测试钉时序用） |
| POST | `/api/retry/rows/{id}/retry` | 人工触发重试，body 可带 `exit_path` |
| POST | `/api/retry/rows/{id}/requeue` | 死信回炉，body 必填 `reason` |


## 本地

```powershell
cd claude-workspace\cc-etl-lab
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest
python run.py
```

http://127.0.0.1:6000/login · **admin / 123456**

## Docker

```powershell
docker compose up -d --build
```

端口 **6000**。冒烟：`python ..\scripts\_smoke_lab_access.py --dir .`
