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
