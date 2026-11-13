FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=6000 \
    DATABASE_PATH=/app/instance/etl_lab.db

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY . .
RUN mkdir -p /app/instance

EXPOSE 6000

CMD ["python", "run.py"]
