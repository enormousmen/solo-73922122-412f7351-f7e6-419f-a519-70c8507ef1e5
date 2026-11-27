FROM python:3.12-slim

WORKDIR /srv
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

# 默认启动 API；receiver / relay 由 compose 覆盖 command
CMD ["uvicorn", "app.api_main:app", "--host", "0.0.0.0", "--port", "8000"]
