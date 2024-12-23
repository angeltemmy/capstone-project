FROM python:3.9-slim-bookworm

WORKDIR /app

COPY requirements.txt .



COPY /app .



CMD ["python", "-m", "scripts.main"]