FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Optional AI segmentation; explicitly enabled at image build, not at app startup.
ARG ENABLE_CATALOG_PHOTO_AI=0
COPY requirements-photo-ai.txt .
RUN if [ "$ENABLE_CATALOG_PHOTO_AI" = "1" ]; then pip install --no-cache-dir -r requirements-photo-ai.txt && python -c "from rembg import new_session; new_session('u2netp')"; fi

COPY . .

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
