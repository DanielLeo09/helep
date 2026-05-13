# ---- build stage ----
FROM python:3.12-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- runtime stage ----
FROM python:3.12-slim
RUN groupadd --gid 10001 helep \
    && useradd --uid 10001 --gid helep --no-create-home --shell /bin/false helep
WORKDIR /app
COPY --from=builder /usr/local /usr/local
COPY --chown=helep:helep app ./app
USER helep
EXPOSE 8003
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8003"]
