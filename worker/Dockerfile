# The order workers (M4 task 6): one image, two roles (ROLE=auditor|ledger).
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml .
COPY taproot_worker/ taproot_worker/
RUN pip install --no-cache-dir .

RUN useradd --system --no-create-home worker
USER worker

ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "taproot_worker"]
