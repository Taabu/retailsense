FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN pip install --no-cache-dir uv
RUN uv sync --frozen --no-dev --no-editable

COPY src/ ./src/

ENTRYPOINT ["uv", "run", "python", "-m", "retailsense_bridge.main"]