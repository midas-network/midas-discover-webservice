FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

COPY pyproject.toml ./
RUN uv sync --no-dev

COPY . .

RUN uv sync --no-dev

# python-decouple reads 'user' key to pick config section (SERVER vs DEV)
RUN echo "user=SERVER" > .env

ENV PORT=8080

CMD ["uv", "run", "gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "120", "app:app"]
