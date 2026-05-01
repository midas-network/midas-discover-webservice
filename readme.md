# MIDAS API (uv)

This repository serves a Flask API and contains data pipeline scripts that rebuild the SQLite database used by the API.

## Prerequisites

- Python 3.11
- `uv` installed: https://docs.astral.sh/uv/
- MIDAS API key in `key.txt` (required for data refresh scripts)
- Google Cloud SDK (`gcloud`) for deployment

## Quick Start (uv)

From the repo root:

```bash
uv sync
```

Run the API locally:

```bash
echo "user=DEV" > .env
uv run flask --app app --debug run
```

Run tests:

```bash
uv run pytest -v
```

## How To Update The Database

Run all commands from the repo root unless noted.

1. Ensure `key.txt` exists and contains a valid MIDAS API key.
2. Ensure `.env` points to local config:

```bash
echo "user=DEV" > .env
```

3. Pull latest upstream JSON data:

```bash
uv run python data_updates/update_midas_data.py
```

4. Rebuild the SQLite database (`midasDB` by default via `config.ini`):

```bash
uv run python data_updates/update_database.py
```

5. Optional validation: start API and hit an endpoint.

```bash
uv run flask --app app --debug run
```

Notes:
- `config.ini` controls database path by environment section (`DEV` and `SERVER`).
- The app reads `.env` variable `user` to select the section in `config.ini`.

## Docker (uv-based)

Build image:

```bash
docker build -t midas-api:local .
```

Run locally:

```bash
docker run --rm -p 8080:8080 midas-api:local
```

## Deploy To Google Cloud Run

This deployment path uses Artifact Registry + Cloud Run.

Set variables (edit values):

```bash
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"
export REPO_NAME="midas-api"
export SERVICE_NAME="midas-api"
```

Authenticate and configure project:

```bash
gcloud auth login
gcloud config set project "$PROJECT_ID"
gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com
```

Create Artifact Registry repository (one-time):

```bash
gcloud artifacts repositories create "$REPO_NAME" \
	--repository-format=docker \
	--location="$REGION" \
	--description="MIDAS API container images"
```

Build and push image with Cloud Build:

```bash
gcloud builds submit \
	--tag "$REGION-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/midas-api:latest"
```

Deploy to Cloud Run:

```bash
gcloud run deploy "$SERVICE_NAME" \
	--image "$REGION-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/midas-api:latest" \
	--region "$REGION" \
	--platform managed \
	--allow-unauthenticated
```

Set runtime environment selector so app uses server config:

```bash
gcloud run services update "$SERVICE_NAME" \
	--region "$REGION" \
	--update-env-vars user=SERVER
```

## Production Notes

- `midasDB` must be present in the container filesystem at runtime for query endpoints to work.
- If you rebuild `midasDB`, rebuild and redeploy the image so Cloud Run gets the updated database file.
- For larger or frequently updated datasets, consider moving database storage off-image (for example Cloud SQL or mounted volume patterns) and update `config.ini` accordingly.

## Useful Commands

```bash
# install/update deps
uv sync

# local api
uv run flask --app app --debug run

# tests
uv run pytest -v

# data refresh
uv run python data_updates/update_midas_data.py
uv run python data_updates/update_database.py
```
