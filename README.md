# Bulk Charge - Python Desktop App (Node-Parity Backend)

This repo is standardized on the node-parity implementation:
- Backend: `backend_node_parity.py`
- Desktop runner: `main.py`

`backend.py` and `main_node_parity.py` are kept as compatibility shims.

## Quick start
1) Install Python 3.11+
2) `pip install -r requirements.txt`
3) `copy .env.example .env` then fill it
4) `python main.py` (or run `start_app.bat` on Windows)

## Smoke test (recommended)
Run `powershell -ExecutionPolicy Bypass -File scripts/smoke_test.ps1` to verify:
- API boots (`GET /api/health`)
- `GET /api/table-data` returns JSON

## Environment variables (.env)
- AppFolio V2 (Basic): `V2_BASE`, `V2_USER`, `V2_PASS`
- Optional scoping: `V2_PROPERTY_IDS` (comma-separated V2 property IDs)
- AppFolio V0 (OAuth): `V0_BASE`, `V0_DEV_ID`, `V0_CLIENT_ID`, `V0_CLIENT_SECRET`
- Charges: `BULK_GL_ACCOUNT_ID`, `TABLE_GL_ACCOUNT_NUMBER`, `FILTER_GL_ACCOUNT`
- Optional scoping: `V0_PROPERTY_IDS` (comma-separated PropertyIds)
- Optional fee rules: `LATE_FEE_THRESHOLD`, `LATE_FEE_PERCENT`, `LATE_FEE_BASE_FEE`
- Performance: `CACHE_TTL_SECONDS`, `V2_TENANT_COLUMNS_MODE`, `HTTP_POOL_MAXSIZE`

## API
- `GET /api/health`
- `GET /api/table-data` (optional: `refresh=1`, `resolve_missing=1`)
- `POST /api/bulk-charges`
- `POST /api/export-csv`

## Deploy to Vercel
Vercel can deploy this as a FastAPI backend directly using `app.py` (no serverless function rewrites needed).

1) Push to GitHub
2) In Vercel: **New Project** → import the repo → Deploy
3) In Vercel project settings, add Environment Variables from your `.env`:
   - `V2_BASE`, `V2_USER`, `V2_PASS`
   - `V0_BASE`, `V0_DEV_ID`, `V0_CLIENT_ID`, `V0_CLIENT_SECRET`
   - `BULK_GL_ACCOUNT_ID`, `TABLE_GL_ACCOUNT_NUMBER`, `FILTER_GL_ACCOUNT`
   - Optional: `V2_PROPERTY_IDS`, `V0_PROPERTY_IDS`, `LATE_FEE_*`, `CACHE_TTL_SECONDS`, `V2_TENANT_COLUMNS_MODE`, `HTTP_POOL_MAXSIZE`

Notes:
- The deployed site serves the UI from `ui/` at `/`.
- `pywebview` is Windows-only and is skipped on Vercel via environment markers in `requirements.txt`.
- After deploy, open `/api/health`, `/api/table-data`, and `/api/debug/aged-raw` to verify.

If Vercel fails with a Serverless Function size limit error, ensure `.vercelignore` is present (this repo includes it) so large Windows build artifacts (`dist/`, `dist_v2/`, `Late_Fee_Live/`) are not uploaded.
