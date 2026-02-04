# Bulk Charge App Flow

This document describes the end-to-end user journey and data flow for the Bulk Charges Builder desktop/web app.

**Scope**
- Desktop runner: `main.py` (pywebview + local FastAPI)
- UI: `ui/index.html`
- API: `backend_node_parity.py`

**Actors**
- User
- UI (browser or pywebview)
- Local API (FastAPI)
- AppFolio V2 Reports
- AppFolio V0 API

**User Journey (Step-by-Step)**
1. User launches the desktop app (`main.py`) or opens the hosted UI at `/`.
2. The UI loads with an empty table and filter controls.
3. The user clicks **Load Data**.
4. The UI requests `GET /api/table-data`.
5. The backend fetches and merges V2 Aged Receivables report rows, V2 Tenant Directory rows, and V0 Tenants (31-day window + property list).
6. The backend maps occupancy IDs, computes late fee amounts, and returns rows + warnings.
7. The UI renders the table, populates filters, and sets the default description date.
8. The user filters by property, charge date (month/year), and optional 0-30 minimum.
9. The user can toggle AR columns (0-30 and Monthly Rent).
10. The user can apply a description date to the currently filtered rows.
11. The user selects rows.
12. The user exports CSV or creates bulk charges.

**Mermaid - User Journey Flow**
```mermaid
flowchart TD
  A[Launch app or open /] --> B[UI loads empty table + filters]
  B --> C[Click Load Data]
  C --> D[GET /api/table-data]
  D --> E[Backend fetches V2 Aged Receivables + V2 Tenant Directory + V0 Tenants]
  E --> F[Map occupancy + compute late fee + build rows]
  F --> G[UI renders table + filters]
  G --> H[User filters/toggles AR columns]
  H --> I[Optional: apply description date]
  I --> J[Select rows]
  J --> K{Action?}
  K -->|Export CSV| L{Desktop app?}
  L -->|Yes| M[pywebview Save dialog + write CSV]
  L -->|No| N[POST /api/export-csv -> browser download]
  K -->|Create Bulk Charges| O[POST /api/bulk-charges]
  O --> P[Backend POST /api/v0/charges/bulk]
  P --> Q[Success or error message]
```

**Mermaid - System Sequence**
```mermaid
sequenceDiagram
  participant User
  participant UI as UI (ui/index.html)
  participant API as FastAPI (backend_node_parity.py)
  participant V2 as AppFolio V2 Reports
  participant V0 as AppFolio V0 API

  User->>UI: Click "Load Data"
  UI->>API: GET /api/table-data
  API->>V2: POST aged_receivables_detail.json (filtered)
  API->>V2: POST tenant_directory.json (filtered)
  API->>V0: GET /tenants (last 31 days + property list)
  API-->>API: Map occupancy IDs + compute late fee amount
  API-->>UI: rows + warnings
  UI-->>User: Table + filters

  User->>UI: Export CSV
  alt Desktop (pywebview)
    UI->>API: window.pywebview.api.save_csv_to_dir(rows)
    API-->>UI: saved path
  else Web/Hosted
    UI->>API: POST /api/export-csv
    API-->>UI: CSV file
  end

  User->>UI: Create Bulk Charges
  UI->>API: POST /api/bulk-charges
  API->>V0: POST /charges/bulk
  V0-->>API: result
  API-->>UI: success or error
```

**Backend Data Rules (Used by `/api/table-data`)**
- V2 Aged Receivables are filtered by `FILTER_GL_ACCOUNT` when set.
- Late fee amount is computed from `0_to30` and `total_amount` with property-specific thresholds.
- `Charge Date` and `Posting Date` default to today if missing.
- `Description` defaults to `IL Custom Late Fee - MM/01/YYYY` based on charge date.
- Rows missing `_v0OccupancyId` trigger a fallback V0 tenant fetch over a 10-year window.
- Warnings are returned if any upstream fetch fails or returns no rows.

**Primary API Endpoints**
- `GET /api/health`
- `GET /api/table-data`
- `POST /api/export-csv`
- `POST /api/bulk-charges`

