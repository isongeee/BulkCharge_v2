
"""
Legacy module name kept for compatibility.

The UI and app entrypoint are built around the "node parity" implementation in
`backend_node_parity.py`. Importing `backend` now re-exports that implementation
so older `uvicorn backend:app` invocations keep working.
"""

from backend_node_parity import *  # noqa: F403
from backend_node_parity import app  # noqa: F401
