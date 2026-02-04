"""
Compatibility entrypoint.

`main.py` is the canonical app runner for this repo now.
This module is kept so existing shortcuts/scripts that reference
`main_node_parity.py` continue to work.
"""

from main import Bridge, run_server  # noqa: F401

import threading
import time
import webview


if __name__ == "__main__":
    t = threading.Thread(target=run_server, daemon=True)
    t.start()
    time.sleep(1.2)
    window = webview.create_window(
        "Bulk Charges Builder",
        "http://127.0.0.1:3255/",
        js_api=Bridge(),
    )
    webview.start()
