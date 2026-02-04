
import threading, time, webview, uvicorn
from datetime import datetime

import backend_node_parity as backend  # parity backend (FastAPI + helpers)


def run_server():
    uvicorn.run("backend_node_parity:app", host="127.0.0.1", port=3255, log_level="info", reload=False)


class Bridge:
    FILE_FILTER = ("*.csv",)

    def save_csv_to_dir(self, rows):
        import os
        try:
            csv_text = backend.rows_to_csv_io(rows).getvalue()
            win = webview.windows[0]

            folder = win.create_file_dialog(webview.FOLDER_DIALOG)
            if not folder:
                return {"ok": False, "error": "User cancelled"}
            if isinstance(folder, (list, tuple)):
                folder = folder[0]
            if not isinstance(folder, str):
                return {"ok": False, "error": "Invalid folder"}

            fname = f"bulk-charges-{datetime.utcnow().date().isoformat()}.csv"
            full_path = os.path.join(folder, fname)
            if not full_path.lower().endswith(".csv"):
                full_path += ".csv"

            with open(full_path, "w", encoding="utf-8-sig", newline="") as f:
                f.write(csv_text)

            return {"ok": True, "path": full_path}
        except Exception as e:
            import traceback

            traceback.print_exc()
            return {"ok": False, "error": str(e)}

    def save_csv(self, rows):
        try:
            csv_text = backend.rows_to_csv_io(rows).getvalue()
            win = webview.windows[0]
            default = f"bulk-charges-{datetime.utcnow().date().isoformat()}.csv"

            save_path = win.create_file_dialog(
                webview.SAVE_DIALOG, save_filename=default, file_types=self.FILE_FILTER
            )
            if not save_path:
                return {"ok": False, "error": "User cancelled"}
            if isinstance(save_path, (list, tuple)):
                save_path = save_path[0]
            if not isinstance(save_path, str):
                return {"ok": False, "error": "Invalid file path"}
            if not save_path.lower().endswith(".csv"):
                save_path += ".csv"

            with open(save_path, "w", encoding="utf-8-sig", newline="") as f:
                f.write(csv_text)

            return {"ok": True, "path": save_path}
        except Exception as e:
            import traceback

            traceback.print_exc()
            return {"ok": False, "error": str(e)}


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
