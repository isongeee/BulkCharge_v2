
@echo off
setlocal

where python >nul 2>nul && (
  where python | findstr /I "\\WindowsApps\\python.exe" >nul 2>nul && (
    echo ERROR: Your "python" resolves to the Microsoft Store shim (WindowsApps).
    echo Install Python 3.11+ from python.org (recommended, includes "py") or disable App execution aliases for python.exe/python3.exe.
    exit /b 1
  )
)

if not exist .venv (
  where py >nul 2>nul && (py -3.11 -m venv .venv) || (python -m venv .venv)
)
call .venv\Scripts\activate
python -m pip install -r requirements.txt
python main.py
