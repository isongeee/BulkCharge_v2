
@echo off
setlocal
call .venv\Scripts\activate
pip install pyinstaller
pyinstaller --noconsole --onefile ^
  --add-data "ui\*;ui" ^
  --add-data "sample_data\*;sample_data" ^
  main.py
echo Built dist\main.exe
