@echo off
cd /d "%~dp0"
py server.py || python server.py || python3 server.py
pause
