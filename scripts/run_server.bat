@echo off
setlocal
cd /d "%~dp0\.."
title CCTV Monitoring Dashboard AMS Service
echo =======================================================
echo   Dashboard Monitoring CCTV - Ant Media Server (AMS)
echo   FastAPI Production Server with Auto-Restart Loop
echo =======================================================

:loop
echo [%date% %time%] Memulai server FastAPI uvicorn...
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
echo.
echo [%date% %time%] [PERINGATAN] Server terhenti secara tidak terduga!
echo [%date% %time%] Merestart otomatis server dalam 3 detik...
timeout /t 3 /nobreak >nul
goto loop
