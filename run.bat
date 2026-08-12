@echo off
echo ===================================================
echo 🚀 LAUNCHING MULTI-USER SECURE TRANSACTION DASHBOARD
echo ===================================================
title Secure Fraud Engine Portal - Port 5001

:: Force-close lingering port locks before starting
taskkill /f /im python.exe 2>nul

:: AUTOMATION TRICK: Forces Windows to open your browser straight to your link instantly
echo 🌐 Launching secure browser gateway...
start "" "http://127.0.0.1:5001"

:: Launch your main Python application engine loop
python app.py
if %ERRORLEVEL% EQU 0 goto end

:: Fallback launcher if system global path path links are frozen
py app.py

:end
echo ===================================================
echo ⚠️ SERVER MONITOR IS CLOSED. KEEP THIS WINDOW OPEN.
echo ===================================================
pause
