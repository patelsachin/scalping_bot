@echo off
title Siva Scalping Bot — Daily Start
echo =====================================================
echo   Siva Scalping Bot — Daily Token Refresh + Start
echo =====================================================
echo.

cd /d "%~dp0"
call venv\Scripts\activate.bat

echo [Step 1] Refreshing Kite access token...
echo.
python -m src.broker.kite_login
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ERROR: Login failed. Check your api_key / api_secret in config\credentials.yaml
    pause
    exit /b 1
)

echo.
echo [Step 2] Starting the bot...
echo.
python main.py

echo.
echo Bot exited.
pause
