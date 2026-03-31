@echo off
echo Clearing port 8000...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8000 "') do (
    taskkill /F /PID %%a >nul 2>&1
)
timeout /t 1 /nobreak >nul
echo.
echo Starting Prosopography Explorer...
echo  ^> http://localhost:8000
echo.
C:\Users\spatt\anaconda3\envs\eliteresearchagent_v3\python.exe -m uvicorn web.app:app --reload --host 127.0.0.1 --port 8000
