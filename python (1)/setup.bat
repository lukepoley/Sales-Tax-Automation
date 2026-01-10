@echo off
:: Check for Admin Permissions
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo [ERROR] PLEASE RIGHT-CLICK AND "RUN AS ADMINISTRATOR"
    pause
    exit /b
)

TITLE NNOGC Tool Setup
echo ====================================================
echo   NNOGC SQL Automation: COMPLETE DEPENDENCY SETUP
echo ====================================================

:: 1. Install Python (Works for Intel and AMD)
echo Downloading Python 3.12...
curl -L https://www.python.org/ftp/python/3.12.0/python-3.12.0-amd64.exe -o python_installer.exe
echo Installing Python...
start /wait python_installer.exe /quiet InstallAllUsers=1 PrependPath=1
del python_installer.exe

:: 2. Install Python Libraries
echo Installing Required Libraries...
:: We use 'python' because it was just added to the path
python -m pip install --upgrade pip
python -m pip install pandas openpyxl xlsxwriter

echo.
echo ====================================================
echo   SETUP COMPLETE!
echo   1. PLEASE RESTART YOUR COMPUTER NOW.
echo   2. After restarting, use 'run.bat' to start.
echo ====================================================
pause