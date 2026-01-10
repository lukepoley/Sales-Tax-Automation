@echo off
TITLE NNOGC SQL Processor
SETLOCAL

:: 1. Check if Python is ready
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not detected. 
    echo Please run 'setup.bat' first and restart your computer.
    pause
    exit /b
)

:: 2. Run the Python Script
python "NNOG Sales Tax Refund.py"

:: 3. Check if Python finished successfully
if %errorlevel% equ 0 (
    echo.
    echo ====================================================
    echo   PROCESS FINISHED SUCCESSFULLY
    echo ====================================================
) else (
    echo.
    echo [ERROR] The Python script encountered an error. 
    echo Check the messages above for details.
)

pause
