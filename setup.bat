@echo off
chcp 65001 >nul
echo ============================================
echo   明星照片下載器 - 安裝程式
echo ============================================
echo.

:: 檢查 Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [錯誤] 找不到 Python，請先安裝 Python 3.8+
    echo 下載: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [1/2] 安裝套件...
pip install -r "%~dp0requirements.txt"
if errorlevel 1 (
    echo [錯誤] 套件安裝失敗
    pause
    exit /b 1
)

echo.
echo [2/2] 建立資料夾...
if not exist "%~dp0Photos" mkdir "%~dp0Photos"
if not exist "%~dp0data" mkdir "%~dp0data"

echo.
echo ============================================
echo   安裝完成！請執行 run_web.bat 啟動程式
echo ============================================
pause
