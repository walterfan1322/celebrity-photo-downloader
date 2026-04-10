@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================
echo   明星照片下載器 Web 版
echo   關閉此視窗即停止伺服器
echo ============================================
python web_app.py
pause
