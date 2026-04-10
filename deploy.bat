@echo off
chcp 65001 >nul
echo ============================================
echo   部署到遠端伺服器
echo ============================================
echo.

:: ── 請修改以下設定 ──
set SERVER_IP=YOUR_SERVER_IP
set SERVER_USER=YOUR_USERNAME
set SERVER_PASS=YOUR_PASSWORD
set SHARE_PATH=\\%SERVER_IP%\D$
:: ────────────────────

if "%SERVER_IP%"=="YOUR_SERVER_IP" (
    echo [錯誤] 請先編輯此檔案，設定 SERVER_IP / SERVER_USER / SERVER_PASS
    pause
    exit /b 1
)

:: 測試連線
ping -n 1 -w 3000 %SERVER_IP% >nul 2>&1
if errorlevel 1 (
    echo [錯誤] 無法連線到 %SERVER_IP%
    echo 請確認電腦已開機且在同一網路
    pause
    exit /b 1
)
echo [OK] 伺服器已連線

:: 建立網路磁碟連線
echo [1/4] 連線到伺服器...
net use %SHARE_PATH% /user:%SERVER_USER% %SERVER_PASS% >nul 2>&1
if errorlevel 1 (
    echo [錯誤] 無法連線到伺服器的共用資料夾
    echo 請確認共用路徑正確，或手動複製檔案
    pause
    exit /b 1
)
echo [OK] 已連線到 %SHARE_PATH%

:: 建立目標資料夾
echo [2/4] 建立資料夾...
if not exist "%SHARE_PATH%\CelebrityPhotoDownloader" mkdir "%SHARE_PATH%\CelebrityPhotoDownloader"
if not exist "%SHARE_PATH%\CelebrityPhotos" mkdir "%SHARE_PATH%\CelebrityPhotos"

:: 複製檔案
echo [3/4] 複製程式檔案...
copy /y "%~dp0celebrity_downloader.py" "%SHARE_PATH%\CelebrityPhotoDownloader\" >nul
copy /y "%~dp0web_app.py" "%SHARE_PATH%\CelebrityPhotoDownloader\" >nul
copy /y "%~dp0requirements.txt" "%SHARE_PATH%\CelebrityPhotoDownloader\" >nul
copy /y "%~dp0setup.bat" "%SHARE_PATH%\CelebrityPhotoDownloader\" >nul
copy /y "%~dp0run.bat" "%SHARE_PATH%\CelebrityPhotoDownloader\" >nul
copy /y "%~dp0run_web.bat" "%SHARE_PATH%\CelebrityPhotoDownloader\" >nul
echo [OK] 檔案已複製

:: 提示遠端設定
echo [4/4] 完成！
echo.
echo ============================================
echo   檔案已部署到伺服器
echo.
echo   接下來請在伺服器上執行:
echo   1. 開啟 D:\CelebrityPhotoDownloader\
echo   2. 執行 setup.bat (安裝套件)
echo   3. 執行 run_web.bat (啟動 Web 版)
echo ============================================
pause
