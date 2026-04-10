@echo off
start "" pythonw "%~dp0celebrity_downloader.py" 2>nul || python "%~dp0celebrity_downloader.py"
