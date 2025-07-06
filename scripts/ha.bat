@echo off
REM HouseAccount - Windows用バッチファイル
REM 使用例: ha.bat process data/sample.csv

cd /d "%~dp0.."
python scripts/houseaccount.py %*