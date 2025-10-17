@echo off
REM === переход в папку проекта ===
cd /d "C:\Users\skywhyline\Desktop\project_stock"

REM === активация виртуального окружения ===
call venv\Scripts\activate

REM === запуск пайплайна и запись логов ===
python main.py >> logs\update.log 2>&1

