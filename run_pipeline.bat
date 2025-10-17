@echo off
REM Navigate to project directory
cd /d "C:\Users\skywhyline\Desktop\project_stock"

REM Activate virtual environment
call venv\Scripts\activate

REM Run the main script and log output
python main.py >> logs\update.log 2>&1

