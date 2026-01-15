@echo off
REM Запуск вебхука (uvicorn) и бота (polling) из venv
REM Убедитесь, что .env заполнен и venv создан/установлены зависимости

cd /d C:\Users\LPM\Desktop\cursor\telegram_sales
set PYTHON=.\.venv\Scripts\python.exe

REM Запускаем uvicorn в отдельном окне
start "uvicorn" %PYTHON% -m uvicorn webhook:app --host 0.0.0.0 --port 8000

REM Запускаем бота в отдельном окне
start "bot" %PYTHON% bot.py

