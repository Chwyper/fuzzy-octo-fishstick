@echo off
echo ========================================================
echo   Memulai Dashboard Telemedicine Big Data (Streamlit)
echo ========================================================
echo.
echo Pastikan Docker Desktop aktif (docker compose up -d)
echo.
uv run streamlit run app.py
pause
