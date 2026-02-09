@echo off
REM ============================================================================
REM Windows Monitoring Script
REM ============================================================================
REM Quick monitoring commands for Windows users
REM
REM Usage:
REM   monitor_windows.bat logs          - View last 50 log lines
REM   monitor_windows.bat health        - Run health check
REM   monitor_windows.bat inspect       - Inspect warehouse
REM   monitor_windows.bat count         - Show anime count
REM ============================================================================

IF "%1"=="" (
    echo Usage: monitor_windows.bat [logs^|health^|inspect^|count]
    echo.
    echo Available commands:
    echo   logs      - View last 50 ETL log lines
    echo   health    - Run system health check
    echo   inspect   - Inspect warehouse contents
    echo   count     - Show anime count
    exit /b 0
)

IF "%1"=="logs" (
    echo ========================================
    echo ETL LOGS - Last 50 Lines
    echo ========================================
    powershell "Get-Content logs/etl/run_etl.log -Tail 50 -ErrorAction SilentlyContinue"
    IF ERRORLEVEL 1 (
        echo No logs found. Run ETL first: python scripts/run_etl.py --limit 20
    )
    exit /b 0
)

IF "%1"=="health" (
    echo ========================================
    echo RUNNING HEALTH CHECK
    echo ========================================
    python scripts/health_check.py
    exit /b 0
)

IF "%1"=="inspect" (
    echo ========================================
    echo WAREHOUSE INSPECTION
    echo ========================================
    python scripts/inspect_warehouse.py
    exit /b 0
)

IF "%1"=="count" (
    echo ========================================
    echo ANIME COUNT
    echo ========================================
    python -c "import duckdb; conn = duckdb.connect('warehouse/anime_dw.duckdb'); print('Total anime:', conn.execute('SELECT COUNT(*) FROM vw_anime_current').fetchone()[0]); conn.close()"
    exit /b 0
)

echo Unknown command: %1
echo Run "monitor_windows.bat" for usage
exit /b 1
