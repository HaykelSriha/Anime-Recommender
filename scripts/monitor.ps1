# ============================================================================
# PowerShell Monitoring Script
# ============================================================================
# Quick monitoring commands for Windows PowerShell users
#
# Usage:
#   .\monitor.ps1 logs          - View last 50 log lines
#   .\monitor.ps1 health        - Run health check
#   .\monitor.ps1 inspect       - Inspect warehouse
#   .\monitor.ps1 count         - Show anime count
#   .\monitor.ps1 stats         - Quick stats
# ============================================================================

param(
    [Parameter(Position=0)]
    [ValidateSet('logs', 'health', 'inspect', 'count', 'stats', '')]
    [string]$Command = ''
)

function Show-Usage {
    Write-Host ""
    Write-Host "Usage: .\monitor.ps1 [command]" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Available commands:" -ForegroundColor Yellow
    Write-Host "  logs      - View last 50 ETL log lines"
    Write-Host "  health    - Run system health check"
    Write-Host "  inspect   - Inspect warehouse contents"
    Write-Host "  count     - Show anime count"
    Write-Host "  stats     - Quick warehouse stats"
    Write-Host ""
}

function Show-Logs {
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "ETL LOGS - Last 50 Lines" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Cyan

    if (Test-Path "logs/etl/run_etl.log") {
        Get-Content "logs/etl/run_etl.log" -Tail 50
    } else {
        Write-Host "No logs found. Run ETL first: python scripts/run_etl.py --limit 20" -ForegroundColor Yellow
    }
}

function Show-Health {
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "RUNNING HEALTH CHECK" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Cyan
    python scripts/health_check.py
}

function Show-Inspect {
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "WAREHOUSE INSPECTION" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Cyan
    python scripts/inspect_warehouse.py
}

function Show-Count {
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "ANIME COUNT" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Cyan
    python -c "import duckdb; conn = duckdb.connect('warehouse/anime_dw.duckdb'); print('Total anime:', conn.execute('SELECT COUNT(*) FROM vw_anime_current').fetchone()[0]); conn.close()"
}

function Show-Stats {
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "QUICK WAREHOUSE STATS" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Cyan

    python -c @"
import duckdb
conn = duckdb.connect('warehouse/anime_dw.duckdb')
print('Anime:', conn.execute('SELECT COUNT(*) FROM vw_anime_current').fetchone()[0])
print('Metrics:', conn.execute('SELECT COUNT(*) FROM fact_anime_metrics').fetchone()[0])
print('Similarities:', conn.execute('SELECT COUNT(*) FROM fact_anime_similarity').fetchone()[0])
conn.close()
"@
}

# Main execution
switch ($Command) {
    'logs' { Show-Logs }
    'health' { Show-Health }
    'inspect' { Show-Inspect }
    'count' { Show-Count }
    'stats' { Show-Stats }
    default { Show-Usage }
}
