"""
System Health Check
===================
Quick health check for the anime data warehouse
"""

import sys
from pathlib import Path
import duckdb
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import WAREHOUSE_DB_PATH

def health_check():
    """Run comprehensive health check"""

    print("=" * 60)
    print("ANIME DATA WAREHOUSE - HEALTH CHECK")
    print("=" * 60)

    conn = duckdb.connect(str(WAREHOUSE_DB_PATH))

    checks = {
        'database_accessible': False,
        'schema_valid': False,
        'has_anime': False,
        'has_metrics': False,
        'has_similarities': False,
        'data_fresh': False,
        'quality_passing': False
    }

    try:
        # 1. Check database accessible
        try:
            conn.execute("SELECT 1")
            checks['database_accessible'] = True
            print("[OK] Database accessible")
        except Exception as e:
            print(f"[FAIL] Database not accessible: {e}")

        # 2. Check schema valid
        try:
            required_tables = ['dim_anime', 'fact_anime_metrics', 'vw_anime_current']
            existing_tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]

            if all(t in existing_tables for t in required_tables):
                checks['schema_valid'] = True
                print(f"[OK] Schema valid ({len(existing_tables)} tables)")
            else:
                print("[FAIL] Schema incomplete")
        except Exception as e:
            print(f"[FAIL] Schema check failed: {e}")

        # 3. Check has anime
        try:
            anime_count = conn.execute("SELECT COUNT(*) FROM vw_anime_current").fetchone()[0]
            checks['has_anime'] = anime_count > 0
            if checks['has_anime']:
                print(f"[OK] Anime data present ({anime_count} anime)")
            else:
                print("[FAIL] No anime data")
        except Exception as e:
            print(f"[FAIL] Anime check failed: {e}")

        # 4. Check has metrics
        try:
            metrics_count = conn.execute("SELECT COUNT(*) FROM fact_anime_metrics").fetchone()[0]
            checks['has_metrics'] = metrics_count > 0
            if checks['has_metrics']:
                print(f"[OK] Metrics present ({metrics_count} records)")
            else:
                print("[FAIL] No metrics data")
        except Exception as e:
            print(f"[FAIL] Metrics check failed: {e}")

        # 5. Check has similarities
        try:
            sim_count = conn.execute("SELECT COUNT(*) FROM fact_anime_similarity").fetchone()[0]
            checks['has_similarities'] = sim_count > 0
            if checks['has_similarities']:
                print(f"[OK] Similarity scores computed ({sim_count} scores)")
            else:
                print("[WARN] No similarity scores (run similarity computation)")
        except Exception as e:
            print(f"[FAIL] Similarity check failed: {e}")

        # 6. Check data freshness (within last 7 days)
        try:
            latest = conn.execute("SELECT MAX(snapshot_date) FROM fact_anime_metrics").fetchone()[0]
            if latest:
                days_old = (datetime.now().date() - latest).days
                checks['data_fresh'] = days_old <= 7
                if checks['data_fresh']:
                    print(f"[OK] Data fresh (last update: {latest})")
                else:
                    print(f"[WARN] Data stale ({days_old} days old)")
            else:
                print("[FAIL] No snapshot dates")
        except Exception as e:
            print(f"[FAIL] Freshness check failed: {e}")

        # 7. Quick quality check
        try:
            null_check = conn.execute("SELECT COUNT(*) FROM dim_anime WHERE title IS NULL").fetchone()[0]
            dup_check = conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT anime_id, COUNT(*) as cnt
                    FROM dim_anime
                    WHERE is_current = TRUE
                    GROUP BY anime_id
                    HAVING cnt > 1
                )
            """).fetchone()[0]

            checks['quality_passing'] = (null_check == 0 and dup_check == 0)
            if checks['quality_passing']:
                print("[OK] Quality checks passing")
            else:
                print(f"[FAIL] Quality issues: {null_check} nulls, {dup_check} duplicates")
        except Exception as e:
            print(f"[FAIL] Quality check failed: {e}")

    finally:
        conn.close()

    # Summary
    print("=" * 60)
    passed = sum(checks.values())
    total = len(checks)
    health_score = (passed / total) * 100

    print(f"HEALTH SCORE: {health_score:.0f}% ({passed}/{total} checks passed)")

    if health_score == 100:
        print("STATUS: HEALTHY")
    elif health_score >= 70:
        print("STATUS: DEGRADED")
    else:
        print("STATUS: CRITICAL")

    print("=" * 60)

    return health_score >= 70


if __name__ == '__main__':
    healthy = health_check()
    sys.exit(0 if healthy else 1)
