from app.core.db import get_db_connection


def get_llm_calls_summary() -> dict:
    sql = """
        SELECT
            COUNT(*) AS total_calls,
            SUM(CASE WHEN TRUNC(created_at) = TRUNC(SYSTIMESTAMP) THEN 1 ELSE 0 END) AS today_calls
        FROM ft_llm_calls
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone() or (0, 0)
            return {
                "total_calls": int(row[0] or 0),
                "today_calls": int(row[1] or 0),
            }
