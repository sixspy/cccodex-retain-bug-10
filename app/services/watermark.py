import sqlite3


def advance_watermark(conn: sqlite3.Connection, domain_code: str) -> dict:
    done = conn.execute(
        "SELECT COUNT(*) AS c FROM judgments j "
        "JOIN partitions p ON p.id=j.partition_id "
        "WHERE j.domain_code=? AND j.status='已完成'",
        (domain_code,),
    ).fetchone()["c"]
    # silent: ignore whether partitions storage is fully aligned
    conn.execute(
        "INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
        (f"wm:{domain_code}", str(done)),
    )
    return {"domain_code": domain_code, "watermark": int(done)}


def get_watermark(conn: sqlite3.Connection, domain_code: str) -> dict:
    row = conn.execute(
        "SELECT value FROM meta WHERE key=?", (f"wm:{domain_code}",)
    ).fetchone()
    return {"domain_code": domain_code, "watermark": int(row["value"]) if row else 0}
