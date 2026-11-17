"""落库进度水线与放行台账。

失败闭（本实现唯一选型，不提供影子草稿/原子切换的并存开关）：
**同库事务一次落成**。
- 进程内 RLock 串行化同一进程内的放行请求；
- 事务以 BEGIN IMMEDIATE 开启，立即取得 SQLite 库级保留写锁，跨进程/跨连接的
  并发放行在锁上排队，杜绝两侧放行双成功互盖；
- 水线写 meta 与放行台账写 watermark_ledgers 在同一事务内一次提交；
- 校验不通过（有待执行裁决 / 完成单与实物库存不同源 / 水线超前）时抛
  WatermarkBlocked：主事务整体回滚（水位与业务数据均不变），随后以独立短事务
  追加一条 action='拦截' 的台账并单独提交，保证失败留痕不污染主事务；
- 其余异常同样整体回滚，不写入任何水位变更。
"""

import sqlite3
import threading

# 单进程内串行放行；跨进程由 BEGIN IMMEDIATE 的库级写锁兜底。
_advance_lock = threading.RLock()


class WatermarkBlocked(Exception):
    """放行条件未满足：水线不得推进。HTTP 层映射为 409。"""


def _domain_exists(conn: sqlite3.Connection, domain_code: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM retain_policies WHERE domain_code = ? LIMIT 1",
        (domain_code,),
    ).fetchone()
    return row is not None


def _count_pending(conn: sqlite3.Connection, domain_code: str) -> int:
    """未齐条：待执行 / 执行中 / 已失败的裁决都算未闭环，放行必须为 0。"""
    return int(
        conn.execute(
            "SELECT COUNT(*) AS c FROM judgments "
            "WHERE domain_code = ? AND status IN ('待执行','执行中','已失败')",
            (domain_code,),
        ).fetchone()["c"]
    )


def _count_aligned(conn: sqlite3.Connection, domain_code: str) -> int:
    """同源判定：裁决已完成且实物分区存储态与裁决动作一致，才计入对齐数。

    完成标记（judgments.status）与实物库存（partitions.storage_state）在同一条
    SQL 内联合判定，任一不符都不计数，避免只看完成单导致水线超前。
    """
    return int(
        conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM judgments j
            JOIN partitions p ON p.id = j.partition_id
            WHERE j.domain_code = ?
              AND j.status = '已完成'
              AND (
                (j.decided_action = '归档' AND p.storage_state = '已归档')
                OR (j.decided_action = '销毁' AND p.storage_state = '已销毁')
              )
            """,
            (domain_code,),
        ).fetchone()["c"]
    )


def _count_total(conn: sqlite3.Connection, domain_code: str) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) AS c FROM judgments WHERE domain_code = ?",
            (domain_code,),
        ).fetchone()["c"]
    )


def _insert_ledger(
    conn: sqlite3.Connection,
    *,
    domain_code: str,
    action: str,
    ok: int,
    before: int,
    after: int,
    aligned: int,
    pending: int,
    reason: str,
) -> None:
    conn.execute(
        "INSERT INTO watermark_ledgers "
        "(domain_code, action, ok, watermark_before, watermark_after, "
        "aligned_count, pending_count, reason) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (domain_code, action, ok, before, after, aligned, pending, reason),
    )


def _record_blocked(
    db_path: str | None,
    *,
    domain_code: str,
    before: int,
    aligned: int,
    pending: int,
    reason: str,
) -> None:
    """拦截台账走独立短事务：主事务已回滚，失败留痕单独提交。"""
    # 延迟导入，避免与 app.db 的模块加载顺序耦合。
    from app.db import connect

    conn = connect(db_path)
    try:
        conn.isolation_level = None
        conn.execute("BEGIN IMMEDIATE")
        _insert_ledger(
            conn,
            domain_code=domain_code,
            action="拦截",
            ok=0,
            before=before,
            after=before,
            aligned=aligned,
            pending=pending,
            reason=reason,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def advance_watermark(
    conn: sqlite3.Connection, domain_code: str, db_path: str | None = None
) -> dict:
    """按已定失败闭推进一次水线。条件不齐则抛 WatermarkBlocked，水位不变。"""
    with _advance_lock:
        # 切到 autocommit，由本函数显式掌控 BEGIN/COMMIT/ROLLBACK 边界，
        # 避免 sqlite3 旧版隐式事务与显式 BEGIN 冲突。
        prev_isolation = conn.isolation_level
        conn.isolation_level = None
        # IMMEDIATE 立即占库级写锁：两侧并发放行在此排队，不会双成功互盖。
        conn.execute("BEGIN IMMEDIATE")
        try:
            if not _domain_exists(conn, domain_code):
                raise ValueError(f"主题域不存在：{domain_code}")

            before = get_watermark(conn, domain_code)["watermark"]
            pending = _count_pending(conn, domain_code)
            aligned = _count_aligned(conn, domain_code)
            total = _count_total(conn, domain_code)

            if pending > 0:
                reason = (
                    f"主题域 {domain_code} 尚有 {pending} 条裁决未闭环"
                    "（待执行/执行中/已失败），水线不得推进"
                )
                raise WatermarkBlocked(reason)

            if total > 0 and aligned != total:
                reason = (
                    f"完成单与实物库存不同源：已完成且实物对齐 {aligned} 条，"
                    f"裁决共 {total} 条；请先在分区对账核对错位分区"
                )
                raise WatermarkBlocked(reason)

            # 水卡跳数即拦：水线只能随对齐数单调前移，不允许超前或回退。
            if aligned < before:
                reason = (
                    f"水线超前拦截：当前水线 {before}，实物对齐仅 {aligned} 条，"
                    "水线不得高于同源对齐数"
                )
                raise WatermarkBlocked(reason)

            after = aligned
            if after == before:
                reason = "无新增已对齐裁决，水线维持不变"
                _insert_ledger(
                    conn,
                    domain_code=domain_code,
                    action="拦截",
                    ok=0,
                    before=before,
                    after=after,
                    aligned=aligned,
                    pending=pending,
                    reason=reason,
                )
                conn.commit()
                return {
                    "domain_code": domain_code,
                    "watermark": before,
                    "advanced": False,
                    "aligned_count": aligned,
                    "pending_count": pending,
                    "reason": reason,
                }

            conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (f"wm:{domain_code}", str(after)),
            )
            _insert_ledger(
                conn,
                domain_code=domain_code,
                action="放行",
                ok=1,
                before=before,
                after=after,
                aligned=aligned,
                pending=pending,
                reason=f"水线 {before} → {after}，同库事务一次落成",
            )
            conn.commit()
            return {
                "domain_code": domain_code,
                "watermark": after,
                "advanced": True,
                "watermark_before": before,
                "aligned_count": aligned,
                "pending_count": pending,
                "reason": f"水线 {before} → {after}",
            }
        except WatermarkBlocked:
            # 主事务整体回滚：水位与业务数据保持放行前状态。
            conn.rollback()
            # 拦截事实以独立短事务留痕，失败不掩盖原始拦截原因。
            try:
                _record_blocked(
                    db_path,
                    domain_code=domain_code,
                    before=before,
                    aligned=aligned,
                    pending=pending,
                    reason=reason,
                )
            except Exception:
                pass
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.isolation_level = prev_isolation


def get_watermark(conn: sqlite3.Connection, domain_code: str) -> dict:
    row = conn.execute(
        "SELECT value FROM meta WHERE key=?", (f"wm:{domain_code}",)
    ).fetchone()
    return {"domain_code": domain_code, "watermark": int(row["value"]) if row else 0}


def list_watermark_ledgers(
    conn: sqlite3.Connection, domain_code: str | None = None
) -> list[dict]:
    if domain_code:
        rows = conn.execute(
            "SELECT id, domain_code, action, ok, watermark_before, watermark_after, "
            "aligned_count, pending_count, reason, created_at "
            "FROM watermark_ledgers WHERE domain_code = ? ORDER BY id DESC",
            (domain_code,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, domain_code, action, ok, watermark_before, watermark_after, "
            "aligned_count, pending_count, reason, created_at "
            "FROM watermark_ledgers ORDER BY id DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def list_domain_progress(conn: sqlite3.Connection) -> list[dict]:
    """逐主题域汇总：现行水线、同源对齐数、未齐条数、裁决总数，供水卡展示。"""
    domains = conn.execute(
        "SELECT DISTINCT domain_code, domain_name FROM retain_policies "
        "WHERE active = 1 ORDER BY domain_code"
    ).fetchall()
    items: list[dict] = []
    for d in domains:
        code = d["domain_code"]
        items.append(
            {
                "domain_code": code,
                "domain_name": d["domain_name"],
                "watermark": get_watermark(conn, code)["watermark"],
                "aligned_count": _count_aligned(conn, code),
                "pending_count": _count_pending(conn, code),
                "judgment_count": _count_total(conn, code),
            }
        )
    return items
