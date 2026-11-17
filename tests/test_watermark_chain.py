"""落库进度水线：拦截、同源判定、放行台账、占锁并发、失败回滚。"""

import os
import threading

import pytest

from app.config import Config
from app.db import connect, get_db
from app.services.watermark import (
    WatermarkBlocked,
    advance_watermark,
    get_watermark,
    list_watermark_ledgers,
)


@pytest.fixture()
def db_path(client):
    # client fixture 已完成建库并把 DATABASE_PATH 指向临时库
    return os.environ["DATABASE_PATH"]


def _scan_and_pending(authed, as_of="2026-11-16"):
    authed.post("/api/scan-jobs", json={"as_of_date": as_of})
    return authed.get(
        "/api/judgments", params={"status": "待执行"}
    ).json()["items"]


def _execute(authed, judgment_id):
    r = authed.post(f"/api/judgments/{judgment_id}/execute")
    assert r.status_code == 200
    return r.json()["run"]


def test_blocked_when_pending_judgments_remain(authed):
    pending = _scan_and_pending(authed)
    assert pending
    domain = pending[0]["domain_code"]

    r = authed.post(f"/api/watermarks/{domain}/advance")
    # 失败给中文提示
    assert r.status_code == 409
    assert "未闭环" in r.json()["detail"]

    # 水位未被推进
    wm = authed.get(f"/api/watermarks/{domain}").json()
    assert wm["watermark"] == 0

    # 放行台账能看见未齐条（拦截留痕）
    ledgers = authed.get(
        "/api/watermark-ledgers", params={"domain_code": domain}
    ).json()["items"]
    blocked = [l for l in ledgers if l["action"] == "拦截"]
    assert blocked and blocked[0]["ok"] == 0
    assert blocked[0]["pending_count"] >= 1
    assert blocked[0]["watermark_before"] == blocked[0]["watermark_after"] == 0


def test_advance_after_all_aligned_and_ledger_records(authed):
    pending = _scan_and_pending(authed)
    # 选一个主题域，把它的裁决全部执行完
    domain = pending[0]["domain_code"]
    todo = [j for j in pending if j["domain_code"] == domain]
    for j in todo:
        _execute(authed, j["id"])

    r = authed.post(f"/api/watermarks/{domain}/advance")
    assert r.status_code == 200, r.text
    item = r.json()["item"]
    assert item["advanced"] is True
    assert item["watermark"] == len(todo)
    assert item["watermark_before"] == 0

    ledgers = authed.get(
        "/api/watermark-ledgers", params={"domain_code": domain}
    ).json()["items"]
    passed = [l for l in ledgers if l["action"] == "放行"]
    assert len(passed) == 1
    assert passed[0]["ok"] == 1
    assert passed[0]["watermark_after"] == len(todo)

    # 再放行一次：无新增对齐，水线维持，不重复跳数
    again = authed.post(f"/api/watermarks/{domain}/advance")
    assert again.status_code == 200
    assert again.json()["item"]["advanced"] is False
    assert authed.get(f"/api/watermarks/{domain}").json()["watermark"] == len(todo)


def _seed_completed_judgment(db_path, domain, aligned: bool):
    """构造一条已完成裁决；aligned=False 时分区实物保持在线（只改完成标记）。"""
    with get_db(db_path) as conn:
        conn.execute(
            "INSERT INTO scan_jobs (as_of_date, status, message) "
            "VALUES ('2026-11-16','已完成','seed')"
        )
        job_id = conn.execute("SELECT last_insert_rowid() AS i").fetchone()["i"]
        part = conn.execute(
            "SELECT * FROM partitions WHERE domain_code=? LIMIT 1", (domain,)
        ).fetchone()
        action = "销毁"
        conn.execute(
            "INSERT INTO judgments "
            "(scan_job_id, partition_id, domain_code, biz_date, policy_version, "
            "keep_days, decided_action, status) "
            "VALUES (?,?,?,?,?,?,'销毁','已完成')",
            (job_id, part["id"], domain, part["biz_date"], 1, 3),
        )
        if aligned:
            conn.execute(
                "UPDATE partitions SET storage_state='已销毁', row_count=0 WHERE id=?",
                (part["id"],),
            )
        else:
            assert dict(part)["storage_state"] == "在线"


def test_completed_judgment_without_physical_partition_is_blocked(db_path):
    """完成单与实物库存不同源：只改完成标记、实物未齐，水卡跳数即拦。"""
    _seed_completed_judgment(db_path, "events", aligned=False)

    conn = connect(db_path)
    try:
        with pytest.raises(WatermarkBlocked) as exc:
            advance_watermark(conn, "events", db_path=db_path)
        assert "不同源" in str(exc.value)
        # 水位仍是 0
        assert get_watermark(connect(db_path), "events")["watermark"] == 0
    finally:
        conn.close()

    # 拦截台账已留痕
    with get_db(db_path) as check:
        ledgers = list_watermark_ledgers(check, domain_code="events")
    assert any(l["action"] == "拦截" and "不同源" in l["reason"] for l in ledgers)


def test_watermark_never_runs_ahead_of_aligned_count(db_path):
    """水线已超前（meta 被污染）时，放行必须拦下而不是顺着错误水位继续。"""
    with get_db(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES('wm:orders','9')"
        )

    conn = connect(db_path)
    try:
        with pytest.raises(WatermarkBlocked) as exc:
            advance_watermark(conn, "orders", db_path=db_path)
        assert "超前" in str(exc.value)
    finally:
        conn.close()

    with get_db(db_path) as check:
        assert get_watermark(check, "orders")["watermark"] == 9  # 原值未被改写
        ledgers = list_watermark_ledgers(check, domain_code="orders")
    assert any("超前" in l["reason"] for l in ledgers)


def test_concurrent_advances_are_lock_serialized(db_path):
    """两侧并发放行占锁写入：只允许一次成功跳数，不允许双成功互盖。"""
    _seed_completed_judgment(db_path, "events", aligned=True)

    results: list[dict] = []
    barrier = threading.Barrier(2)

    def worker():
        conn = connect(db_path, timeout=10)
        barrier.wait()  # 尽量同时发起
        try:
            try:
                item = advance_watermark(conn, "events", db_path=db_path)
                results.append({"ok": True, "advanced": item["advanced"]})
            except WatermarkBlocked as exc:
                results.append({"ok": False, "reason": str(exc)})
        finally:
            conn.close()

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert len(results) == 2
    advanced = [r for r in results if r.get("advanced")]
    # 只有一次真正把水线从 0 推到 1
    assert len(advanced) == 1
    with get_db(db_path) as check:
        assert get_watermark(check, "events")["watermark"] == 1
        passed = [
            l
            for l in list_watermark_ledgers(check, domain_code="events")
            if l["action"] == "放行"
        ]
    assert len(passed) == 1


def test_blocked_rollback_keeps_all_business_data(authed):
    """拦截后主事务回滚：除拦截台账外，不产生任何水位或业务数据变更。"""
    pending = _scan_and_pending(authed)
    domain = pending[0]["domain_code"]
    authed.post(f"/api/watermarks/{domain}/advance")

    ledgers_before = authed.get("/api/watermark-ledgers").json()["items"]
    parts_before = authed.get("/api/partitions").json()["items"]
    judgments_before = authed.get("/api/judgments").json()["items"]

    r = authed.post(f"/api/watermarks/{domain}/advance")
    assert r.status_code == 409

    parts_after = authed.get("/api/partitions").json()["items"]
    judgments_after = authed.get("/api/judgments").json()["items"]
    ledgers_after = authed.get("/api/watermark-ledgers").json()["items"]

    assert parts_after == parts_before
    assert judgments_after == judgments_before
    # 仅多出一条拦截台账
    assert len(ledgers_after) == len(ledgers_before) + 1


def test_progress_overview_groups_cards(authed):
    _scan_and_pending(authed)
    items = authed.get("/api/watermarks/progress/overview").json()["items"]
    codes = {i["domain_code"] for i in items}
    assert {"orders", "payments", "events", "inventory"} <= codes
    card = next(i for i in items if i["pending_count"] > 0)
    assert card["watermark"] == 0
    assert card["judgment_count"] >= card["aligned_count"]
