"""端到端自测：跨日窗口、迟到/宽限、重复修订、修订替代、缺失、未提交、结果失效。"""
from __future__ import annotations

import pytest
from datetime import datetime, timedelta, timezone
from fastapi.testclient import TestClient

from app.main import create_app

DAY = "2026-11-20"      # 分区日期
BIZ = "2026-11-21"      # 日报业务日期（offset=1 -> 依赖 DAY）
AS_OF = "2026-11-21T06:00:00+00:00"


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def mk_partition(hour: int, submitted_min: int = 55, revision: int = 1,
                 deadline_min: int = 60, day: str = DAY) -> dict:
    start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    h = start + timedelta(hours=hour)
    return {
        "partition_hour": h.isoformat(),
        "planned_deadline": (h + timedelta(minutes=deadline_min)).isoformat(),
        "submitted_at": (h + timedelta(minutes=submitted_min)).isoformat(),
        "revision": revision,
    }


def full_day(day: str = DAY, skip: set[int] | None = None,
             overrides: dict[int, list[dict]] | None = None) -> list[dict]:
    """生成某一日 24 个准时分区，可跳过/覆盖指定小时。"""
    parts = []
    for h in range(24):
        if skip and h in skip:
            continue
        if overrides and h in overrides:
            parts.extend(overrides[h])
        else:
            parts.append(mk_partition(h, day=day))
    return parts


def setup_catalog(client: "TestClient", partitions: list[dict],
                  reports: list[dict] | None = None) -> int:
    cid = client.post("/api/catalogs", json={"name": "t"}).json()["id"]
    if partitions:
        r = client.post(f"/api/catalogs/{cid}/partitions", json={"partitions": partitions})
        assert r.status_code == 201, r.text
    for rep in (reports if reports is not None else
                [{"name": "日报A", "offset_days": 1, "grace_minutes": 30}]):
        r = client.post(f"/api/catalogs/{cid}/reports", json=rep)
        assert r.status_code == 201, r.text
    return cid


def run_check(client: "TestClient", cid: int, business_date: str = BIZ,
              as_of: str = AS_OF) -> dict:
    r = client.post(f"/api/catalogs/{cid}/checks",
                    json={"business_date": business_date, "as_of": as_of})
    assert r.status_code == 201, r.text
    return r.json()


# ---------- 跨日 ----------

def test_cross_day_window_and_watermark(client):
    """offset=1 的日报在业务日 11-21 应使用 11-20 的 24 个分区，全部准时就绪。"""
    cid = setup_catalog(client, full_day())
    check = run_check(client, cid)
    res = check["results"][0]
    assert res["status"] == "READY"
    assert res["window_start"] == "2026-11-20T00:00:00+00:00"
    assert res["window_end"] == "2026-11-20T23:00:00+00:00"
    assert res["watermark_hour"] == "2026-11-20T23:00:00+00:00"
    assert res["block_reason"] is None


def test_offset_two_uses_day_before_yesterday(client):
    """offset=2 跨两日：11-21 的日报依赖 11-19 的分区；只传 11-20 会在首小时缺失阻断。"""
    cid = setup_catalog(client, full_day(),
                        reports=[{"name": "回顾报", "offset_days": 2, "grace_minutes": 30}])
    res = run_check(client, cid)["results"][0]
    assert res["status"] == "BLOCKED"
    assert res["block_reason"] == "MISSING"
    assert res["block_hour"] == "2026-11-19T00:00:00+00:00"
    assert res["watermark_hour"] is None


# ---------- 迟到与宽限 ----------

def test_late_beyond_grace_blocks(client):
    """13 点分区迟到 50 分钟 > 宽限 30：水位停在 12 点，阻断原因为 LATE。"""
    parts = full_day(overrides={13: [mk_partition(13, submitted_min=110)]})  # 01:50 提交
    cid = setup_catalog(client, parts)
    res = run_check(client, cid)["results"][0]
    assert res["status"] == "BLOCKED"
    assert res["block_reason"] == "LATE"
    assert res["block_hour"] == "2026-11-20T13:00:00+00:00"
    assert res["watermark_hour"] == "2026-11-20T12:00:00+00:00"
    assert "宽限 30 分钟" in res["block_message"]


def test_late_within_grace_usable(client):
    """同样的迟到 50 分钟，宽限 60 则可用，但逐小时明细中标记为宽限内迟到。"""
    parts = full_day(overrides={13: [mk_partition(13, submitted_min=110)]})
    cid = setup_catalog(client, parts,
                        reports=[{"name": "日报A", "offset_days": 1, "grace_minutes": 60}])
    res = run_check(client, cid)["results"][0]
    assert res["status"] == "READY"
    assert res["watermark_hour"] == "2026-11-20T23:00:00+00:00"
    d13 = res["details"][13]
    assert d13["state"] == "late_within_grace" and d13["usable"] is True
    assert res["counts"]["late_within_grace"] == 1


# ---------- 重复提交 / 修订替代 ----------

def test_duplicate_revision_blocks(client):
    """09 点同一修订被提交两次：阻断，原因 DUPLICATE。"""
    parts = full_day(overrides={9: [mk_partition(9), mk_partition(9)]})
    cid = setup_catalog(client, parts)
    res = run_check(client, cid)["results"][0]
    assert res["status"] == "BLOCKED"
    assert res["block_reason"] == "DUPLICATE"
    assert res["block_hour"] == "2026-11-20T09:00:00+00:00"
    assert res["watermark_hour"] == "2026-11-20T08:00:00+00:00"
    # 分区列表中两条都标记 duplicate
    plist = client.get(f"/api/catalogs/{cid}/partitions").json()["partitions"]
    dup = [p for p in plist if p["partition_hour"].startswith("2026-11-20T09")]
    assert len(dup) == 2 and all(p["status"] == "duplicate" for p in dup)


def test_superseded_revision_not_blocking(client):
    """05 点 r1 迟到 2 小时，r2 准时：只有 r2 生效，r1 标记为已被替代，不阻断。"""
    parts = full_day(overrides={5: [mk_partition(5, submitted_min=180, revision=1),
                                    mk_partition(5, submitted_min=55, revision=2)]})
    cid = setup_catalog(client, parts)
    res = run_check(client, cid)["results"][0]
    assert res["status"] == "READY"
    d5 = res["details"][5]
    assert d5["revision"] == 2 and d5["superseded_count"] == 1 and d5["usable"] is True
    plist = client.get(f"/api/catalogs/{cid}/partitions").json()["partitions"]
    r1 = [p for p in plist if p["partition_hour"].startswith("2026-11-20T05")
          and p["revision"] == 1][0]
    assert r1["status"] == "superseded"
    assert "late" in r1["flags"]  # 迟到事实仍保留在标记中


# ---------- 缺失 / 截至前未提交 ----------

def test_missing_partition_blocks(client):
    """18 点无任何提交：水位停在 17 点，原因 MISSING。"""
    cid = setup_catalog(client, full_day(skip={18}))
    res = run_check(client, cid)["results"][0]
    assert res["status"] == "BLOCKED"
    assert res["block_reason"] == "MISSING"
    assert res["block_hour"] == "2026-11-20T18:00:00+00:00"
    assert res["watermark_hour"] == "2026-11-20T17:00:00+00:00"


def test_pending_after_as_of_blocks(client):
    """22 点分区在截至时刻之后才提交：按“截至前未提交”阻断。"""
    parts = full_day(overrides={22: [{
        "partition_hour": "2026-11-20T22:00:00+00:00",
        "planned_deadline": "2026-11-20T23:00:00+00:00",
        "submitted_at": "2026-11-21T10:00:00+00:00",
        "revision": 1,
    }]})
    cid = setup_catalog(client, parts)
    res = run_check(client, cid)["results"][0]
    assert res["status"] == "BLOCKED"
    assert res["block_reason"] == "PENDING"
    assert res["block_hour"] == "2026-11-20T22:00:00+00:00"
    assert res["watermark_hour"] == "2026-11-20T21:00:00+00:00"


# ---------- 结果失效 ----------

def test_check_stale_after_catalog_update(client):
    """目录更新（上传分区/改依赖）后，旧核查结果标记为已失效。"""
    cid = setup_catalog(client, full_day())
    check = run_check(client, cid)
    checks = client.get(f"/api/catalogs/{cid}/checks").json()["checks"]
    assert checks[0]["stale"] is False

    # 上传新分区 -> 旧核查失效
    client.post(f"/api/catalogs/{cid}/partitions",
                json={"partitions": [mk_partition(0, day="2026-11-21")]})
    checks = client.get(f"/api/catalogs/{cid}/checks").json()["checks"]
    assert checks[0]["stale"] is True
    detail = client.get(f"/api/checks/{check['id']}").json()
    assert detail["stale"] is True

    # 新核查基于新版本，恢复有效
    check2 = run_check(client, cid)
    checks = client.get(f"/api/catalogs/{cid}/checks").json()["checks"]
    by_id = {c["id"]: c for c in checks}
    assert by_id[check2["id"]]["stale"] is False
    assert by_id[check["id"]]["stale"] is True

    # 修改日报依赖 -> 同样失效
    client.post(f"/api/catalogs/{cid}/reports",
                json={"name": "日报B", "offset_days": 1, "grace_minutes": 0})
    checks = client.get(f"/api/catalogs/{cid}/checks").json()["checks"]
    assert all(c["stale"] for c in checks)


# ---------- 演示数据 ----------

def test_demo_dataset_story(client):
    """演示目录：运营日报被重复提交阻断、设备日报被超宽限迟到阻断、前日回顾报就绪。"""
    demo = client.post("/api/demo").json()
    cid = demo["catalog_id"]
    check = run_check(client, cid, demo["business_date"], demo["as_of"])
    by_name = {r["report_name"]: r for r in check["results"]}

    ops = by_name["运营日报"]          # 宽限 60：07 点迟到 50 分放行，13 点重复提交阻断
    assert ops["status"] == "BLOCKED" and ops["block_reason"] == "DUPLICATE"
    assert ops["block_hour"].endswith("T13:00:00+00:00")
    assert ops["watermark_hour"].endswith("T12:00:00+00:00")
    states = {d["hour"][11:13]: d["state"] for d in ops["details"]}
    assert states["07"] == "late_within_grace"
    assert states["18"] == "missing"
    assert states["22"] == "pending"
    d5 = [d for d in ops["details"] if d["hour"][11:13] == "05"][0]
    assert d5["superseded_count"] == 1 and d5["revision"] == 2

    dev = by_name["设备日报"]          # 宽限 30：07 点迟到 50 分超宽限阻断
    assert dev["status"] == "BLOCKED" and dev["block_reason"] == "LATE"
    assert dev["block_hour"].endswith("T07:00:00+00:00")
    assert dev["watermark_hour"].endswith("T06:00:00+00:00")

    retro = by_name["前日回顾报"]       # offset=2：前天分区全部准时
    assert retro["status"] == "READY"
    assert retro["watermark_hour"].endswith("T23:00:00+00:00")


# ---------- 输入校验 ----------

def test_validation_errors(client):
    cid = setup_catalog(client, [])
    r = client.post(f"/api/catalogs/{cid}/partitions",
                    json={"partitions": [{"partition_hour": "not-a-time",
                                          "planned_deadline": "2026-11-20T01:00:00Z",
                                          "submitted_at": "2026-11-20T00:55:00Z"}]})
    assert r.status_code == 422
    r = client.post(f"/api/catalogs/{cid}/checks",
                    json={"business_date": "2026/11/21", "as_of": AS_OF})
    assert r.status_code == 422
    assert client.get("/api/catalogs/999").status_code == 404


# ---------- 重建排障目录：输入快照 ----------

def _snapshot_rows(client, check_id):
    """直接查库读取核查快照（白盒断言用）。"""
    import sqlite3
    db_path = client.app.state.db_path
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        reports = [dict(r) for r in conn.execute(
            "SELECT * FROM check_input_reports WHERE check_id=? ORDER BY position",
            (check_id,))]
        parts = [dict(r) for r in conn.execute(
            "SELECT * FROM check_input_partitions WHERE check_id=? ORDER BY id",
            (check_id,))]
        return reports, parts, conn.execute(
            "SELECT COUNT(*) FROM catalogs").fetchone()[0]
    finally:
        conn.close()


def test_check_persists_input_snapshot(client):
    """运行核查即保存依赖与所涉小时的全部提交，快照小时仅限窗口并集。"""
    parts = full_day()
    parts.append(mk_partition(3, day="2026-11-19"))  # 窗口外的小时不应进入快照
    cid = setup_catalog(client, parts, reports=[
        {"name": "日报A", "offset_days": 1, "grace_minutes": 30},
        {"name": "日报B", "offset_days": 1, "grace_minutes": 60},
    ])
    check = run_check(client, cid)
    reports, snap_parts, _ = _snapshot_rows(client, check["id"])
    assert [r["report_name"] for r in reports] == ["日报A", "日报B"]
    assert [r["grace_minutes"] for r in reports] == [30, 60]
    hours = {p["partition_hour"] for p in snap_parts}
    assert len(hours) == 24                      # 两个日报共用窗口，只存一组小时
    assert "2026-11-19T03:00:00+00:00" not in hours
    assert len(snap_parts) == 24                 # 每小时一条准时记录，不重复复制
    detail = client.get(f"/api/checks/{check['id']}").json()
    assert detail["has_snapshot"] is True


def test_snapshot_keeps_duplicates_and_low_revisions(client):
    """同一小时内的重复提交与较低修订号记录在快照中全部保留。"""
    parts = full_day(overrides={
        5: [mk_partition(5, submitted_min=180, revision=1),
            mk_partition(5, submitted_min=55, revision=2)],
        9: [mk_partition(9), mk_partition(9)],
    })
    cid = setup_catalog(client, parts)
    check = run_check(client, cid)
    _, snap_parts, _ = _snapshot_rows(client, check["id"])
    h5 = [p for p in snap_parts if p["partition_hour"].startswith("2026-11-20T05")]
    h9 = [p for p in snap_parts if p["partition_hour"].startswith("2026-11-20T09")]
    assert sorted(p["revision"] for p in h5) == [1, 2]
    assert len(h9) == 2 and {p["revision"] for p in h9} == {1}


def test_rebuild_reproduces_results_after_source_upload_and_reset(client):
    """源目录补传、再清空后，仍能从原核查快照重建出完全相同的新目录与结论。"""
    parts = full_day(overrides={
        5: [mk_partition(5, submitted_min=180, revision=1),
            mk_partition(5, submitted_min=55, revision=2)],
        9: [mk_partition(9), mk_partition(9)],
    })
    cid = setup_catalog(client, parts, reports=[
        {"name": "运营日报", "offset_days": 1, "grace_minutes": 60},
        {"name": "设备日报", "offset_days": 1, "grace_minutes": 30},
        {"name": "前日回顾报", "offset_days": 2, "grace_minutes": 30},
    ])
    check = run_check(client, cid)

    # 源目录补传（并发/后续补传场景）：新增 09 点 r3，旧快照不应受影响
    client.post(f"/api/catalogs/{cid}/partitions",
                json={"partitions": [mk_partition(9, revision=3)]})
    r = client.post(f"/api/checks/{check['id']}/rebuild", json={"name": "排障目录-补传后"})
    assert r.status_code == 201, r.text
    rebuilt = r.json()
    assert rebuilt["catalog_id"] != cid
    assert rebuilt["id"] != check["id"]
    assert rebuilt["results"] == check["results"]   # 逐日报逐小时完全一致

    # 新目录的依赖、分区（含重复与低修订）与结论已一并保存
    new_parts = client.get(f"/api/catalogs/{rebuilt['catalog_id']}/partitions").json()["partitions"]
    assert len(new_parts) == 26                      # 24 小时 + 05 点 r1/r2 + 09 点两条 r1
    h9 = [p for p in new_parts if p["partition_hour"].startswith("2026-11-20T09")]
    # 核查之后补传的 r3 不在快照里：重建复现的是核查当时的目录
    assert sorted(p["revision"] for p in h9) == [1, 1]
    new_reports = client.get(f"/api/catalogs/{rebuilt['catalog_id']}/reports").json()["reports"]
    assert [(x["name"], x["offset_days"], x["grace_minutes"]) for x in new_reports] == [
        ("运营日报", 1, 60), ("设备日报", 1, 30), ("前日回顾报", 2, 30)]
    new_check = client.get(f"/api/checks/{rebuilt['id']}").json()
    assert new_check["business_date"] == check["business_date"]
    assert new_check["as_of"] == check["as_of"]
    assert new_check["rebuilt_from_check_id"] == check["id"]
    assert new_check["has_snapshot"] is True

    # 清空源目录后再次重建：仍可复现原结论
    client.post(f"/api/catalogs/{cid}/reset")
    r2 = client.post(f"/api/checks/{check['id']}/rebuild", json={})
    assert r2.status_code == 201, r2.text
    assert r2.json()["results"] == check["results"]
    assert r2.json()["copied_partitions"] == 26


def test_rebuild_old_check_without_snapshot_returns_409(client):
    """旧核查未保存输入时，明确返回不可重建说明。"""
    cid = setup_catalog(client, full_day())
    check = run_check(client, cid)
    import sqlite3
    conn = sqlite3.connect(client.app.state.db_path)
    try:
        conn.execute("UPDATE checks SET snapshot_saved=0 WHERE id=?", (check["id"],))
        conn.execute("DELETE FROM check_input_reports WHERE check_id=?", (check["id"],))
        conn.execute("DELETE FROM check_input_partitions WHERE check_id=?", (check["id"],))
        conn.commit()
    finally:
        conn.close()
    r = client.post(f"/api/checks/{check['id']}/rebuild", json={})
    assert r.status_code == 409
    assert "无法重建" in r.json()["detail"]
    detail = client.get(f"/api/checks/{check['id']}").json()
    assert detail["has_snapshot"] is False
    assert client.post("/api/checks/99999/rebuild", json={}).status_code == 404


def test_rebuild_failure_leaves_no_catalog_or_copied_rows(client, monkeypatch):
    """重建写入中途失败：不保存新目录，也不留下任何复制记录或新核查。"""
    import app.main as main_mod
    cid = setup_catalog(client, full_day())
    check = run_check(client, cid)
    before_catalogs = len(client.get("/api/catalogs").json()["catalogs"])
    before_checks = len(client.get(f"/api/catalogs/{cid}/checks").json()["checks"])

    real = main_mod.create_check_record

    def boom(*args, **kwargs):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(main_mod, "create_check_record", boom)
    r = client.post(f"/api/checks/{check['id']}/rebuild", json={"name": "应回滚"})
    monkeypatch.setattr(main_mod, "create_check_record", real)
    assert r.status_code == 500
    catalogs = client.get("/api/catalogs").json()["catalogs"]
    assert len(catalogs) == before_catalogs
    assert all(c["name"] != "应回滚" for c in catalogs)
    assert len(client.get(f"/api/catalogs/{cid}/checks").json()["checks"]) == before_checks

    # 回滚后仍可正常重建
    ok = client.post(f"/api/checks/{check['id']}/rebuild", json={})
    assert ok.status_code == 201, ok.text


def test_legacy_db_migrates_checks_as_not_rebuildable(tmp_path):
    """旧库（无快照列/表）可平滑升级；既有核查标记为未保存输入。"""
    import sqlite3
    from app import db as db_mod
    path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE catalogs (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
        CREATE TABLE checks (id INTEGER PRIMARY KEY AUTOINCREMENT,
            catalog_id INTEGER NOT NULL, catalog_version INTEGER NOT NULL,
            business_date TEXT NOT NULL, as_of TEXT NOT NULL, created_at TEXT NOT NULL);
        INSERT INTO catalogs(name, version, created_at) VALUES ('旧目录', 0, 'x');
        INSERT INTO checks(catalog_id, catalog_version, business_date, as_of, created_at)
            VALUES (1, 0, '2026-11-21', '2026-11-21T06:00:00+00:00', 'x');
    """)
    conn.commit(); conn.close()
    db_mod.init_db(path)
    conn = sqlite3.connect(path); conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM checks WHERE id=1").fetchone()
        assert row["snapshot_saved"] == 0 and row["rebuilt_from_check_id"] is None
        assert conn.execute(
            "SELECT COUNT(*) FROM check_input_reports").fetchone()[0] == 0
    finally:
        conn.close()


# ---------- 并发：补传 / 清空 与 核查、重建 ----------

def test_concurrent_uploads_and_resets_do_not_corrupt_checks(client):
    """并发补传/清空时：每次核查仍对应一个完整目录版本，重建始终复现自身结论。

    使用真实 sqlite 文件 + 多线程（TestClient 线程安全），验证 BEGIN IMMEDIATE
    写锁与 busy_timeout 下不会读到半批上传，也不会产生残缺快照。
    """
    from concurrent.futures import ThreadPoolExecutor
    cid = setup_catalog(client, full_day(), reports=[
        {"name": "运营日报", "offset_days": 1, "grace_minutes": 60},
        {"name": "设备日报", "offset_days": 1, "grace_minutes": 30},
    ])

    def uploader(i: int):
        if i % 4 == 3:
            return client.post(f"/api/catalogs/{cid}/reset")
        hour = i % 24
        rev = 10 + i
        return client.post(f"/api/catalogs/{cid}/partitions",
                           json={"partitions": [mk_partition(hour, revision=rev)]})

    def checker(_i: int):
        return client.post(f"/api/catalogs/{cid}/checks",
                           json={"business_date": BIZ, "as_of": AS_OF})

    with ThreadPoolExecutor(max_workers=8) as pool:
        up_futs = [pool.submit(uploader, i) for i in range(24)]
        chk_futs = [pool.submit(checker, i) for i in range(12)]
        for f in up_futs:
            assert f.result().status_code in (200, 201)
        checks = [f.result() for f in chk_futs]
    assert all(r.status_code == 201 for r in checks), [r.text for r in checks]

    # 每条核查都可重建，且重建结论与核查结论逐字段一致
    for r in checks:
        check = r.json()
        rebuilt = client.post(f"/api/checks/{check['id']}/rebuild", json={})
        assert rebuilt.status_code == 201, rebuilt.text
        assert rebuilt.json()["results"] == check["results"]

    # 最终目录状态可再核查一次，且快照记录数 == 复制记录数 == 目录记录数
    final = run_check(client, cid)
    _, snap_parts, _ = _snapshot_rows(client, final["id"])
    parts_now = client.get(f"/api/catalogs/{cid}/partitions").json()["partitions"]
    assert len(snap_parts) == len(parts_now)


def test_rebuild_rejects_corrupted_snapshot_without_writing(client):
    """快照损坏（如记录被改）时不重建：409，且不新增目录或核查。"""
    import sqlite3
    cid = setup_catalog(client, full_day())
    check = run_check(client, cid)
    conn = sqlite3.connect(client.app.state.db_path)
    try:
        conn.execute(
            "UPDATE check_input_partitions SET revision = 99 WHERE check_id = ?"
            " AND partition_hour LIKE '2026-11-20T09%'", (check["id"],))
        conn.commit()
    finally:
        conn.close()
    before = len(client.get("/api/catalogs").json()["catalogs"])
    r = client.post(f"/api/checks/{check['id']}/rebuild", json={})
    assert r.status_code == 409
    assert "不一致" in r.json()["detail"]
    assert len(client.get("/api/catalogs").json()["catalogs"]) == before


def test_rebuild_accepts_empty_body_or_named_catalog(client):
    """重建请求体可省略；默认命名包含来源核查号，也可自定名称。"""
    cid = setup_catalog(client, full_day())
    check = run_check(client, cid)
    r = client.post(f"/api/checks/{check['id']}/rebuild")  # 无请求体
    assert r.status_code == 201, r.text
    default_name = {c["id"]: c["name"] for c in client.get("/api/catalogs").json()["catalogs"]}[
        r.json()["catalog_id"]]
    assert default_name == f"排障-核查#{check['id']}"
    r2 = client.post(f"/api/checks/{check['id']}/rebuild", json={"name": "  自定义  "})
    assert r2.status_code == 201, r2.text
    names = {c["id"]: c["name"] for c in client.get("/api/catalogs").json()["catalogs"]}
    assert names[r2.json()["catalog_id"]] == "自定义"


def test_rebuild_empty_snapshot_reproduces_all_missing(client):
    """核查时目录为空（或刚被清空）：快照为空但合法，重建为同样的全缺失结论。"""
    cid = setup_catalog(client, [])
    check = run_check(client, cid)
    assert check["results"][0]["block_reason"] == "MISSING"
    r = client.post(f"/api/checks/{check['id']}/rebuild", json={})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["copied_partitions"] == 0
    assert body["results"] == check["results"]
    assert client.get(f"/api/catalogs/{body['catalog_id']}/partitions").json()["partitions"] == []
    assert len(client.get(f"/api/catalogs/{body['catalog_id']}/reports").json()["reports"]) == 1
