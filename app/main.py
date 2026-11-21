"""FastAPI 入口：数据产品新鲜度核查服务。

同一目录快照语义：每次核查记录当时的目录版本号；目录分区或日报依赖
发生变化都会递增版本，此前生成的核查结果即标记为失效（stale）。
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import db, logic

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DEFAULT_DB = str(Path.cwd() / "data" / "freshness.db")


# ---------- 请求模型 ----------

class CatalogIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class PartitionIn(BaseModel):
    partition_hour: str
    planned_deadline: str
    submitted_at: str
    revision: int = Field(default=1, ge=0)


class PartitionBatch(BaseModel):
    partitions: list[PartitionIn] = Field(min_length=1)


class ReportIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    offset_days: int = Field(default=1, ge=0, le=30)
    grace_minutes: int = Field(default=0, ge=0, le=24 * 60)


class CheckIn(BaseModel):
    business_date: str  # YYYY-MM-DD
    as_of: str          # ISO8601 时间


class RebuildIn(BaseModel):
    name: str | None = Field(default=None, max_length=100)  # 新目录名称，可选


# ---------- 依赖与工具 ----------

def get_db(request: Request):
    conn = db.connect(request.app.state.db_path)
    try:
        yield conn
    finally:
        conn.close()


def get_catalog_or_404(conn, catalog_id: int):
    row = conn.execute("SELECT * FROM catalogs WHERE id = ?", (catalog_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"目录 {catalog_id} 不存在")
    return row


def bump_version(conn, catalog_id: int) -> None:
    conn.execute("UPDATE catalogs SET version = version + 1 WHERE id = ?", (catalog_id,))


def current_version(conn, catalog_id: int) -> int:
    return conn.execute(
        "SELECT version FROM catalogs WHERE id = ?", (catalog_id,)
    ).fetchone()["version"]


def parse_dt_or_422(value: str, field: str) -> datetime:
    try:
        return logic.parse_dt(value)
    except Exception:
        raise HTTPException(status_code=422, detail=f"字段 {field} 不是合法时间: {value!r}")


def check_to_dict(row, current_ver: int) -> dict:
    d = dict(row)
    d["stale"] = row["catalog_version"] != current_ver
    d["has_snapshot"] = bool(row["snapshot_saved"])
    return d


def begin_immediate(conn) -> None:
    """开启写事务。

    核查/重建都在一个事务内完成“读取依赖与分区 → 计算 → 写入结论/快照”：
    BEGIN IMMEDIATE 先取得写锁，避免并发补传在两次读取之间提交，
    从而保证日报依赖与所涉小时提交来自同一目录快照。
    """
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")


def create_check_record(
    conn,
    catalog_id: int,
    catalog_version: int,
    business_date: date,
    as_of: datetime,
    reports: list[dict],
    by_hour: dict[str, list],
    *,
    snapshot_saved: bool,
    rebuilt_from: int | None = None,
    results: list[dict] | None = None,
) -> tuple[int, str, list[dict]]:
    """写入一次核查及其逐日报结果；snapshot_saved=True 时同事务保存输入快照。

    调用方负责 commit/rollback。快照与结论必须在同一事务内全部落库：
    失败时由调用方回滚，不会留下半份核查或半份快照。
    """
    now_iso = logic.iso(logic.now())
    cur = conn.execute(
        "INSERT INTO checks(catalog_id, catalog_version, business_date, as_of, created_at,"
        " snapshot_saved, snapshot_at, rebuilt_from_check_id)"
        " VALUES (?,?,?,?,?,?,?,?)",
        (catalog_id, catalog_version, business_date.isoformat(), logic.iso(as_of),
         now_iso, 1 if snapshot_saved else 0,
         now_iso if snapshot_saved else None, rebuilt_from),
    )
    check_id = cur.lastrowid

    if results is None:
        results = logic.evaluate_reports(reports, business_date, by_hour, as_of)
    for res in results:
        conn.execute(
            "INSERT INTO check_results(check_id, report_name, offset_days, grace_minutes,"
            " window_start, window_end, watermark_hour, status, block_hour, block_reason,"
            " block_message, counts_json, detail_json)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (check_id, res["report_name"], res["offset_days"], res["grace_minutes"],
             res["window_start"], res["window_end"], res["watermark_hour"],
             res["status"], res["block_hour"], res["block_reason"],
             res["block_message"], json.dumps(res["counts"], ensure_ascii=False),
             json.dumps(res["details"], ensure_ascii=False)),
        )

    if snapshot_saved:
        report_rows, partition_rows = logic.build_input_snapshot(
            reports, business_date, by_hour)
        conn.executemany(
            "INSERT INTO check_input_reports(check_id, position, report_name,"
            " offset_days, grace_minutes) VALUES (?,?,?,?,?)",
            [(check_id, i, r["report_name"], r["offset_days"], r["grace_minutes"])
             for i, r in enumerate(report_rows)],
        )
        conn.executemany(
            "INSERT INTO check_input_partitions(check_id, partition_hour, planned_deadline,"
            " submitted_at, revision) VALUES (?,?,?,?,?)",
            [(check_id, p["partition_hour"], p["planned_deadline"],
              p["submitted_at"], p["revision"]) for p in partition_rows],
        )
    return check_id, now_iso, results


def load_snapshot(conn, check_id: int) -> tuple[list[dict], dict[str, list]]:
    """读取核查保存的输入快照（调用方需先用 snapshot_saved 判断是否可重建）。"""
    report_rows = conn.execute(
        "SELECT report_name, offset_days, grace_minutes FROM check_input_reports"
        " WHERE check_id = ? ORDER BY position", (check_id,)).fetchall()
    by_hour: dict[str, list] = {}
    for row in conn.execute(
            "SELECT partition_hour, planned_deadline, submitted_at, revision"
            " FROM check_input_partitions WHERE check_id = ? ORDER BY id", (check_id,)):
        by_hour.setdefault(row["partition_hour"], []).append(row)
    return [dict(r) for r in report_rows], by_hour


RESULT_COMPARE_FIELDS = (
    "report_name", "offset_days", "grace_minutes", "window_start", "window_end",
    "watermark_hour", "status", "block_hour", "block_reason", "block_message",
)


def snapshot_matches_original(conn, check_id: int, expected: list[dict]) -> bool:
    """用快照重算的结论必须与原核查逐日报、逐小时完全一致，避免损坏快照被静默重建。"""
    stored = conn.execute(
        "SELECT * FROM check_results WHERE check_id = ? ORDER BY id", (check_id,)
    ).fetchall()
    if len(stored) != len(expected):
        return False
    for row, exp in zip(stored, expected):
        if any(row[f] != exp[f] for f in RESULT_COMPARE_FIELDS):
            return False
        if json.loads(row["counts_json"]) != exp["counts"]:
            return False
        if json.loads(row["detail_json"]) != exp["details"]:
            return False
    return True


# ---------- 应用工厂 ----------

def create_app(db_path: str | None = None) -> FastAPI:
    app = FastAPI(title="数据产品新鲜度核查", version="1.0.0")
    path = db_path or os.environ.get("DB_PATH", DEFAULT_DB)
    db.init_db(path)
    app.state.db_path = path

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(STATIC_DIR / "index.html")

    # ----- 目录 -----

    @app.post("/api/catalogs", status_code=201)
    def create_catalog(body: CatalogIn, conn=Depends(get_db)):
        cur = conn.execute(
            "INSERT INTO catalogs(name, version, created_at) VALUES (?, 0, ?)",
            (body.name, logic.iso(logic.now())),
        )
        conn.commit()
        return dict(get_catalog_or_404(conn, cur.lastrowid))

    @app.get("/api/catalogs")
    def list_catalogs(conn=Depends(get_db)):
        rows = conn.execute("SELECT * FROM catalogs ORDER BY id").fetchall()
        return {"catalogs": [dict(r) for r in rows]}

    @app.get("/api/catalogs/{catalog_id}")
    def get_catalog(catalog_id: int, conn=Depends(get_db)):
        return dict(get_catalog_or_404(conn, catalog_id))

    # ----- 分区 -----

    @app.post("/api/catalogs/{catalog_id}/partitions", status_code=201)
    def upload_partitions(catalog_id: int, body: PartitionBatch, conn=Depends(get_db)):
        get_catalog_or_404(conn, catalog_id)
        now_iso = logic.iso(logic.now())
        rows = []
        for p in body.partitions:
            hour = logic.floor_hour(parse_dt_or_422(p.partition_hour, "partition_hour"))
            deadline = parse_dt_or_422(p.planned_deadline, "planned_deadline")
            submitted = parse_dt_or_422(p.submitted_at, "submitted_at")
            rows.append((catalog_id, logic.iso(hour), logic.iso(deadline),
                         logic.iso(submitted), p.revision, now_iso))
        conn.executemany(
            "INSERT INTO partitions(catalog_id, partition_hour, planned_deadline,"
            " submitted_at, revision, created_at) VALUES (?,?,?,?,?,?)",
            rows,
        )
        bump_version(conn, catalog_id)  # 目录更新 -> 旧核查结果失效
        conn.commit()
        return {"inserted": len(rows), "catalog_version": current_version(conn, catalog_id)}

    @app.get("/api/catalogs/{catalog_id}/partitions")
    def list_partitions(catalog_id: int, conn=Depends(get_db)):
        get_catalog_or_404(conn, catalog_id)
        rows = conn.execute(
            "SELECT * FROM partitions WHERE catalog_id = ?", (catalog_id,)
        ).fetchall()
        return {"partitions": logic.classify_rows(rows)}

    @app.post("/api/catalogs/{catalog_id}/reset")
    def reset_partitions(catalog_id: int, conn=Depends(get_db)):
        get_catalog_or_404(conn, catalog_id)
        deleted = conn.execute(
            "DELETE FROM partitions WHERE catalog_id = ?", (catalog_id,)).rowcount
        bump_version(conn, catalog_id)
        conn.commit()
        return {"deleted": deleted, "catalog_version": current_version(conn, catalog_id)}

    # ----- 日报依赖 -----

    @app.post("/api/catalogs/{catalog_id}/reports", status_code=201)
    def create_report(catalog_id: int, body: ReportIn, conn=Depends(get_db)):
        get_catalog_or_404(conn, catalog_id)
        cur = conn.execute(
            "INSERT INTO reports(catalog_id, name, offset_days, grace_minutes, created_at)"
            " VALUES (?,?,?,?,?)",
            (catalog_id, body.name, body.offset_days, body.grace_minutes,
             logic.iso(logic.now())),
        )
        bump_version(conn, catalog_id)  # 依赖定义变化同样使旧核查失效
        conn.commit()
        row = conn.execute("SELECT * FROM reports WHERE id = ?", (cur.lastrowid,)).fetchone()
        return dict(row)

    @app.get("/api/catalogs/{catalog_id}/reports")
    def list_reports(catalog_id: int, conn=Depends(get_db)):
        get_catalog_or_404(conn, catalog_id)
        rows = conn.execute(
            "SELECT * FROM reports WHERE catalog_id = ? ORDER BY id", (catalog_id,)
        ).fetchall()
        return {"reports": [dict(r) for r in rows]}

    @app.delete("/api/catalogs/{catalog_id}/reports/{report_id}")
    def delete_report(catalog_id: int, report_id: int, conn=Depends(get_db)):
        get_catalog_or_404(conn, catalog_id)
        cur = conn.execute(
            "DELETE FROM reports WHERE id = ? AND catalog_id = ?", (report_id, catalog_id))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail=f"日报 {report_id} 不存在")
        bump_version(conn, catalog_id)
        conn.commit()
        return {"deleted": report_id}

    # ----- 核查 -----

    @app.post("/api/catalogs/{catalog_id}/checks", status_code=201)
    def run_check(catalog_id: int, body: CheckIn, conn=Depends(get_db)):
        catalog = get_catalog_or_404(conn, catalog_id)
        try:
            business_date = date.fromisoformat(body.business_date)
        except ValueError:
            raise HTTPException(status_code=422,
                                detail=f"business_date 应为 YYYY-MM-DD: {body.business_date!r}")
        as_of = parse_dt_or_422(body.as_of, "as_of")

        try:
            begin_immediate(conn)
            # 同一事务快照内读取：目录版本、日报依赖、全部提交记录。
            catalog = get_catalog_or_404(conn, catalog_id)
            reports = [dict(r) for r in conn.execute(
                "SELECT * FROM reports WHERE catalog_id = ? ORDER BY id", (catalog_id,))]
            by_hour: dict[str, list] = {}
            for r in conn.execute(
                    "SELECT * FROM partitions WHERE catalog_id = ? ORDER BY id",
                    (catalog_id,)):
                by_hour.setdefault(r["partition_hour"], []).append(r)

            check_id, now_iso, results = create_check_record(
                conn, catalog_id, catalog["version"], business_date, as_of,
                reports, by_hour, snapshot_saved=True)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        return {
            "id": check_id, "catalog_id": catalog_id,
            "catalog_version": catalog["version"],
            "business_date": business_date.isoformat(),
            "as_of": logic.iso(as_of), "created_at": now_iso,
            "stale": False, "has_snapshot": True, "results": results,
        }

    @app.get("/api/catalogs/{catalog_id}/checks")
    def list_checks(catalog_id: int, conn=Depends(get_db)):
        catalog = get_catalog_or_404(conn, catalog_id)
        rows = conn.execute(
            "SELECT * FROM checks WHERE catalog_id = ? ORDER BY id DESC", (catalog_id,)
        ).fetchall()
        checks = []
        for r in rows:
            d = check_to_dict(r, catalog["version"])
            summary = conn.execute(
                "SELECT status, COUNT(*) AS c FROM check_results WHERE check_id = ?"
                " GROUP BY status", (r["id"],)).fetchall()
            d["summary"] = {s["status"]: s["c"] for s in summary}
            checks.append(d)
        return {"checks": checks, "current_version": catalog["version"]}

    @app.get("/api/checks/{check_id}")
    def get_check(check_id: int, conn=Depends(get_db)):
        row = conn.execute("SELECT * FROM checks WHERE id = ?", (check_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail=f"核查 {check_id} 不存在")
        ver = current_version(conn, row["catalog_id"])
        results = []
        for rr in conn.execute(
                "SELECT * FROM check_results WHERE check_id = ? ORDER BY id",
                (check_id,)).fetchall():
            d = dict(rr)
            d["counts"] = json.loads(d.pop("counts_json"))
            d["details"] = json.loads(d.pop("detail_json"))
            results.append(d)
        return {**check_to_dict(row, ver), "results": results}

    @app.post("/api/checks/{check_id}/rebuild", status_code=201)
    def rebuild_check(check_id: int, body: RebuildIn | None = None,
                      conn=Depends(get_db)):
        """从历史核查保存的输入快照重建排障目录。

        - 只使用该核查的快照：日报依赖与所涉小时的全部提交，与结论一起保存；
        - 多个日报共用的小时只复制一组记录；重复提交与低修订记录原样保留；
        - 源目录此后补传/清空不影响重建（完全不读源目录分区）；
        - 读取快照与写入新目录在单个事务内完成：失败则全部回滚。
        """
        source = conn.execute("SELECT * FROM checks WHERE id = ?", (check_id,)).fetchone()
        if source is None:
            raise HTTPException(status_code=404, detail=f"核查 {check_id} 不存在")
        if not source["snapshot_saved"]:
            raise HTTPException(
                status_code=409,
                detail="该核查生成于输入快照功能上线前，未保存日报依赖与分区记录，无法重建")
        name = ((body.name if body else None) or "").strip() or f"排障-核查#{check_id}"
        if len(name) > 100:
            raise HTTPException(status_code=422, detail="目录名称最长 100 个字符")

        try:
            begin_immediate(conn)
            # 快照读取、评估与写入新目录全部在同一写事务内；失败则整体回滚。
            source = conn.execute(
                "SELECT * FROM checks WHERE id = ?", (check_id,)).fetchone()
            if source is None or not source["snapshot_saved"]:
                raise HTTPException(status_code=409, detail="该核查的输入快照缺失，无法重建")
            reports, by_hour = load_snapshot(conn, check_id)
            business_date = date.fromisoformat(source["business_date"])
            as_of = logic.parse_dt(source["as_of"])
            expected = logic.evaluate_reports(reports, business_date, by_hour, as_of)
            if not snapshot_matches_original(conn, check_id, expected):
                raise HTTPException(
                    status_code=409,
                    detail="输入快照与原核查结论不一致（快照可能损坏），已中止重建")

            cur = conn.execute(
                "INSERT INTO catalogs(name, version, created_at) VALUES (?, 0, ?)",
                (name, logic.iso(logic.now())))
            new_catalog_id = cur.lastrowid
            now_iso = logic.iso(logic.now())
            # 空快照是合法的（核查时目录本就为空/刚被清空）：此时不复制分区，
            # 重建结果同样是“全部缺失”。
            copied = 0
            for hour_iso, rows in by_hour.items():
                conn.executemany(
                    "INSERT INTO partitions(catalog_id, partition_hour, planned_deadline,"
                    " submitted_at, revision, created_at) VALUES (?,?,?,?,?,?)",
                    [(new_catalog_id, hour_iso, r["planned_deadline"],
                      r["submitted_at"], r["revision"], now_iso) for r in rows])
                copied += len(rows)
            conn.executemany(
                "INSERT INTO reports(catalog_id, name, offset_days, grace_minutes, created_at)"
                " VALUES (?,?,?,?,?)",
                [(new_catalog_id, r["report_name"], r["offset_days"],
                  r["grace_minutes"], now_iso) for r in reports])
            bump_version(conn, new_catalog_id)
            new_catalog_version = current_version(conn, new_catalog_id)

            new_check_id, created_at, results = create_check_record(
                conn, new_catalog_id, new_catalog_version,
                business_date, as_of, reports, by_hour,
                snapshot_saved=True, rebuilt_from=check_id, results=expected)
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise HTTPException(status_code=500, detail="重建失败，未保存新目录或复制记录")

        return {
            "id": new_check_id, "catalog_id": new_catalog_id,
            "catalog_version": new_catalog_version,
            "business_date": business_date.isoformat(), "as_of": logic.iso(as_of),
            "created_at": created_at, "stale": False, "has_snapshot": True,
            "rebuilt_from_check_id": check_id,
            "source_check_id": check_id,
            "copied_partitions": copied, "copied_reports": len(reports),
            "results": results,
        }

    # ----- 演示数据 -----

    @app.post("/api/demo", status_code=201)
    def load_demo(conn=Depends(get_db)):
        """创建/重置演示目录，覆盖：跨日依赖、宽限内/超宽限迟到、重复提交、
        修订替代、分区缺失、截至前未提交 等场景。"""
        name = "演示目录"
        row = conn.execute("SELECT * FROM catalogs WHERE name = ?", (name,)).fetchone()
        if row is None:
            cur = conn.execute(
                "INSERT INTO catalogs(name, version, created_at) VALUES (?, 0, ?)",
                (name, logic.iso(logic.now())))
            catalog_id = cur.lastrowid
        else:
            catalog_id = row["id"]
        conn.execute("DELETE FROM partitions WHERE catalog_id = ?", (catalog_id,))
        conn.execute("DELETE FROM reports WHERE catalog_id = ?", (catalog_id,))

        now_iso = logic.iso(logic.now())
        today = logic.now().date()
        d1 = today - timedelta(days=1)
        d2 = today - timedelta(days=2)

        def mk(day: date, hour: int, deadline_min: int, submitted_min: int, rev: int):
            start = datetime(day.year, day.month, day.day, tzinfo=logic.UTC)
            h = start + timedelta(hours=hour)
            return (catalog_id, logic.iso(h),
                    logic.iso(h + timedelta(minutes=deadline_min)),
                    logic.iso(h + timedelta(minutes=submitted_min)),
                    rev, now_iso)

        rows = []
        # 前天：24 个分区全部准时（供 offset=2 的日报 READY）
        for h in range(24):
            rows.append(mk(d2, h, 60, 55, 1))
        # 昨天：埋入各类异常
        for h in range(24):
            if h == 18:
                continue  # 缺失：无任何提交
            if h == 5:
                rows.append(mk(d1, 5, 60, 150, 1))   # r1 迟到 90 分钟
                rows.append(mk(d1, 5, 60, 55, 2))    # r2 准时 -> r1 被新修订替代
            elif h == 7:
                rows.append(mk(d1, 7, 60, 110, 1))   # 迟到 50 分钟（宽限 30 阻断 / 60 放行）
            elif h == 13:
                rows.append(mk(d1, 13, 60, 55, 1))
                rows.append(mk(d1, 13, 60, 55, 1))   # 同一修订重复提交
            elif h == 22:
                # 截至时刻（今天 06:00）之后才提交
                sub = datetime(today.year, today.month, today.day, 10, tzinfo=logic.UTC)
                start = datetime(d1.year, d1.month, d1.day, tzinfo=logic.UTC)
                h22 = start + timedelta(hours=22)
                rows.append((catalog_id, logic.iso(h22),
                             logic.iso(h22 + timedelta(minutes=60)),
                             logic.iso(sub), 1, now_iso))
            else:
                rows.append(mk(d1, h, 60, 55, 1))
        conn.executemany(
            "INSERT INTO partitions(catalog_id, partition_hour, planned_deadline,"
            " submitted_at, revision, created_at) VALUES (?,?,?,?,?,?)", rows)

        reports = [("运营日报", 1, 60), ("设备日报", 1, 30), ("前日回顾报", 2, 30)]
        conn.executemany(
            "INSERT INTO reports(catalog_id, name, offset_days, grace_minutes, created_at)"
            " VALUES (?,?,?,?,?)",
            [(catalog_id, n, o, g, now_iso) for n, o, g in reports])

        bump_version(conn, catalog_id)
        conn.commit()
        as_of = datetime(today.year, today.month, today.day, 6, tzinfo=logic.UTC)
        return {"catalog_id": catalog_id, "business_date": today.isoformat(),
                "as_of": logic.iso(as_of),
                "catalog_version": current_version(conn, catalog_id)}

    return app


app = create_app()
