"""SQLite 存取层（标准库 sqlite3，按请求开连接）。"""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS catalogs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    version     INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS partitions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    catalog_id       INTEGER NOT NULL REFERENCES catalogs(id),
    partition_hour   TEXT NOT NULL,   -- UTC 整点 ISO8601
    planned_deadline TEXT NOT NULL,
    submitted_at     TEXT NOT NULL,
    revision         INTEGER NOT NULL,
    created_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_partitions_catalog_hour
    ON partitions(catalog_id, partition_hour);

CREATE TABLE IF NOT EXISTS reports (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    catalog_id    INTEGER NOT NULL REFERENCES catalogs(id),
    name          TEXT NOT NULL,
    offset_days   INTEGER NOT NULL DEFAULT 1,
    grace_minutes INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS checks (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    catalog_id              INTEGER NOT NULL REFERENCES catalogs(id),
    catalog_version         INTEGER NOT NULL,   -- 核查时的目录版本；与当前版本不一致即失效
    business_date           TEXT NOT NULL,      -- YYYY-MM-DD
    as_of                   TEXT NOT NULL,
    created_at              TEXT NOT NULL,
    snapshot_saved          INTEGER NOT NULL DEFAULT 0,  -- 1=已保存可重建的输入快照
    snapshot_at             TEXT,
    rebuilt_from_check_id   INTEGER REFERENCES checks(id)
);
CREATE INDEX IF NOT EXISTS idx_checks_catalog ON checks(catalog_id);

CREATE TABLE IF NOT EXISTS check_results (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    check_id       INTEGER NOT NULL REFERENCES checks(id),
    report_name    TEXT NOT NULL,       -- 快照：报表定义可能随后变化
    offset_days    INTEGER NOT NULL,
    grace_minutes  INTEGER NOT NULL,
    window_start   TEXT NOT NULL,
    window_end     TEXT NOT NULL,
    watermark_hour TEXT,                -- 安全水位（窗口内最后一个连续可用分区）
    status         TEXT NOT NULL,       -- READY | BLOCKED
    block_hour     TEXT,
    block_reason   TEXT,                -- MISSING | PENDING | DUPLICATE | LATE
    block_message  TEXT,
    counts_json    TEXT NOT NULL,
    detail_json    TEXT NOT NULL        -- 24 个分区逐小时判定明细
);
CREATE INDEX IF NOT EXISTS idx_check_results_check ON check_results(check_id);

-- 核查输入快照：日报依赖按位置保存，可重建原核查。
CREATE TABLE IF NOT EXISTS check_input_reports (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    check_id      INTEGER NOT NULL REFERENCES checks(id),
    position      INTEGER NOT NULL,
    report_name   TEXT NOT NULL,
    offset_days   INTEGER NOT NULL,
    grace_minutes INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_check_input_reports_check
    ON check_input_reports(check_id, position);

-- 核查输入快照：所有日报窗口的并集小时。每个小时只保存一组记录；
-- 同一小时内的重复提交与较低修订号记录均原样保留。
CREATE TABLE IF NOT EXISTS check_input_partitions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    check_id         INTEGER NOT NULL REFERENCES checks(id),
    partition_hour   TEXT NOT NULL,
    planned_deadline TEXT NOT NULL,
    submitted_at     TEXT NOT NULL,
    revision         INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_check_input_partitions_check
    ON check_input_partitions(check_id, partition_hour);
"""


def _migrate(conn) -> None:
    """兼容旧库：旧版 checks 表没有输入快照字段，补列时默认标记为不可重建。"""
    columns = {row[1] for row in conn.execute("PRAGMA table_info(checks)")}
    if "snapshot_saved" not in columns:
        conn.execute(
            "ALTER TABLE checks ADD COLUMN snapshot_saved INTEGER NOT NULL DEFAULT 0")
    if "snapshot_at" not in columns:
        conn.execute("ALTER TABLE checks ADD COLUMN snapshot_at TEXT")
    if "rebuilt_from_check_id" not in columns:
        conn.execute(
            "ALTER TABLE checks ADD COLUMN rebuilt_from_check_id INTEGER REFERENCES checks(id)")


def connect(path: str) -> sqlite3.Connection:
    # 每个请求独占一个连接；FastAPI 的同步依赖可能在线程池的不同线程里
    # 进入/关闭生成器，因此允许跨线程使用（不存在同一连接上的并发操作）。
    conn = sqlite3.connect(path, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def init_db(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = connect(path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")  # 读写并发：核查读取不被上传阻塞
        conn.executescript(SCHEMA)
        _migrate(conn)
        conn.commit()
    finally:
        conn.close()
