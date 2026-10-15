#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""xmxmon capture store: SQLite schema, ingest, and query helpers.

Model: `runs` is the spine (one row per capture file; identity, wall-clock
anchor, ingest idempotency key). Each metric group gets its own table
`samples_<Group>` holding that group's columns exactly — a run belongs to one
group, so nothing is duplicated, and "this group doesn't report that metric"
is expressed by the column not existing rather than by NULL. `groups` is a
small registry so the model can be enumerated from SQL.

Tables are built from the capture rows themselves (driver drift = a new
group's table, or ALTER on a known one — not a migration). Everything
derived — wall clock, per-second rates — comes from the per-group v_samples_
views or the helpers below, never from duplicated columns.

Importable on purpose (underscore name, like xmxderive.py): the xmx-ingest
CLI and the MCP server both consume this. xmxderive provides the percent-vs-
counter semantics and the derived-metric math so offline numbers agree with
the live daemon.
"""
import json
import os
import re
import sqlite3
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import xmxderive

# Bookkeeping fields the hardware stamps on every report. Framing fields get
# named lowercase columns; the rest are preserved verbatim in the `extra`
# JSON column — nothing is dropped, but they don't earn columns.
FRAMING = {"t": "t", "GpuTime": "gpu_ns", "GpuCoreClocks": "gpu_core_clocks",
           "AvgGpuCoreFrequencyMHz": "avg_core_mhz",
           "CoreFrequencyMHz": "core_mhz"}
EXTRA_KEYS = {"QueryBeginTime", "ReportReason", "ContextIdValid", "ContextId",
              "SourceId", "StreamMarker"}

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FNAME = re.compile(r"^(?P<name>.+)-dev(?P<dev>\d+)-(?P<ts>\d+)\.ndjson$")

_SPINE = """
CREATE TABLE IF NOT EXISTS runs (
  run_id       INTEGER PRIMARY KEY,
  name         TEXT NOT NULL,
  device       INTEGER NOT NULL,
  metric_group TEXT NOT NULL,
  period_ms    REAL,           -- not recorded in captures; NULL until known
  started_ts   REAL NOT NULL,  -- unix epoch, from the capture filename
  ended_ts     REAL,           -- started + span of the rows (approximation)
  rows         INTEGER NOT NULL DEFAULT 0,
  path         TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS groups (
  metric_group TEXT PRIMARY KEY,
  table_name   TEXT NOT NULL,
  metrics      INTEGER NOT NULL DEFAULT 0
);
"""

_FRAMING_COLS = [("run_id", "INTEGER NOT NULL REFERENCES runs(run_id)"),
                 ("seq", "INTEGER NOT NULL"),
                 ("t", "REAL NOT NULL"),
                 ("gpu_ns", "REAL"),
                 ("gpu_core_clocks", "REAL"),
                 ("avg_core_mhz", "REAL"),
                 ("core_mhz", "REAL"),
                 ("extra", "TEXT")]
_FRAMING_NAMES = {n for n, _ in _FRAMING_COLS}


def table_for(group):
    """Table name for a metric group. Group names come from the driver via
    --list (plain identifiers), but guard anyway."""
    if not _IDENT.match(group):
        raise ValueError(f"unsafe group name: {group!r}")
    return f"samples_{group}"


def connect(db_path):
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    con.executescript(_SPINE)
    return con


def metric_columns(con, group):
    skip = _FRAMING_NAMES
    try:
        return [r["name"] for r in
                con.execute(f"PRAGMA table_info({table_for(group)})")
                if r["name"] not in skip]
    except ValueError:
        return []


def _view_sql(group, table):
    # Wall clock is derived: capture start (filename) + t relative to the
    # first row of the run. FIRST_VALUE keeps raw `t` untouched.
    return f"""
CREATE VIEW IF NOT EXISTS v_{table} AS
SELECT s.*, r.name,
       r.started_ts + (s.t - FIRST_VALUE(s.t) OVER w) AS wall_ts
FROM {table} s JOIN runs r USING (run_id)
WINDOW w AS (PARTITION BY s.run_id ORDER BY s.seq
             ROWS UNBOUNDED PRECEDING);
"""


def _ensure_table(con, group, keys):
    """Create the group's table if needed, ALTER in newly seen metrics.
    Returns the set of metric columns now present."""
    table = table_for(group)
    if not con.execute("SELECT 1 FROM groups WHERE metric_group=?",
                       (group,)).fetchone():
        cols = ",\n  ".join([f"{n} {t}" for n, t in _FRAMING_COLS] +
                           [f'"{k}" REAL' for k in sorted(keys)
                            if _IDENT.match(k)])
        con.execute(f"CREATE TABLE {table} (\n  {cols},\n"
                    f"  PRIMARY KEY (run_id, seq))")
        con.execute(_view_sql(group, table).replace("CREATE VIEW", "CREATE VIEW"))
        con.execute("INSERT INTO groups (metric_group, table_name, metrics)"
                    " VALUES (?,?,?)", (group, table, len(keys)))
    existing = set(metric_columns(con, group))
    for k in sorted(keys):
        if k in existing or not _IDENT.match(k):
            continue
        con.execute(f'ALTER TABLE {table} ADD COLUMN "{k}" REAL')
        existing.add(k)
    con.execute("UPDATE groups SET metrics=? WHERE metric_group=?",
                (len(existing), group))
    return existing


def ingest_file(con, path):
    """Ingest one capture ndjson. Returns (run_id, group) or None if the
    file is already in the store (idempotency key: runs.path)."""
    if con.execute("SELECT 1 FROM runs WHERE path=?", (path,)).fetchone():
        return None
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("{"):
                rows.append(json.loads(line))
    if not rows:
        return None
    m = _FNAME.match(os.path.basename(path))
    if m:
        name, device, started = m["name"], int(m["dev"]), float(m["ts"])
    else:  # foreign ndjson: keep it ingestible, derive identity from the file
        name = os.path.basename(path).rsplit(".ndjson", 1)[0]
        device, started = -1, os.path.getmtime(path)
    # A capture can straddle a group-switch respawn (one stale-schema row
    # from the dying sampler lands in the new file). Label by the MODAL row
    # schema; minority rows ingest with their extra fields in `extra`.
    group = Counter(xmxderive.detect_group(r.keys()) for
                    r in rows).most_common(1)[0][0]

    frame_keys = set(FRAMING) | EXTRA_KEYS
    modal = [k for r in rows if xmxderive.detect_group(r.keys()) == group
             for k in r.keys() if k not in frame_keys]
    existing = _ensure_table(con, group, set(modal))

    table = table_for(group)
    sql_keys = sorted(existing)
    ins = (f"INSERT INTO {table} (run_id, seq, t, gpu_ns, gpu_core_clocks,"
           f" avg_core_mhz, core_mhz, extra, "
           + ", ".join(f'"{k}"' for k in sql_keys)
           + f") VALUES ({', '.join('?' * (8 + len(sql_keys)))})")

    with con:
        cur = con.execute(
            "INSERT INTO runs (name, device, metric_group, started_ts,"
            " ended_ts, rows, path) VALUES (?,?,?,?,?,?,?)",
            (name, device, group, started, started, len(rows), path))
        run_id = cur.lastrowid
        t0 = rows[0].get("t", 0.0)
        for seq, r in enumerate(rows):
            extra = {k: r.get(k) for k in EXTRA_KEYS if k in r}
            nonstd = {k: v for k, v in r.items()
                      if k not in frame_keys and k not in existing}
            if nonstd:
                extra["off_schema"] = nonstd
            con.execute(ins, (
                run_id, seq, r.get("t"), r.get("GpuTime"),
                r.get("GpuCoreClocks"), r.get("AvgGpuCoreFrequencyMHz"),
                r.get("CoreFrequencyMHz"),
                json.dumps(extra) if extra else None,
                *[r.get(k) for k in sql_keys]))
        con.execute("UPDATE runs SET ended_ts=? WHERE run_id=?",
                    (started + (rows[-1].get("t", t0) - t0), run_id))
    return run_id, group


def ingest_dir(db_path, captures_dir):
    """Ingest every .ndjson in a directory not already stored. Returns a list
    of (name, run_id) for newly added runs."""
    con = connect(db_path)
    out = []
    try:
        for fn in sorted(os.listdir(captures_dir)):
            if not fn.endswith(".ndjson"):
                continue
            res = ingest_file(con, os.path.join(captures_dir, fn))
            if res:
                out.append((fn.rsplit(".ndjson", 1)[0], res[0]))
    finally:
        con.close()
    return out


def list_runs(con):
    return [dict(r) for r in con.execute(
        "SELECT run_id, name, device, metric_group, started_ts, ended_ts,"
        " rows, path FROM runs ORDER BY started_ts DESC")]


def _range(t0, t1, params):
    sql = ""
    if t0 is not None:
        sql += " AND wall_ts >= ?"
        params.append(t0)
    if t1 is not None:
        sql += " AND wall_ts <= ?"
        params.append(t1)
    return sql


def _run_meta(con, run_id):
    run = con.execute("SELECT * FROM runs WHERE run_id=?",
                      (run_id,)).fetchone()
    if not run:
        raise KeyError(f"no run {run_id}")
    return dict(run)


def dump_run(con, run_id, t0=None, t1=None, metrics=None, max_rows=2000):
    """Wide rows for a run, optionally wall-time-ranged and column-projected."""
    run = _run_meta(con, run_id)
    cols = metric_columns(con, run["metric_group"])
    if metrics is not None:
        bad = [m for m in metrics if m not in cols]
        if bad:
            raise KeyError(f"metrics not in {run['metric_group']}: {bad}")
        cols = list(metrics)
    sel = ", ".join(f'"{c}"' for c in
                    ["seq", "wall_ts", "t", "gpu_ns", "avg_core_mhz"] + cols)
    params = [run_id]
    sql = (f"SELECT {sel} FROM v_{table_for(run['metric_group'])}"
           f" WHERE run_id=?" + _range(t0, t1, params)
           + f" ORDER BY seq LIMIT {max_rows + 1}")
    rows = [list(r) for r in con.execute(sql, params)]
    truncated = len(rows) > max_rows
    return {"run": run,
            "columns": ["seq", "wall_ts", "t", "gpu_ns", "avg_core_mhz"]
                       + cols,
            "rows": rows[:max_rows], "truncated": truncated}


def summarize_run(con, run_id, t0=None, t1=None):
    """Aggregates with xmxderive semantics: levels are averaged, counters are
    summed over SUM(gpu_ns) for per-second rates; derived metrics come from
    the same xmxderive.derive the daemon uses, so offline and live agree."""
    run = _run_meta(con, run_id)
    group = run["metric_group"]
    cols = metric_columns(con, group)
    params = [run_id]
    rng = _range(t0, t1, params)
    agg = ", ".join(f'AVG("{c}"), MIN("{c}"), MAX("{c}"), SUM("{c}")'
                    for c in cols)
    row = tuple(con.execute(
        f"SELECT COUNT(*), SUM(gpu_ns), AVG(avg_core_mhz), {agg}"
        f" FROM v_{table_for(group)} WHERE run_id=?" + rng,
        params).fetchone())
    n, gpu_ns_total, mhz_avg = row[0], row[1], row[2]

    metrics, v = {}, {}
    for i, c in enumerate(cols):
        avg, mn, mx, tot = row[3 + i * 4: 7 + i * 4]
        if avg is None:
            continue  # reported by the group but absent in this range
        if xmxderive.is_percent(c):
            metrics[c] = {"kind": "level", "avg": avg, "min": mn, "max": mx}
            v[c] = avg
        else:
            rate = tot * 1e9 / gpu_ns_total if gpu_ns_total else None
            metrics[c] = {"kind": "counter", "total": tot,
                          "rate_per_s": rate, "max_per_window": mx}
            if rate is not None:
                v[c] = rate
    derived = [{"label": lbl,
                "value": (None if val != val or val in (float("inf"),
                                                         float("-inf"))
                          else val),
                "unit": unit, "note": note}
               for lbl, val, unit, note in xmxderive.derive(v, group)]
    wall = con.execute(
        f"SELECT MIN(wall_ts), MAX(wall_ts) FROM v_{table_for(group)}"
        f" WHERE run_id=?" + rng, params).fetchone()
    return {"run": {**run, "rows_in_range": n,
                    "gpu_s": (gpu_ns_total or 0) / 1e9,
                    "wall_s": ((wall[1] - wall[0]) if n > 1 else 0.0),
                    "avg_core_mhz": mhz_avg},
            "metrics": metrics, "derived": derived}
