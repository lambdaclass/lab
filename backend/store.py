"""SQLite store shared by the collector, the backfill, the block indexer and the API.

One database per network (`data/<network>.sqlite`). Each holds one row per engine call
observed by a node's Lighthouse (`np_obs` for engine_newPayload, `gb_obs` for
engine_getBlobs), plus one row per canonical block (`blocks`). Calls are recorded first
and matched to a slot later by `attribute.py`, once the block index covers the time they
completed in.

Networks are described in `networks/<name>.json`: the nodes to poll, where to read history
from, and the `lab` entry the API hands to the frontend.
"""

import json
import os
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("LAB_DATA_DIR", ROOT / "data"))
NETWORKS_DIR = ROOT / "networks"

SCHEMA = """
CREATE TABLE IF NOT EXISTS blocks (
    slot        INTEGER PRIMARY KEY,
    number      INTEGER NOT NULL,
    hash        TEXT    NOT NULL,
    block_root  TEXT    NOT NULL DEFAULT '',
    gas_used    INTEGER NOT NULL,
    gas_limit   INTEGER NOT NULL,
    tx_count    INTEGER NOT NULL,
    blob_count  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS blocks_number ON blocks(number);

CREATE TABLE IF NOT EXISTS np_obs (
    node        TEXT    NOT NULL,
    t_prev      REAL    NOT NULL,   -- previous poll: the call completed in (t_prev, t_done]
    t_done      REAL    NOT NULL,
    duration_ms REAL    NOT NULL,
    status      TEXT    NOT NULL,
    impl        TEXT    NOT NULL,
    version     TEXT    NOT NULL,
    source      TEXT    NOT NULL,   -- 'live' (~1 s poll) or 'prometheus' (scrape history)
    slot        INTEGER,            -- NULL until attributed, -1 when no block matches
    PRIMARY KEY (node, t_done)
);
CREATE INDEX IF NOT EXISTS np_obs_slot ON np_obs(slot);
CREATE INDEX IF NOT EXISTS np_obs_pending ON np_obs(node, slot, t_done);

CREATE TABLE IF NOT EXISTS gb_obs (
    node        TEXT    NOT NULL,
    t_prev      REAL    NOT NULL,
    t_done      REAL    NOT NULL,
    duration_ms REAL    NOT NULL,
    requested   INTEGER NOT NULL,
    returned    INTEGER NOT NULL,
    status      TEXT    NOT NULL,
    impl        TEXT    NOT NULL,
    version     TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    slot        INTEGER,
    PRIMARY KEY (node, t_done)
);
CREATE INDEX IF NOT EXISTS gb_obs_slot ON gb_obs(slot);
CREATE INDEX IF NOT EXISTS gb_obs_pending ON gb_obs(node, slot, t_done);

CREATE TABLE IF NOT EXISTS node_versions (
    node    TEXT NOT NULL,
    since   REAL NOT NULL,
    version TEXT NOT NULL,
    PRIMARY KEY (node, since)
);
"""


def network_names() -> list[str]:
    return sorted(p.stem for p in NETWORKS_DIR.glob("*.json"))


def load_config(network: str) -> dict:
    cfg = json.loads((NETWORKS_DIR / f"{network}.json").read_text())
    cfg["genesis_time"] = cfg["lab"]["genesis_time"]
    return cfg


def connect(network: str) -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DATA_DIR / f"{network}.sqlite", timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    return conn


def slot_start(cfg: dict, slot: int) -> int:
    return cfg["genesis_time"] + slot * cfg["seconds_per_slot"]


def slot_at(cfg: dict, t: float) -> int:
    return int((t - cfg["genesis_time"]) // cfg["seconds_per_slot"])
