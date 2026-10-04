"""Backfill history from Prometheus: the same Lighthouse counters the live collector
polls, replayed from the raw 5 s scrape samples Prometheus keeps.

Each consecutive pair of scrapes is treated like a pair of live polls, so a counter
step becomes one call with its exact duration; only the completion window is wider
(5 s instead of 250 ms). Scrape gaps longer than MAX_GAP_SECONDS are skipped rather
than guessed. Stops where the live collector's data starts, so the two never overlap.

Usage: python3 backfill.py --network NAME [--since ISO] [--skip-blocks | --blocks-only]
(--since defaults to the network's `backfill_since`, or `backfill_days` before now)
"""

import argparse
import datetime as dt
import json
import logging
import time
import urllib.parse
import urllib.request

import attribute
import blocks
import collector
import normalize_versions
import store

CHUNK_SECONDS = 6 * 3600
MAX_GAP_SECONDS = 20
INDEX_THROTTLE_SECONDS = 0.2

SERIES = (
    "execution_layer_request_times_sum|execution_layer_request_times_count|execution_layer_payload_status"
    "|beacon_engine_getBlobsV[0-9]+_request_duration_seconds_(sum|count)"
    "|beacon_engine_getBlobsV[0-9]+_(complete|partial)_responses_total"
    "|beacon_blobs_from_el_expected_sum|beacon_blobs_from_el_received_total_sum|execution_layer_info"
)

log = logging.getLogger("backfill")


def prom_raw(cfg: dict, instance: str, at: float) -> list:
    """Raw samples of every series we need for one Lighthouse, over the CHUNK before `at`."""
    query = f'{{__name__=~"{SERIES}",job="{cfg["prometheus_job"]}",instance="{instance}"}}[{CHUNK_SECONDS}s]'
    url = f'{cfg["prometheus"]}/api/v1/query?' + urllib.parse.urlencode({"query": query, "time": f"{at:.3f}"})
    with urllib.request.urlopen(url, timeout=120) as resp:
        return json.loads(resp.read())["data"]["result"]


def snapshots(series: list) -> dict[float, dict]:
    """Regroup raw series into one snapshot per scrape timestamp, shaped like collector.scrape()."""
    snaps: dict[float, dict] = {}

    def at(ts: float) -> dict:
        return snaps.setdefault(ts, {"np_status": {}, "gb": {}})

    for s in series:
        m, name = s["metric"], s["metric"]["__name__"]
        for ts, raw in s["values"]:
            value, snap = float(raw), at(float(ts))
            if name == "execution_layer_request_times_sum" and m.get("method") == "new_payload":
                snap["np_sum"] = value
            elif name == "execution_layer_request_times_count" and m.get("method") == "new_payload":
                snap["np_count"] = value
            elif name == "execution_layer_payload_status" and m.get("method") == "new_payload":
                snap["np_status"][m.get("status", "").upper()] = value
            elif name == "beacon_blobs_from_el_expected_sum":
                snap["blobs_expected"] = value
            elif name == "beacon_blobs_from_el_received_total_sum":
                snap["blobs_received"] = value
            elif name == "execution_layer_info":
                if m.get("name", "Lighthouse") != "Lighthouse" and value == 1:
                    snap["el_version"] = collector.el_version(m)
            else:
                collector.add_getblobs(snap, name, value)
    return snaps


def backfill_node(conn, cfg: dict, node: dict, since: float) -> tuple[int, int]:
    # The live collector's first call after `since` ends the backfill. Live rows from before
    # `since` (another collector that has since stopped) do not count.
    first_live = conn.execute(
        "SELECT MIN(t_prev) FROM np_obs WHERE node = ? AND source = 'live' AND t_prev > ?", (node["name"], since)
    ).fetchone()[0]
    until = first_live or time.time()
    prev, prev_ts, version = None, None, "unknown"
    np_total = gb_total = 0
    chunk_end = since + CHUNK_SECONDS
    while chunk_end - CHUNK_SECONDS < until:
        snaps = snapshots(prom_raw(cfg, node["prometheus_instance"], min(chunk_end, until)))
        np_rows, gb_rows = [], []
        for ts in sorted(snaps):
            if ts <= (prev_ts or since) or ts > until:
                continue
            cur = snaps[ts]
            version = cur.get("el_version", version)
            if prev is not None and "np_count" in cur and ts - prev_ts <= MAX_GAP_SECONDS:
                np_rows += collector.newpayload_rows(node, version, prev, cur, prev_ts, ts)
                gb_rows += collector.getblobs_rows(node, version, prev, cur, prev_ts, ts)
            if "np_count" in cur:
                prev, prev_ts = cur, ts
        conn.executemany(
            "INSERT OR IGNORE INTO np_obs(node, t_prev, t_done, duration_ms, status, impl, version, source) "
            "VALUES (?,?,?,?,?,?,?,'prometheus')", np_rows)
        conn.executemany(
            "INSERT OR IGNORE INTO gb_obs(node, t_prev, t_done, duration_ms, requested, returned, status, "
            "impl, version, source) VALUES (?,?,?,?,?,?,?,?,?,'prometheus')", gb_rows)
        np_total, gb_total = np_total + len(np_rows), gb_total + len(gb_rows)
        log.info("%s: up to %s, +%d newPayload, +%d getBlobs", node["name"],
                 dt.datetime.fromtimestamp(min(chunk_end, until), dt.UTC).isoformat(timespec="minutes"),
                 len(np_rows), len(gb_rows))
        chunk_end += CHUNK_SECONDS
    return np_total, gb_total


def first_block_at(url: str, t: float) -> int:
    """Lowest block number whose timestamp is >= t (binary search over the EL)."""
    lo, hi = 0, int(blocks.rpc(url, "eth_blockNumber", []), 16)
    while lo < hi:
        mid = (lo + hi) // 2
        if int(blocks.rpc(url, "eth_getBlockByNumber", [hex(mid), False])["timestamp"], 16) < t:
            lo = mid + 1
        else:
            hi = mid
    return lo


def index_history(conn, cfg: dict, since: float) -> int:
    # Use an EL with full block history: a snap-synced node lacks old bodies.
    url = cfg.get("history_rpc", cfg["block_rpc"][0])
    first = first_block_at(url, since - cfg["seconds_per_slot"])
    head = int(blocks.rpc(url, "eth_blockNumber", []), 16)
    have = {n for (n,) in conn.execute("SELECT number FROM blocks WHERE number >= ?", (first,))}
    missing = [n for n in range(first, head + 1) if n not in have]
    log.info("indexing %d missing blocks of %d..%d from %s", len(missing), first, head, url)
    headers = cfg.get("history_headers", False)
    stored, run_start = 0, None
    for i, n in enumerate(missing):
        run_start = n if run_start is None else run_start
        last_of_run = i + 1 == len(missing) or missing[i + 1] != n + 1 or n - run_start + 1 == blocks.BATCH
        if last_of_run:
            stored += blocks.index_range(conn, cfg, url, run_start, n, headers=headers)
            run_start = None
            time.sleep(INDEX_THROTTLE_SECONDS)
            if stored and stored % 5000 < blocks.BATCH:
                log.info("indexed %d blocks", stored)
    return stored


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--network", required=True, choices=store.network_names())
    parser.add_argument("--since", help="UTC start (ISO 8601); defaults from the network config")
    parser.add_argument("--skip-blocks", action="store_true")
    parser.add_argument("--blocks-only", action="store_true", help="skip the Prometheus phase (resume indexing)")
    args = parser.parse_args()
    cfg, conn = store.load_config(args.network), store.connect(args.network)
    if args.since:
        since = dt.datetime.fromisoformat(args.since).replace(tzinfo=dt.UTC).timestamp()
    elif cfg.get("backfill_since"):
        since = dt.datetime.fromisoformat(cfg["backfill_since"]).replace(tzinfo=dt.UTC).timestamp()
    else:
        since = time.time() - cfg.get("backfill_days", 31) * 86400

    for node in [] if args.blocks_only else cfg["nodes"]:
        np_n, gb_n = backfill_node(conn, cfg, node, since)
        log.info("%s: %d newPayload and %d getBlobs calls backfilled", node["name"], np_n, gb_n)
    if not args.skip_blocks:
        log.info("indexed %d historical blocks", index_history(conn, cfg, since))
    for table in (attribute.NEWPAYLOAD, attribute.GETBLOBS):
        conn.execute(f"UPDATE {table} SET slot = NULL WHERE source = 'prometheus'")
        log.info("%s: %d calls matched to slots", table, attribute.attribute(conn, cfg, table, "prometheus"))
    log.info("%d version strings normalized", normalize_versions.normalize(conn))


if __name__ == "__main__":
    main()
