"""Match recorded engine calls to the slot whose block they processed.

A node's Lighthouse calls engine_newPayload once per canonical block, in order, while
that slot is running. Each observation only says the call completed somewhere in
(t_prev, t_done] (between two polls), so it is matched to the earliest slot that:
  - has a canonical block (with blobs, for getBlobs),
  - is later than the slot matched to that node's previous call in time,
  - could plausibly have finished in that interval: from the slot start up to
    LATE_SECONDS after it.
Calls that match nothing get slot -1 and are left out of every table: payloads re-sent
after an EL restart, blocks that lost a reorg.

Calls are walked per node in time order across every row (matched or not), so live and
backfilled rows can be attributed in any order without one shadowing the other.
"""

import bisect

LATE_SECONDS = 13.0  # a payload can finish just after its slot ends
NEWPAYLOAD = "np_obs"
GETBLOBS = "gb_obs"


def attribute(conn, cfg: dict, table: str, source: str | None = None) -> int:
    """Attribute pending rows (optionally only those from `source`); returns how many matched."""
    covered = conn.execute("SELECT MIN(slot), MAX(slot) FROM blocks").fetchone()
    if covered[1] is None:
        return 0
    genesis, sps = cfg["genesis_time"], cfg["seconds_per_slot"]
    # Only attribute calls that finished before the newest indexed slot started, so the
    # block they processed is already in the index.
    horizon = genesis + covered[1] * sps
    source_filter = "" if source is None else " AND source = ?"
    source_args = () if source is None else (source,)
    blob_filter = " WHERE blob_count > 0" if table == GETBLOBS else ""
    slots = [s for (s,) in conn.execute(f"SELECT slot FROM blocks{blob_filter} ORDER BY slot")]
    matched = 0
    nodes = conn.execute(
        f"SELECT DISTINCT node FROM {table} WHERE slot IS NULL AND t_done < ?{source_filter}",
        (horizon, *source_args)).fetchall()
    for (node,) in nodes:
        first, last_t = conn.execute(
            f"SELECT MIN(t_done), MAX(t_done) FROM {table} WHERE node = ? AND slot IS NULL AND t_done < ?"
            f"{source_filter}", (node, horizon, *source_args)).fetchone()
        last = conn.execute(
            f"SELECT MAX(slot) FROM {table} WHERE node = ? AND slot >= 0 AND t_done < ?", (node, first)
        ).fetchone()[0]
        last = -1 if last is None else last
        rows = conn.execute(
            f"SELECT t_prev, t_done, slot, source FROM {table} WHERE node = ? AND t_done BETWEEN ? AND ? "
            "ORDER BY t_done", (node, first, last_t)).fetchall()
        updates = []
        for t_prev, t_done, slot, row_source in rows:
            if slot is not None:
                last = max(last, slot)
                continue
            if source is not None and row_source != source:
                continue  # left for its own pass; it does not move `last` until matched
            match = -1
            lo = max(int((t_prev - LATE_SECONDS - genesis) // sps), last + 1)
            hi = int((t_done - genesis) // sps)
            i = bisect.bisect_left(slots, lo)
            while i < len(slots) and slots[i] <= hi:
                start = genesis + slots[i] * sps
                if start < t_done and start + LATE_SECONDS > t_prev:
                    match = slots[i]
                    break
                i += 1
            if match >= 0:
                last = match
                matched += 1
            updates.append((match, node, t_done))
        conn.executemany(f"UPDATE {table} SET slot = ? WHERE node = ? AND t_done = ?", updates)
    return matched
