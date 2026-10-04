"""Serve the lab's engine-timing tables for every network in networks/, in the cbt-api
request/response contract the lab frontend expects, and optionally the built frontend.

  GET /api/v1/config                         networks + feature flags
  GET /api/v1/<network>/bounds               {table: {min, max}} data bounds
  GET /api/v1/<network>/<table>?<filters>    {<table>: [rows], next_page_token}
  GET /<anything else>                       the frontend (with --static), index.html fallback

Filters are `<column>_<op>=<value>` with op in eq, ne, lt, lte, gt, gte, in, nin; plus
`order_by=<column> [ASC|DESC][, ...]`, `page_size` and `page_token`. Tables are computed
on request from the observations the collector and backfill store. Any other table
answers with an empty list, so the rest of the lab loads without errors.

Field types follow the lab's generated zod schemas (fields.json). Durations are sent
with two decimals; this branch's src/api/zod.gen.ts accepts them (upstream validates them
as integers, and Plataberget payloads take a few milliseconds).

Usage: python3 api.py [--port 8080] [--static ../dist]
"""

import argparse
import json
import math
import mimetypes
import threading
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import store

FIELDS = json.loads((Path(__file__).resolve().parent / "fields.json").read_text())
CACHE_SECONDS = 30
OPS = ("_nin", "_in", "_gte", "_lte", "_gt", "_lt", "_ne", "_eq")
NEWPAYLOAD_TABLES = {
    "fct_engine_new_payload_by_el_client",
    "fct_engine_new_payload_by_el_client_hourly",
    "fct_engine_new_payload_duration_chunked_50ms",
    "fct_engine_new_payload_winrate_hourly",
    "int_engine_new_payload_fastest_execution_by_node_class",
}
GETBLOBS_TABLES = {
    "fct_engine_get_blobs_by_el_client",
    "fct_engine_get_blobs_by_el_client_hourly",
    "fct_engine_get_blobs_duration_chunked_50ms",
}


def is_decimal(field: str) -> bool:
    return field.endswith("duration_ms") and field != "chunk_duration_ms"


def percentile(sorted_values: list[float], q: float) -> float:
    """Linear interpolation between closest ranks (numpy's default)."""
    if not sorted_values:
        return 0.0
    pos = (len(sorted_values) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo)


def duration_stats(durations: list[float], median_name: str = "median_duration_ms") -> dict:
    d = sorted(durations)
    return {
        "avg_duration_ms": sum(d) / len(d),
        median_name: percentile(d, 0.5),
        "p95_duration_ms": percentile(d, 0.95),
        "min_duration_ms": d[0],
        "max_duration_ms": d[-1],
    }


class Tables:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.node_class = cfg["node_class"]
        self.sps, self.spe, self.genesis = cfg["seconds_per_slot"], cfg["slots_per_epoch"], cfg["genesis_time"]
        self.cache: dict = {}
        self.cache_lock = threading.Lock()

    # -- shared ---------------------------------------------------------------------------

    def slot_fields(self, slot: int) -> dict:
        epoch = slot // self.spe
        return {
            "slot": slot,
            "slot_start_date_time": self.genesis + slot * self.sps,
            "epoch": epoch,
            "epoch_start_date_time": self.genesis + epoch * self.spe * self.sps,
        }

    def hour_of(self, slot: int) -> int:
        return (self.genesis + slot * self.sps) // 3600 * 3600

    def slot_range(self, filters: list) -> tuple[int, int]:
        """Translate the request's time filters into a slot range for the SQL prefilter."""
        lo, hi = 0, 2**31
        for field, op, value in filters:
            if field not in ("slot_start_date_time", "hour_start_date_time", "slot"):
                continue
            v = int(value)
            if field != "slot":
                v = (v - self.genesis) // self.sps
            if op in ("gte", "gt", "eq"):
                lo = max(lo, v - (3600 // self.sps if field == "hour_start_date_time" else 0))
            if op in ("lt", "lte", "eq"):
                hi = min(hi, v + (3600 // self.sps if field == "hour_start_date_time" else 0) + 1)
        return lo, hi

    def observations(self, kind: str, lo: int, hi: int) -> list:
        """Rows for a slot range, cached briefly: the page fires several overlapping
        requests at once, and a 31-day range is ~670k rows."""
        key = (kind, lo, hi)
        with self.cache_lock:
            hit = self.cache.get(key)
            if hit and time.time() - hit[0] < CACHE_SECONDS:
                return hit[1]
        rows = self.load_observations(kind, lo, hi)
        with self.cache_lock:
            self.cache = {k: v for k, v in self.cache.items() if time.time() - v[0] < CACHE_SECONDS}
            self.cache[key] = (time.time(), rows)
        return rows

    def load_observations(self, kind: str, lo: int, hi: int) -> list:
        if kind == "np":
            sql = ("SELECT o.node, o.duration_ms, o.status, o.impl, o.version, o.slot, b.hash, b.block_root, "
                   "b.gas_used, b.gas_limit, b.tx_count, b.blob_count FROM np_obs o JOIN blocks b USING(slot) "
                   "WHERE o.slot BETWEEN ? AND ?")
        else:
            sql = ("SELECT o.node, o.duration_ms, o.status, o.impl, o.version, o.slot, b.hash, b.block_root, "
                   "o.requested, o.returned FROM gb_obs o JOIN blocks b USING(slot) WHERE o.slot BETWEEN ? AND ?")
        conn = store.connect(self.cfg["name"])
        try:
            return conn.execute(sql, (lo, hi)).fetchall()
        finally:
            conn.close()

    # -- engine_newPayload ----------------------------------------------------------------

    def fct_engine_new_payload_by_el_client(self, obs: list) -> list:
        groups = defaultdict(list)
        for node, dur, status, impl, ver, slot, h, root, gu, gl, txs, blobs in obs:
            groups[(slot, h, root, gu, gl, txs, blobs, impl, ver, status)].append((node, dur))
        rows = []
        for (slot, h, root, gu, gl, txs, blobs, impl, ver, status), items in groups.items():
            rows.append({
                **self.slot_fields(slot), "block_hash": h, "block_root": root, "gas_used": gu, "gas_limit": gl,
                "tx_count": txs, "blob_count": blobs, "meta_execution_implementation": impl,
                "meta_execution_version": ver, "status": status, "node_class": self.node_class,
                "observation_count": len(items), "unique_node_count": len({n for n, _ in items}),
                **duration_stats([d for _, d in items]),
            })
        return rows

    def fct_engine_new_payload_by_el_client_hourly(self, obs: list) -> list:
        groups = defaultdict(list)
        for row in obs:
            groups[(self.hour_of(row[5]), row[3], row[4])].append(row)
        out = []
        for (hour, impl, ver), items in groups.items():
            valid = [r for r in items if r[2] == "VALID"]
            count = lambda s: sum(1 for r in items if r[2] == s)  # noqa: E731
            mean = lambda i: sum(r[i] for r in valid) / len(valid) if valid else 0  # noqa: E731
            out.append({
                "hour_start_date_time": hour, "meta_execution_implementation": impl,
                "meta_execution_version": ver, "node_class": self.node_class,
                "observation_count": len(items), "slot_count": len({r[5] for r in items}),
                "unique_node_count": len({r[0] for r in items}),
                "valid_count": count("VALID"), "invalid_count": count("INVALID") + count("INVALID_BLOCK_HASH"),
                "syncing_count": count("SYNCING"), "accepted_count": count("ACCEPTED"),
                "avg_gas_used": mean(8), "avg_gas_limit": mean(9), "avg_tx_count": mean(10),
                "avg_blob_count": mean(11),
                **duration_stats([r[1] for r in items], "p50_duration_ms"),
            })
        return out

    def fct_engine_new_payload_duration_chunked_50ms(self, obs: list) -> list:
        groups = defaultdict(lambda: [0, 0, 0])
        for node, dur, status, impl, ver, slot, h, *_ in obs:
            g = groups[(slot, h, int(dur // 50) * 50)]
            g[0] += 1
            g[1] += status == "VALID"
            g[2] += status in ("INVALID", "INVALID_BLOCK_HASH")
        return [
            {**self.slot_fields(slot), "block_hash": h, "chunk_duration_ms": chunk, "node_class": self.node_class,
             "observation_count": n, "valid_count": v, "invalid_count": i}
            for (slot, h, chunk), (n, v, i) in groups.items()
        ]

    def fastest_by_slot(self, obs: list) -> dict:
        best = {}
        for row in obs:
            if row[2] == "VALID" and (row[5] not in best or row[1] < best[row[5]][1]):
                best[row[5]] = row
        return best

    def int_engine_new_payload_fastest_execution_by_node_class(self, obs: list) -> list:
        return [
            {**self.slot_fields(slot), "block_hash": r[6], "duration_ms": r[1], "meta_client_name": r[0],
             "meta_execution_implementation": r[3], "meta_execution_version": r[4], "node_class": self.node_class}
            for slot, r in self.fastest_by_slot(obs).items()
        ]

    def fct_engine_new_payload_winrate_hourly(self, obs: list) -> list:
        wins = defaultdict(int)
        for slot, r in self.fastest_by_slot(obs).items():
            wins[(self.hour_of(slot), r[3])] += 1
        return [
            {"hour_start_date_time": hour, "meta_execution_implementation": impl, "node_class": self.node_class,
             "win_count": n}
            for (hour, impl), n in wins.items()
        ]

    # -- engine_getBlobs ------------------------------------------------------------------

    def fct_engine_get_blobs_by_el_client(self, obs: list) -> list:
        groups = defaultdict(list)
        for node, dur, status, impl, ver, slot, h, root, req, ret in obs:
            groups[(slot, root, impl, ver, status)].append((node, dur, req, ret))
        return [
            {**self.slot_fields(slot), "block_root": root, "meta_execution_implementation": impl,
             "meta_execution_version": ver, "status": status, "node_class": self.node_class,
             "observation_count": len(items), "unique_node_count": len({i[0] for i in items}),
             "avg_returned_count": sum(i[3] for i in items) / len(items),
             "max_requested_count": max(i[2] for i in items),
             **duration_stats([i[1] for i in items])}
            for (slot, root, impl, ver, status), items in groups.items()
        ]

    def fct_engine_get_blobs_by_el_client_hourly(self, obs: list) -> list:
        groups = defaultdict(list)
        for row in obs:
            groups[(self.hour_of(row[5]), row[3], row[4])].append(row)
        out = []
        for (hour, impl, ver), items in groups.items():
            count = lambda s: sum(1 for r in items if r[2] == s)  # noqa: E731
            out.append({
                "hour_start_date_time": hour, "meta_execution_implementation": impl,
                "meta_execution_version": ver, "node_class": self.node_class,
                "observation_count": len(items), "slot_count": len({r[5] for r in items}),
                "unique_node_count": len({r[0] for r in items}),
                "success_count": count("SUCCESS"), "partial_count": count("PARTIAL"), "empty_count": count("EMPTY"),
                "unsupported_count": count("UNSUPPORTED"), "error_count": count("ERROR"),
                "avg_returned_count": sum(r[9] for r in items) / len(items),
                **duration_stats([r[1] for r in items], "p50_duration_ms"),
            })
        return out

    def fct_engine_get_blobs_duration_chunked_50ms(self, obs: list) -> list:
        groups = defaultdict(lambda: defaultdict(int))
        for node, dur, status, impl, ver, slot, h, root, *_ in obs:
            g = groups[(slot, root, int(dur // 50) * 50)]
            g["observation_count"] += 1
            key = {"SUCCESS": "success_count", "PARTIAL": "partial_count", "EMPTY": "empty_count"}.get(status, "error_count")
            g[key] += 1
        return [
            {**self.slot_fields(slot), "block_root": root, "chunk_duration_ms": chunk, "node_class": self.node_class,
             "success_count": 0, "partial_count": 0, "empty_count": 0, "error_count": 0, **counts}
            for (slot, root, chunk), counts in groups.items()
        ]

    # -- request handling -----------------------------------------------------------------

    def query(self, table: str, params: dict) -> dict:
        filters = []
        for key, values in params.items():
            for op in OPS:
                if key.endswith(op):
                    filters.append((key[: -len(op)], op[1:], values[0]))
                    break
        if table not in NEWPAYLOAD_TABLES | GETBLOBS_TABLES:
            return {table: [], "next_page_token": ""}
        lo, hi = self.slot_range(filters)
        obs = self.observations("np" if table in NEWPAYLOAD_TABLES else "gb", lo, hi)
        rows = [self.typed(table, r) for r in getattr(self, table)(obs)]
        rows = [r for r in rows if all(matches(r, f) for f in filters)]
        for clause in reversed([c.strip() for c in params.get("order_by", [""])[0].split(",") if c.strip()]):
            column, _, direction = clause.partition(" ")
            rows.sort(key=lambda r: r.get(column, 0), reverse=direction.strip().upper() == "DESC")
        size = int(params.get("page_size", ["100"])[0])
        offset = int(params.get("page_token", ["0"])[0] or 0)
        page = rows[offset: offset + size]
        return {table: page, "next_page_token": str(offset + size) if offset + size < len(rows) else ""}

    def typed(self, table: str, row: dict) -> dict:
        kinds, now = FIELDS[table], int(time.time())
        out = {}
        for field, kind in kinds.items():
            value = now if field == "updated_date_time" else row.get(field, "" if kind == "str" else 0)
            if kind == "int":
                value = round(value, 2) if is_decimal(field) else int(round(value))
            elif kind == "float":
                value = round(float(value), 3)
            out[field] = value
        return out

    def bounds(self) -> dict:
        conn = store.connect(self.cfg["name"])
        try:
            np_lo, np_hi = conn.execute("SELECT MIN(slot), MAX(slot) FROM np_obs WHERE slot >= 0").fetchone()
            gb_lo, gb_hi = conn.execute("SELECT MIN(slot), MAX(slot) FROM gb_obs WHERE slot >= 0").fetchone()
        finally:
            conn.close()
        out = {}
        for tables, lo, hi in ((NEWPAYLOAD_TABLES, np_lo, np_hi), (GETBLOBS_TABLES, gb_lo, gb_hi)):
            if lo is None:
                continue
            for t in tables:
                out[t] = {"min": self.genesis + lo * self.sps, "max": self.genesis + hi * self.sps}
        return out

def matches(row: dict, f: tuple) -> bool:
    field, op, raw = f
    if field not in row:
        return True  # a filter on a column this table doesn't have
    value = row[field]
    cast = (lambda x: x) if isinstance(value, str) else float
    if op in ("in", "nin"):
        hit = value in {cast(v) for v in raw.split(",")}
        return hit if op == "in" else not hit
    other = cast(raw)
    return {"eq": value == other, "ne": value != other, "lt": value < other, "lte": value <= other,
            "gt": value > other, "gte": value >= other}[op]


def make_handler(networks: dict[str, Tables], static: Path | None):
    # mainnet first: the frontend selects it by default when present.
    order = sorted(networks, key=lambda n: (n != "mainnet", n))
    config = {"networks": [networks[n].cfg["lab"] for n in order], "features": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlparse(self.path)
            parts = [p for p in url.path.split("/") if p]
            try:
                if parts[:2] != ["api", "v1"]:
                    return self.static(url.path)
                if parts[2:] == ["config"]:
                    return self.reply(200, config)
                if len(parts) == 4 and parts[2] in networks:
                    tables = networks[parts[2]]
                    if parts[3] == "bounds":
                        return self.reply(200, tables.bounds())
                    return self.reply(200, tables.query(parts[3], parse_qs(url.query)))
                return self.reply(404, {"error": f"unknown path {url.path}"})
            except Exception as error:  # report, don't kill the server
                return self.reply(500, {"error": repr(error)})

        def static(self, path: str) -> None:
            if static is None:
                return self.reply(404, {"error": "not found"})
            file = (static / path.lstrip("/")).resolve()
            if not file.is_file() or static not in file.parents:
                file = static / "index.html"  # client-side route
            data = file.read_bytes()
            self.send_response(200)
            self.send_header("content-type", mimetypes.guess_type(file.name)[0] or "application/octet-stream")
            self.send_header("content-length", str(len(data)))
            # Hashed bundles never change; index.html must always be fresh.
            self.send_header("cache-control", "public, max-age=31536000, immutable"
                             if "/assets/" in path else "no-cache")
            self.end_headers()
            self.wfile.write(data)

        def reply(self, code: int, body: dict) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, fmt, *args):
            if self.path.startswith("/api/"):
                print(f"{self.log_date_time_string()} {self.command} {self.path[:160]} -> "
                      f"{args[1] if len(args) > 1 else ''}", flush=True)

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--static", type=Path, help="built frontend to serve (the lab's dist/)")
    args = parser.parse_args()
    networks = {name: Tables(store.load_config(name)) for name in store.network_names()}
    static = args.static.resolve() if args.static else None
    server = ThreadingHTTPServer((args.host, args.port), make_handler(networks, static))
    print(f"serving the lab API for {', '.join(networks)} on http://{args.host}:{args.port}"
          + (f", frontend from {static}" if static else ""), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
