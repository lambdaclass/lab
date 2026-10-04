"""Live collector: one row per engine call each node's Lighthouse makes to its EL.

Lighthouse exports cumulative counters for every engine call:
  execution_layer_request_times_{sum,count}{method="new_payload"}   seconds spent / calls
  execution_layer_payload_status{method="new_payload",status=...}   calls by response status
  beacon_engine_getBlobsV<N>_request_duration_seconds_{sum,count}   getBlobs calls
  beacon_blobs_from_el_{expected,received_total}_sum                blobs asked for / returned
Polling them every POLL_SECONDS turns each counter step into one call: the change in the
sum is that call's exact duration as Lighthouse measured it, independent of the poll
rate. The poll interval only bounds when the call finished, which is what slot
attribution needs. Nothing is changed on the nodes.

Also runs the block indexer and the slot attribution every few seconds.
"""

import argparse
import logging
import re
import threading
import time
import urllib.request

import attribute
import blocks
import store

POLL_SECONDS = 0.25
INDEX_SECONDS = 4.0

LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)")
LABEL = re.compile(r'(\w+)="([^"]*)"')
GETBLOBS = re.compile(r"^beacon_engine_getBlobs(V\d+)_request_duration_seconds_(sum|count)$")
# getBlobsV4 (EIP-8070, cells) reports outcomes as counters instead of blobs received.
GETBLOBS_OUTCOME = re.compile(r"^beacon_engine_getBlobs(V\d+)_(complete|partial)_responses_total$")

log = logging.getLogger("collector")


def scrape(url: str) -> dict:
    """The counters this collector needs, from one Prometheus text exposition."""
    with urllib.request.urlopen(url, timeout=2) as resp:
        text = resp.read().decode()
    snap = {"np_status": {}, "gb": {}}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        m = LINE.match(line)
        if not m:
            continue
        name, labels, value = m.group(1), dict(LABEL.findall(m.group(2) or "")), float(m.group(3))
        if name == "execution_layer_request_times_sum" and labels.get("method") == "new_payload":
            snap["np_sum"] = value
        elif name == "execution_layer_request_times_count" and labels.get("method") == "new_payload":
            snap["np_count"] = value
        elif name == "execution_layer_payload_status" and labels.get("method") == "new_payload":
            snap["np_status"][labels.get("status", "").upper()] = value
        elif name == "execution_layer_info" and labels.get("name", "Lighthouse") != "Lighthouse" and value == 1:
            snap["el_version"] = el_version(labels)
        elif name == "beacon_blobs_from_el_expected_sum":
            snap["blobs_expected"] = value
        elif name == "beacon_blobs_from_el_received_total_sum":
            snap["blobs_received"] = value
        else:
            add_getblobs(snap, name, value)
    return snap


def add_getblobs(snap: dict, name: str, value: float) -> None:
    g = GETBLOBS.match(name) or GETBLOBS_OUTCOME.match(name)
    if g:
        snap["gb"].setdefault(g.group(1), {})[g.group(2)] = value


BASE_VERSION = re.compile(r"\d+\.\d+\.\d+(?:-(?:alpha|beta|rc|preview|unstable|stable)[.\d]*)?")


def canonical_version(text: str, commit: str = "") -> str:
    """One spelling per build, whichever source reported it: `<semver>-<commit7>`, or
    `<semver>+<commit7>` when the client writes it that way (Nethermind).

    Accepts Lighthouse's execution_layer_info (`version` + `commit` labels) and the
    middle of web3_clientVersion (`v2.7.0-bd3238a`, `v2.2.0-preview+a4cf3dda-f`,
    `v28.0.0-perf/<branch>-<40-hex commit>`)."""
    text = text.removeprefix("v")
    base = BASE_VERSION.search(text)
    if not base:
        return text
    if not commit:
        rest = re.split(r"[-/+_.]", text[base.end():])
        commit = next((t for t in rest if len(t) >= 7 and re.fullmatch(r"[0-9a-f]+", t)), "")
    sep = "+" if "+" in text[base.end(): base.end() + 1] else "-"
    return f"{base.group(0)}{sep}{commit[:7]}" if commit else base.group(0)


def el_version(labels: dict) -> str:
    """The EL build as Lighthouse reports it (engine_getClientVersionV1)."""
    return canonical_version(labels.get("version", ""), labels.get("commit", ""))


def web3_version(client_version: str) -> str:
    """The same from web3_clientVersion, used when Lighthouse has no execution_layer_info
    for its EL (it drops the series between engine version-cache refreshes)."""
    parts = client_version.split("/")
    return canonical_version("/".join(parts[1:]) if len(parts) > 1 else client_version)


def statuses(prev: dict, cur: dict, calls: int) -> list[str]:
    """One status per call, from the status counters that moved."""
    out = []
    for status, value in cur.items():
        out += [status] * max(0, int(value - prev.get(status, 0)))
    out = sorted(out, key=lambda s: s != "VALID")[:calls]
    # A call whose request failed increments no status counter.
    return out + ["ERROR"] * (calls - len(out))


def newpayload_rows(node: dict, version: str, prev: dict, cur: dict, t_prev: float, t_done: float) -> list:
    if "np_count" not in cur or "np_count" not in prev:
        return []
    calls = int(cur["np_count"] - prev["np_count"])
    if calls <= 0:  # no call, or Lighthouse restarted and the counters reset
        return []
    spent_ms = (cur["np_sum"] - prev["np_sum"]) * 1000
    status = statuses(prev["np_status"], cur["np_status"], calls)
    # Several calls inside one poll share the window evenly; they are rare at 250 ms.
    step = (t_done - t_prev) / calls
    return [
        (node["name"], t_prev + i * step, t_prev + (i + 1) * step, spent_ms / calls, status[i],
         node["impl"], version)
        for i in range(calls)
    ]


def getblobs_rows(node: dict, version: str, prev: dict, cur: dict, t_prev: float, t_done: float) -> list:
    """One row per engine_getBlobs call.

    getBlobsV3 returns blobs, and Lighthouse records how many came back
    (beacon_blobs_from_el_received_total), so the status follows from returned vs requested.
    getBlobsV4 (EIP-8070) returns cells, and Lighthouse only counts complete and partial
    responses (V3 exports those counters too, but its exact blob count is preferred). There, a complete response counts as all requested blobs returned. Partial
    and empty responses count as none, since how many whole blobs a partial response held
    is not recorded."""
    rows = []
    for ver, now in cur["gb"].items():
        before = prev["gb"].get(ver)
        if not before or "count" not in now or "count" not in before:
            continue
        calls = int(now["count"] - before["count"])
        if calls <= 0:
            continue
        spent_ms = (now["sum"] - before["sum"]) * 1000
        requested = int(cur.get("blobs_expected", 0) - prev.get("blobs_expected", 0))
        if "blobs_received" not in cur or "blobs_received" not in prev:
            # No blob count exported (getBlobsV4): use the response-outcome counters.
            complete = int(now.get("complete", 0) - before.get("complete", 0))
            partial = int(now.get("partial", 0) - before.get("partial", 0))
            outcomes = ["SUCCESS"] * complete + ["PARTIAL"] * partial
            outcomes = (outcomes + ["EMPTY"] * calls)[:calls]
            returned_per = [requested // calls if o == "SUCCESS" else 0 for o in outcomes]
        else:
            returned = int(cur.get("blobs_received", 0) - prev.get("blobs_received", 0))
            req, ret = requested // calls, returned // calls
            outcome = "EMPTY" if ret == 0 else ("SUCCESS" if ret >= req else "PARTIAL")
            outcomes, returned_per = [outcome] * calls, [ret] * calls
        step = (t_done - t_prev) / calls
        for i in range(calls):
            rows.append((node["name"], t_prev + i * step, t_prev + (i + 1) * step, spent_ms / calls,
                         requested // calls, returned_per[i], outcomes[i], node["impl"], version))
    return rows


class Poller(threading.Thread):
    def __init__(self, network: str, node: dict):
        super().__init__(daemon=True, name=f"poll-{node['name']}")
        self.network = network
        self.node = node
        self.version = "unknown"
        self.web3_checked = 0.0

    def track_version(self, conn, snap: dict, t: float) -> None:
        version = snap.get("el_version")
        if not version and t - self.web3_checked > 60:
            self.web3_checked = t
            try:
                version = web3_version(blocks.rpc(self.node["rpc"], "web3_clientVersion", [], timeout=3))
            except Exception as error:
                log.warning("%s: web3_clientVersion failed: %s", self.node["name"], error)
        if version and version != self.version:
            self.version = version
            conn.execute("INSERT OR IGNORE INTO node_versions VALUES (?,?,?)", (self.node["name"], t, version))
            log.info("%s runs %s %s", self.node["name"], self.node["impl"], version)

    def run(self) -> None:
        conn = store.connect(self.network)
        prev, t_prev = None, None
        while True:
            started = time.time()
            try:
                cur = scrape(self.node["metrics"])
                t_now = time.time()
            except Exception as error:
                log.warning("%s: scrape failed: %s", self.node["name"], error)
                prev = None  # never span a gap: the next call's window would be unbounded
                time.sleep(1)
                continue
            self.track_version(conn, cur, t_now)
            if prev is not None:
                np_rows = newpayload_rows(self.node, self.version, prev, cur, t_prev, t_now)
                gb_rows = getblobs_rows(self.node, self.version, prev, cur, t_prev, t_now)
                if np_rows:
                    conn.executemany(
                        "INSERT OR IGNORE INTO np_obs(node, t_prev, t_done, duration_ms, status, impl, "
                        "version, source) VALUES (?,?,?,?,?,?,?,'live')", np_rows)
                if gb_rows:
                    conn.executemany(
                        "INSERT OR IGNORE INTO gb_obs(node, t_prev, t_done, duration_ms, requested, "
                        "returned, status, impl, version, source) VALUES (?,?,?,?,?,?,?,?,?,'live')",
                        gb_rows)
            prev, t_prev = cur, t_now
            time.sleep(max(0.0, POLL_SECONDS - (time.time() - started)))


def index_loop(cfg: dict) -> None:
    conn = store.connect(cfg["name"])
    while True:
        try:
            blocks.index_new_blocks(conn, cfg)
            # History is attributed by backfill.py once all of its blocks are indexed.
            attribute.attribute(conn, cfg, attribute.NEWPAYLOAD, source="live")
            attribute.attribute(conn, cfg, attribute.GETBLOBS, source="live")
        except Exception as error:
            log.warning("index/attribute failed: %s", error)
        time.sleep(INDEX_SECONDS)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(threadName)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--network", required=True, choices=store.network_names())
    args = parser.parse_args()
    cfg = store.load_config(args.network)
    store.connect(args.network).close()  # create the schema once before the threads start
    for node in cfg["nodes"]:
        Poller(args.network, node).start()
    log.info("%s: polling %d nodes every %.2fs", args.network, len(cfg["nodes"]), POLL_SECONDS)
    index_loop(cfg)


if __name__ == "__main__":
    main()
