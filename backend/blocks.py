"""Index canonical blocks by slot from the EL JSON-RPC.

The tables the lab serves are keyed by slot and carry the block's hash, gas, transaction
count and blob count. Amsterdam headers carry the slot number (EIP-7843); for older
headers the slot follows from the timestamp, since every slot starts at a fixed offset
from genesis.
"""

import json
import time
import urllib.request

GAS_PER_BLOB = 1 << 17
BATCH = 100


def rpc_batch(url: str, calls: list[tuple[str, list]], timeout: float = 20, attempts: int = 4) -> list:
    body = json.dumps([{"jsonrpc": "2.0", "id": i, "method": m, "params": p} for i, (m, p) in enumerate(calls)])
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, data=body.encode(), headers={"content-type": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                replies = json.loads(resp.read())
            break
        except Exception:  # a dropped connection or truncated read: retry with backoff
            if attempt + 1 == attempts:
                raise
            time.sleep(2**attempt)
    by_id = {r["id"]: r for r in replies}
    return [by_id.get(i, {}).get("result") for i in range(len(calls))]


def rpc(url: str, method: str, params: list, timeout: float = 10):
    return rpc_batch(url, [(method, params)], timeout)[0]


def latest_number(urls: list[str]) -> tuple[int, str]:
    """Head number from the first EL that answers, and the URL that answered."""
    last_error = None
    for url in urls:
        try:
            return int(rpc(url, "eth_blockNumber", []), 16), url
        except Exception as error:  # try the next node
            last_error = error
    raise RuntimeError(f"no EL answered eth_blockNumber: {last_error}")


def block_row(cfg: dict, block: dict, tx_count: int | None = None) -> tuple:
    timestamp = int(block["timestamp"], 16)
    if block.get("slotNumber") is not None:
        slot = int(block["slotNumber"], 16)
    else:
        slot = (timestamp - cfg["genesis_time"]) // cfg["seconds_per_slot"]
    blob_gas = int(block.get("blobGasUsed") or "0x0", 16)
    return (
        slot,
        int(block["number"], 16),
        block["hash"],
        int(block["gasUsed"], 16),
        int(block["gasLimit"], 16),
        len(block["transactions"]) if tx_count is None else tx_count,
        blob_gas // GAS_PER_BLOB,
    )


def index_range(conn, cfg: dict, url: str, first: int, last: int, headers: bool = False) -> int:
    """Fetch blocks first..=last and upsert them; returns how many were stored.

    With `headers`, read eth_getHeaderByNumber + eth_getBlockTransactionCountByNumber
    instead of whole blocks: a mainnet block with its transaction hashes is ~26 KB, the
    header and the count ~2 KB. Not every client serves eth_getHeaderByNumber (reth and
    Nethermind do, ethrex does not)."""
    stored = 0
    for start in range(first, last + 1, BATCH):
        numbers = range(start, min(start + BATCH, last + 1))
        if headers:
            replies = rpc_batch(url, [c for n in numbers for c in (("eth_getHeaderByNumber", [hex(n)]),
                                                                  ("eth_getBlockTransactionCountByNumber", [hex(n)]))],
                                timeout=60)
            rows = [block_row(cfg, h, int(c, 16)) for h, c in zip(replies[0::2], replies[1::2]) if h and c]
        else:
            blocks = rpc_batch(url, [("eth_getBlockByNumber", [hex(n), False]) for n in numbers], timeout=60)
            rows = [block_row(cfg, b) for b in blocks if b]
        conn.executemany(
            "INSERT INTO blocks(slot, number, hash, gas_used, gas_limit, tx_count, blob_count) "
            "VALUES (?,?,?,?,?,?,?) ON CONFLICT(slot) DO UPDATE SET number=excluded.number, "
            "hash=excluded.hash, gas_used=excluded.gas_used, gas_limit=excluded.gas_limit, "
            "tx_count=excluded.tx_count, blob_count=excluded.blob_count",
            rows,
        )
        stored += len(rows)
    return stored


def index_new_blocks(conn, cfg: dict, lookback: int = 8) -> int:
    """Index from just below the highest stored block up to the head.

    Re-reading the last `lookback` blocks picks up short reorgs: a slot whose block
    changed is overwritten with the canonical one.
    """
    head, url = latest_number(cfg["block_rpc"])
    top = conn.execute("SELECT MAX(number) FROM blocks").fetchone()[0]
    first = head - lookback if top is None else max(0, min(top, head) - lookback)
    return index_range(conn, cfg, url, first, head)
