"""Rewrite stored EL versions into collector.canonical_version's single spelling.

Rows written before that spelling existed (or while Lighthouse had no
execution_layer_info for its EL) can carry an 8-character commit, the web3 branch name,
or 'unknown'. Versions without a commit take the version of the same node's nearest
row in time that has one: the node was running that build.

Usage: python3 normalize_versions.py --network NAME
"""

import re

import collector
import store

HAS_COMMIT = re.compile(r"[-+][0-9a-f]{7}$")


def normalize(conn) -> int:
    changed = 0
    for table in ("np_obs", "gb_obs"):
        for (version,) in conn.execute(f"SELECT DISTINCT version FROM {table}").fetchall():
            canonical = collector.canonical_version(version)
            if canonical != version and HAS_COMMIT.search(canonical):
                changed += conn.execute(f"UPDATE {table} SET version = ? WHERE version = ?",
                                        (canonical, version)).rowcount
        orphans = conn.execute(
            f"SELECT rowid, node, t_done, version FROM {table} WHERE version NOT GLOB '*[-+][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]'"
        ).fetchall()
        for rowid, node, t, _ in orphans:
            nearest = conn.execute(
                f"SELECT version FROM {table} WHERE node = ? AND version GLOB "
                "'*[-+][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]' "
                "ORDER BY ABS(t_done - ?) LIMIT 1", (node, t)).fetchone()
            if nearest:
                conn.execute(f"UPDATE {table} SET version = ? WHERE rowid = ?", (nearest[0], rowid))
                changed += 1
    return changed


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--network", required=True, choices=store.network_names())
    print(f"{normalize(store.connect(parser.parse_args().network))} rows rewritten")
