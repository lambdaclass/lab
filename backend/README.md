# LambdaClass lab backend

Backend for the `lambdaclass` branch of this fork: the lab's **Engine API Timings** page
(`/ethereum/execution/timings`), fed by LambdaClass's own nodes instead of the ethPandaOps
fleet. It serves the cbt-api tables that page reads, plus the built frontend.

| Network | Nodes (EL + its own Lighthouse) |
|---|---|
| mainnet | `nethermind-mainnet-1`, `reth-mainnet-1`, `ethrex-mainnet-2` |
| plataberget | `ethrex-mainnet-3` (Nethermind), `ethrex-mainnet-4` (Reth), `ethrex-mainnet-5` (ethrex) |

Hosted on `ethrex-grafana`: **https://grafana.ethrex.xyz/lab/** (Caddy routes `/lab/*` to the API on :8080,
which also serves the frontend; to move to its own domain, build without `LAB_BASE_PATH` and point the domain at :8080).

## Run it

Prerequisites: Python 3.11+ (standard library only), network access to the nodes' Lighthouse
metrics (`:5054`), EL JSON-RPC (`:8545`) and the Prometheus on `ethrex-grafana`. To build the
frontend: Node 22+ and pnpm 10.

```sh
# collectors, one per network (live data, ~1 s polls)
python3 backend/collector.py --network mainnet
python3 backend/collector.py --network plataberget

# history from Prometheus (one-off; mainnet = last 31 days, plataberget = since 2026-09-15)
python3 backend/backfill.py --network mainnet
python3 backend/backfill.py --network plataberget

# API + frontend
pnpm install && pnpm build
python3 backend/api.py --port 8080 --static dist
open http://localhost:8080/ethereum/execution/timings
```

For frontend development, run the API without `--static` and `pnpm dev` (it proxies `/api` to
`localhost:8080`). Data lives in `backend/data/<network>.sqlite` (override with `LAB_DATA_DIR`).

Example API calls (the same ones the page makes):

```sh
curl -s localhost:8080/api/v1/config            # -> {"networks": [{"name": "mainnet", ...}, {"name": "plataberget", ...}], ...}
H=$(( $(date -u +%s) / 3600 * 3600 ))
curl -s "localhost:8080/api/v1/mainnet/fct_engine_new_payload_by_el_client_hourly?hour_start_date_time_gte=$((H-86400))&hour_start_date_time_lt=$H"
# -> {"fct_engine_new_payload_by_el_client_hourly": [{"meta_execution_implementation": "ethrex",
#     "p50_duration_ms": 41.2, "p95_duration_ms": 118.3, "valid_count": 299, ...}, ...], "next_page_token": ""}
```

### Deploy on ethrex-grafana

```sh
LAB_BASE_PATH=/lab/ pnpm build             # the frontend lives under grafana.ethrex.xyz/lab/
rsync -az --delete --exclude data/ --exclude __pycache__/ backend dist admin@ethrex-grafana:lab/
ssh admin@ethrex-grafana
sudo cp ~/lab/backend/deploy/lab-collector@.service ~/lab/backend/deploy/lab-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now lab-collector@mainnet lab-collector@plataberget lab-api
cd ~/lab/backend && LAB_DATA_DIR=~/lab-data python3 backfill.py --network mainnet   # once
```

The Caddy route on `ethrex-grafana` (`/etc/caddy/Caddyfile`, inside the `grafana.ethrex.xyz` block):

```
redir /lab /lab/ 308
handle_path /lab/* {
  reverse_proxy http://127.0.0.1:8080
}
handle {
  reverse_proxy http://127.0.0.1:3000
}
```

Adding a network: copy `networks/mainnet.json`, list its nodes (each EL with its own
Lighthouse) and their Prometheus instance labels, set `lab` to the network's entry (genesis
time and fork schedule, as in ethPandaOps' `/api/v1/config`), then enable
`lab-collector@<name>` and restart `lab-api`.

## How it works

```
 node Lighthouse :5054/metrics ──(poll ~1 s)──► collector.py ─┐
 Prometheus (5 s scrapes) ──────(one-off)────► backfill.py ───┤──► data/<network>.sqlite ──► api.py :8080 ──► browser
 EL JSON-RPC (blocks, headers) ───────────────► blocks.py ────┘            ▲                (API + built frontend)
                                                             attribute.py ──┘
```

**What is measured.** Each Lighthouse exports cumulative counters for the engine calls it
makes to its own EL: `execution_layer_request_times_{sum,count}{method="new_payload"}`,
`execution_layer_payload_status{method="new_payload",status=…}`,
`beacon_engine_getBlobsV<N>_request_duration_seconds_{sum,count}` and the blob counters.
Between two polls, a counter step of one call means the change in `sum` is exactly that
call's duration as Lighthouse timed it. That is the CL's view of the engine API round trip,
the same vantage point as lab.ethpandaops.io. The same Lighthouse build fronts every client
of a network.

**Matching calls to slots.** A call is only known to have finished between two polls. It is
matched to the earliest canonical block whose slot could have finished in that window and
that comes after the previous call matched on that node (`attribute.py`). Calls that match
nothing are excluded: payloads re-sent after an EL restart, or blocks that lost a reorg.

**Versions** come from Lighthouse's `execution_layer_info` (the EL's
engine_getClientVersionV1), falling back to `web3_clientVersion`. Both are normalized to
`<semver>-<commit7>`.

**getBlobs.** Lighthouse exports how many blobs came back for getBlobsV3; for getBlobsV4
(EIP-8070, cells) it only counts complete and partial responses. Which version it uses
depends on the EL, so getBlobs durations can compare different methods across clients.

**Win-rate** is per slot (the fastest VALID newPayload wins the slot), as upstream computes
it. The "L1 Client Comparison" Grafana dashboards count wins the same way, from Prometheus.

## Differences from upstream

- `src/api/zod.gen.ts`: the engine timing tables accept decimal durations. Upstream
  validates them as integers, and Plataberget payloads take 2–5 ms.
- The sidebar only enables Timings, and `/` redirects there.
- `LAB_BASE_PATH` (default `/`) builds the lab for a sub-path; the router, the API prefix and
  the `public/` asset paths follow it.
- The backend replaces ethPandaOps' xatu + ClickHouse + cbt-api stack with SQLite and
  aggregates on request, which is enough for three nodes per network.

## Limitations

- `block_root` is empty: blocks are indexed from the EL, which has no beacon root.
- "P50" over 7d/31d is upstream's aggregation: the observation-weighted mean of hourly
  P50s. On bimodal load (Plataberget's idle vs spam hours) it can differ a lot from the
  median of all calls.
- History starts when Prometheus started scraping each Lighthouse.
