# ChunkTevict

Cost simulation for geo-distributed object storage. A trace records when each object is written and read in each region. An offline index records the unique chunks of each object. The simulator places those chunks, assigns a TTL, and reports storage cost, network transfer cost, chunk hit rate, and lookup latency.

This repository contains:

1. Multi-region traces generated from the SNIA IBM object traces
2. Offline scripts that bind object keys to files, cut chunks, and build the chunk index
3. The simulator: placement policies, transfer selection, price and latency tables

Layout:

* `scripts/` — content binding, FastCDC, and the chunk index
* `simulation/main.py` — run one object-level policy
* `simulation/src/dedup/aware_replay.py` — run `chunk_tevict`
* `simulation/src/placement_policy/` — placement policies. `chunk_tevict` is the chunk policy; `tevict_ranges` is the object policy used in the same matrix
* `simulation/src/transfer_policy/` — which region serves a miss (`cheapest`)
* `simulation/config/` — YAML configs
* `simulation/src/utils/profiles/` — storage price, transfer price, throughput, and latency
* `simulation/SNIA_traces/` — how the multi-region traces are generated
* `simulation/run_fig5_matrix_parallel.py` — run the evaluation matrix

IBM trace files and the file pool are not in this repository. Download the traces from SNIA (see `simulation/SNIA_traces/README.md`) and place generated traces under `simulation/traces`.

## Getting Started

Python 3.10 or newer.

```bash
cd simulation
pip install -r requirements.txt
```

The offline scripts also need `blake3` and `fastcdc`:

```bash
pip install blake3 fastcdc
```

Run commands from `simulation/` unless a step says otherwise. `load_config` resolves paths from that directory.

## Trace Format

```
timestamp, op, issue_region, obj_key, size
2023-10-12 10:00:01,write,aws:us-west-2,1234,1000000
2023-10-12 10:00:02,read,aws:us-east-1,1234,1000000
```

`op` is `read` or `write`. `issue_region` is the region that issues the request. `size` is the object size in bytes. One trace id is shared by every region count: `T015`, `T029`, `T065`, `T078`, `T079`.

## Config Format

```yml
placement_policy: "chunk_tevict"
transfer_policy: "cheapest"
regions: ['aws:us-west-2', 'aws:us-east-1']
window_size: -1
fixed_base_region: true
chunk_store_dir: "/data/dedup_exp/T065"
```

`placement_policy` is one of `always_evict`, `pull_on_read` (always store), `teven`, `dynamicttl`, `ewma`, `replicate_all`, `tevict_ranges`, `optimal`, `chunk_tevict`.

`fixed_base_region: true` keeps the object, or its chunks, in the region of the first PUT. `false` keeps at least one copy and allows that copy to expire. `chunk_store_dir` is read by `chunk_tevict` only.

## Chunk index

The simulator does not cut chunks. Build the index once per trace id. All region counts of that trace share it.

Expected inputs, produced outside this repository:

* `/data/file_pool` — real files (Linux trees, PHP trees, Ubuntu images, PostgreSQL, and the other pools listed in `simulation/SNIA_traces/Documentation/DATASET_PREP.md`)
* trace CSVs the scripts already know under `/data/skystore/traces`

From the repository root:

```bash
python scripts/remap_zero.py
python scripts/fill_family_pad.py
python scripts/remap_zero.py --reindex
python scripts/chunk_objects.py 065
python scripts/prep_chunkstore.py /data/dedup_exp/T065
```

Repeat `chunk_objects.py` and `prep_chunkstore.py` for `015`, `029`, `078`, and `079`. `fill_family_pad.py` fills the two large T079 objects; skip it when `remap_zero.py` reports no remaining gap.

`prep_chunkstore.py` writes, under `/data/dedup_exp/T0xx/`:

* `lens_u.npy` — length of each unique chunk
* `obj_uids_data.npy`, `obj_uids_indptr.npy` — per-object unique chunk ids
* `obj_meta.csv.gz` — `obj_key`, `size`

FastCDC bounds are 2 KiB / 8 KiB / 64 KiB. The fingerprint is the first 8 bytes of BLAKE3.

## Usage

Object-level policies:

```bash
cd simulation
python main.py --config config/tevictranges.yaml --trace traces/IBMObjectStoreTrace065Part0.typeE.mc-aug --vm 5 --setbase --simversion 1 --tag tevict_ranges_T65
```

`chunk_tevict`:

```bash
cd simulation
python -m src.dedup.aware_replay \
  --config config/chunk_tevict_2reg_fb.yaml \
  --trace traces/IBMObjectStoreTrace065Part0.typeE.mc-aug \
  --chunks /data/dedup_exp/T065 \
  --out results_chunk_tevict_2reg_T65.json
```

`--max-ops N` stops after N requests. Configs for 2, 3, 6, and 9 regions are `config/chunk_tevict_{2,3,6,9}reg_{fb,fp}.yaml`.

The matrix runner writes one JSON per job under `simulation/results_fig5_matrix/`. Chunk indexes are read from `/data/dedup_exp/T{num}`. Traces are read from `simulation/traces` (2 regions) or `simulation/traces{3,6,9}` (more regions).

```bash
cd simulation
python run_fig5_matrix_parallel.py --setups 2reg --traces T65 --policies chunk_tevict,tevict_ranges --workers 2
```

`--fp` turns off the fixed base region. Result tags then end in `_fp`. `--types A,B,C,D` selects the workload type when the region count is 3, 6, or 9. Two-region traces are type E.

## Fixed base region

`--setbase` stores every object in the region that first wrote it, for the whole trace. Omit the flag and the first copy can expire, but some region always keeps one copy. `chunk_tevict` reads the same switch from `fixed_base_region` in the YAML file. On a chunk miss the simulator fetches each missing chunk from the cheapest region that still holds it, and sets that chunk's TTL from the live leases in the other regions.

## Simulator version

`--simversion 1` is the version used here. It applies expirations from the request timestamp instead of enqueueing an event per eviction. The files named `*_v2.py` belong to this version, including `src/simulator_v2.py`, `src/placement_policy/policy_tevict_ranges_new.py`, and `src/transfer_policy/policy_cheapest_v2.py`. `chunk_tevict` uses that TTL implementation and the same price graph, and keeps its own per-chunk lease state in `src/dedup/`.
