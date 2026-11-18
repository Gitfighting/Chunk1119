#!/usr/bin/env python3
"""
Stage A of the DedupAwareTevict experiment: build the chunk-fingerprint cache.

For every object in the zero-error manifest, reconstruct its payload byte
stream from the recipe (exact / trim / pad / extent / empty), run FastCDC
(min 2K / avg 8K / max 64K) and BLAKE3, and record the per-object chunk list
as (fp64, length) pairs.

Output (under /data/dedup_exp/<TAG>/):
    fps.npy     int64  - 8-byte BLAKE3 prefix of every chunk, in object order
    lens.npy    uint32 - chunk lengths, same order
    index.csv.gz       - obj_key,start,count,size  (offsets into fps/lens)

The chunk lists depend only on (obj_key -> content); they are shared by all
region variants (typeE 2reg, typeA-D 9reg) of the same trace ID.

Usage: python3 chunk_objects.py 079
"""
import csv
import gzip
import os
import sys
import time
from multiprocessing import Process

import numpy as np
from blake3 import blake3
from fastcdc.fastcdc_cy import fastcdc_cy

POOL = "/data/file_pool"
MAN = "/data/manifests"
OUT_BASE = "/data/dedup_exp"
NPROC = 8
BUF = 32 * 1024 * 1024
MIN_SZ, AVG_SZ, MAX_SZ = 2 * 1024, 8 * 1024, 64 * 1024


def payload_buffers(row):
    """Yield the object's payload as a sequence of byte buffers."""
    action = row["action"]
    size = int(row["size_trace"])
    if action == "empty" or size == 0:
        return
    path = os.path.join(POOL, row["file_rel"])
    fsize = int(row["file_size"])
    if action == "extent":
        start, remaining = int(row["offset"]), size
    elif action == "trim" or action == "exact":
        start, remaining = 0, size
    elif action == "pad":
        start, remaining = 0, fsize
    else:
        raise ValueError(f"unknown action {action}")

    with open(path, "rb") as f:
        f.seek(start)
        while remaining > 0:
            buf = f.read(min(BUF, remaining))
            if not buf:
                raise IOError(f"short read {path} at {f.tell()}")
            remaining -= len(buf)
            yield buf
        if action == "pad":
            # cyclic same-file bytes until the exact target size
            need = size - fsize
            while need > 0:
                f.seek(0)
                take = min(fsize, need)
                got = 0
                while got < take:
                    buf = f.read(min(BUF, take - got))
                    got += len(buf)
                    yield buf
                need -= take


def chunk_object(row, fps_out, lens_out):
    """CDC over the streamed payload with carry-over across buffers."""
    carry = b""
    nc = 0
    for buf in payload_buffers(row):
        data = carry + buf
        chunks = list(fastcdc_cy(data, MIN_SZ, AVG_SZ, MAX_SZ, False, None))
        if len(chunks) <= 1:
            carry = data
            continue
        last_off = chunks[-1].offset
        mv = memoryview(data)
        for c in chunks[:-1]:
            piece = mv[c.offset:c.offset + c.length]
            # uint64: BLAKE3 prefix is an unsigned 8-byte value
            fps_out.append(np.uint64(
                int.from_bytes(blake3(piece).digest()[:8], "little")))
            lens_out.append(np.uint32(c.length))
            nc += 1
        carry = bytes(mv[last_off:])
    if carry:
        for c in fastcdc_cy(carry, MIN_SZ, AVG_SZ, MAX_SZ, False, None):
            piece = carry[c.offset:c.offset + c.length]
            fps_out.append(np.uint64(
                int.from_bytes(blake3(piece).digest()[:8], "little")))
            lens_out.append(np.uint32(c.length))
            nc += 1
    return nc


def worker(wid, rows, outdir):
    # Pre-typed lists avoid OverflowError when casting unsigned 64-bit fps
    # into signed int64 (the bug that killed the first run at save time).
    fps, lens, idx = [], [], []
    done_bytes = 0
    t0 = time.time()
    for i, row in enumerate(rows):
        start = len(fps)
        n = chunk_object(row, fps, lens)
        size = int(row["size_trace"])
        idx.append((row["obj_key"], start, n, size))
        done_bytes += size
        if (i + 1) % 50 == 0 or i + 1 == len(rows):
            el = time.time() - t0
            print(f"[w{wid}] {i+1}/{len(rows)} objs "
                  f"{done_bytes/1e9:.1f}GB {done_bytes/1e6/max(el,1):.0f}MB/s",
                  flush=True)
    np.save(os.path.join(outdir, f"shard{wid}.fps.npy"),
            np.asarray(fps, dtype=np.uint64))
    np.save(os.path.join(outdir, f"shard{wid}.lens.npy"),
            np.asarray(lens, dtype=np.uint32))
    with gzip.open(os.path.join(outdir, f"shard{wid}.idx.csv.gz"), "wt",
                   newline="") as f:
        w = csv.writer(f)
        for r in idx:
            w.writerow(r)
    print(f"[w{wid}] DONE {len(idx)} objs {len(fps)} chunks", flush=True)


def main():
    tag = sys.argv[1] if len(sys.argv) > 1 else "079"
    man_path = os.path.join(
        MAN, f"IBMObjectStoreTrace{tag}Part0.typeE.mc-aug.manifest.csv.gz")
    outdir = os.path.join(OUT_BASE, f"T{tag}")
    os.makedirs(outdir, exist_ok=True)

    with gzip.open(man_path, "rt") as f:
        rows = list(csv.DictReader(f))
    print(f"[main] {len(rows)} objects, "
          f"{sum(int(r['size_trace']) for r in rows)/1e9:.1f}GB logical",
          flush=True)

    # greedy balance by byte size across workers
    buckets = [[] for _ in range(NPROC)]
    loads = [0] * NPROC
    for row in sorted(rows, key=lambda r: -int(r["size_trace"])):
        j = loads.index(min(loads))
        buckets[j].append(row)
        loads[j] += int(row["size_trace"])
    for j in range(NPROC):
        print(f"[main] worker {j}: {len(buckets[j])} objs {loads[j]/1e9:.1f}GB",
              flush=True)

    procs = [Process(target=worker, args=(j, buckets[j], outdir))
             for j in range(NPROC)]
    t0 = time.time()
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    if any(p.exitcode != 0 for p in procs):
        raise SystemExit("worker failed")

    # merge shards into flat arrays + global index
    fps_all, lens_all, idx_all = [], [], []
    off = 0
    for j in range(NPROC):
        fps = np.load(os.path.join(outdir, f"shard{j}.fps.npy"))
        lens = np.load(os.path.join(outdir, f"shard{j}.lens.npy"))
        with gzip.open(os.path.join(outdir, f"shard{j}.idx.csv.gz"), "rt") as f:
            for key, start, count, size in csv.reader(f):
                idx_all.append((key, off + int(start), int(count), int(size)))
        fps_all.append(fps)
        lens_all.append(lens)
        off += len(fps)
    np.save(os.path.join(outdir, "fps.npy"), np.concatenate(fps_all))
    np.save(os.path.join(outdir, "lens.npy"), np.concatenate(lens_all))
    with gzip.open(os.path.join(outdir, "index.csv.gz"), "wt", newline="") as f:
        w = csv.writer(f)
        w.writerow(["obj_key", "start", "count", "size"])
        for r in idx_all:
            w.writerow(r)
    for j in range(NPROC):
        for suf in ("fps.npy", "lens.npy", "idx.csv.gz"):
            os.remove(os.path.join(outdir, f"shard{j}.{suf}"))
    print(f"[main] MERGED {off} chunks, {len(idx_all)} objects "
          f"in {(time.time()-t0)/60:.1f} min -> {outdir}", flush=True)


if __name__ == "__main__":
    main()
