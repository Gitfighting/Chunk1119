#!/usr/bin/env python3
"""
Convert the raw chunk cache (fps.npy / lens.npy / index.csv.gz) into a
replay-ready, mmap-shareable store:

    lens_u.npy          int64  - length of each unique chunk (by uid)
    obj_uids_data.npy   int32  - concatenated per-object unique uid lists (CSR)
    obj_uids_indptr.npy int64  - CSR offsets (n_objects + 1)
    obj_meta.csv.gz            - obj_key,size  (row i = CSR slot i)

All replay processes mmap the same read-only arrays, so page-cache RAM is
shared across parallel runs.

Usage: python3 prep_chunkstore.py /data/dedup_exp/T079
"""
import csv
import gzip
import os
import sys
import time

import numpy as np


def main():
    d = sys.argv[1]
    t0 = time.time()
    fps = np.load(os.path.join(d, "fps.npy"))
    lens = np.load(os.path.join(d, "lens.npy"))
    uniq, inverse = np.unique(fps, return_inverse=True)
    n_unique = len(uniq)
    del fps, uniq
    inverse = inverse.astype(np.int64)
    lens_u = np.zeros(n_unique, dtype=np.int64)
    lens_u[inverse] = lens
    del lens
    np.save(os.path.join(d, "lens_u.npy"), lens_u)

    data_parts, indptr, keys, sizes = [], [0], [], []
    with gzip.open(os.path.join(d, "index.csv.gz"), "rt") as f:
        for row in csv.DictReader(f):
            s, c = int(row["start"]), int(row["count"])
            u = np.unique(inverse[s:s + c]).astype(np.int32)
            data_parts.append(u)
            indptr.append(indptr[-1] + len(u))
            keys.append(row["obj_key"])
            sizes.append(int(row["size"]))
    np.save(os.path.join(d, "obj_uids_data.npy"),
            np.concatenate(data_parts) if data_parts else
            np.empty(0, dtype=np.int32))
    np.save(os.path.join(d, "obj_uids_indptr.npy"),
            np.array(indptr, dtype=np.int64))
    with gzip.open(os.path.join(d, "obj_meta.csv.gz"), "wt", newline="") as f:
        w = csv.writer(f)
        w.writerow(["obj_key", "size"])
        for k, s in zip(keys, sizes):
            w.writerow([k, s])
    print(f"[prep] {len(keys)} objects, {n_unique/1e6:.2f}M unique chunks, "
          f"{lens_u.sum()/1e9:.1f}GB unique bytes, "
          f"{(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
