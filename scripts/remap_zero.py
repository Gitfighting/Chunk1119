#!/usr/bin/env python3
"""
Zero-error remap of all five traces, three-level strategy:
  L1  single-file assignment (nearest pool file within +/-10MB window)
  L2  already-built same-family combo tars (they are pool files too)
  L3  trim/pad recipe in the manifest: payload = first S bytes of the file
      (trim), or file bytes + cyclic same-file bytes (pad) -> EXACTLY S bytes.
Disk files are never modified; the manifest is the authoritative recipe.

Stages:
  1. Rebuild pool index (parallel walk of /data/file_pool, 8 procs)
       -> /data/pool2_sorted.tsv  (size \t relpath, sorted by size)
  2. Per trace: unique obj -> greedy order-preserving assignment,
       write /data/manifests/<trace>.manifest.csv.gz  (+ .misses.csv)
  3. Coverage + |delta| stats per trace.
  4. Usage aggregation over all traces -> deletion-candidate reports.
"""
import os, csv, gzip, stat, time, subprocess, sys
import numpy as np
from collections import defaultdict
from multiprocessing import Pool

FP = "/data/file_pool"
TR = "/data/skystore/traces"
TMP = "/data/tmp_index"
SORTED = "/data/pool2_sorted.tsv"
MAN = "/data/manifests"
REP = "/data/reports"
WIN = 10 * 1024 * 1024
NPROC = 8

TRACES = [
    "IBMObjectStoreTrace015Part0.typeE.mc-aug.csv",
    "IBMObjectStoreTrace029Part0.typeE.mc-aug.csv",
    "IBMObjectStoreTrace065Part0.typeE.mc-aug.csv",
    "IBMObjectStoreTrace078Part0.typeE.mc-aug.csv",
    "IBMObjectStoreTrace079Part0.typeE.mc-aug.csv",
]


# ---------- stage 1: index ----------

def walk_top(args):
    i, top = args
    outp = os.path.join(TMP, f"{i:04d}.tsv")
    n = 0
    with open(outp, "w") as out:
        base = os.path.join(FP, top)
        for dirpath, _dirs, files in os.walk(base):
            for f in files:
                p = os.path.join(dirpath, f)
                try:
                    st = os.stat(p, follow_symlinks=False)
                except OSError:
                    continue
                if not stat.S_ISREG(st.st_mode) or st.st_size <= 0:
                    continue
                rel = os.path.relpath(p, FP)
                if "\t" in rel or "\n" in rel:
                    continue
                out.write(f"{st.st_size}\t{rel}\n")
                n += 1
    return (top, n)


def stage1_index():
    print("[stage1] rebuild pool index", flush=True)
    t0 = time.time()
    os.makedirs(TMP, exist_ok=True)
    for f in os.listdir(TMP):
        os.remove(os.path.join(TMP, f))
    tops = sorted(os.listdir(FP))
    with Pool(NPROC) as pool:
        res = pool.map(walk_top, list(enumerate(tops)), chunksize=4)
    tot = sum(n for _t, n in res)
    print(f"  walked {len(tops)} topdirs, {tot:,} files "
          f"({time.time()-t0:.0f}s)", flush=True)
    r = subprocess.run(
        ["bash", "-c",
         f"sort -k1,1n -S 6G --parallel={NPROC} -T {TMP} {TMP}/*.tsv -o {SORTED}"],
        capture_output=True, text=True, timeout=3600)
    if r.returncode != 0:
        print("  SORT FAILED:", r.stderr[-500:], flush=True)
        sys.exit(1)
    print(f"  sorted -> {SORTED} ({time.time()-t0:.0f}s)", flush=True)
    for f in os.listdir(TMP):
        os.remove(os.path.join(TMP, f))
    return tot


def load_sizes(nfiles):
    print("[load] sizes array", flush=True)
    sizes = np.empty(nfiles, dtype=np.int64)
    i = 0
    with open(SORTED) as f:
        for line in f:
            sizes[i] = int(line.split("\t", 1)[0])
            i += 1
    assert i == nfiles
    return sizes


# ---------- stage 2: per-trace assignment ----------

def read_trace_objs(trace):
    puts = {}
    with open(os.path.join(TR, trace)) as f:
        for row in csv.DictReader(f):
            if row.get("op", "").upper() == "PUT":
                k = row.get("obj_key", "")
                if k and k not in puts:
                    puts[k] = int(float(row["size"]))
    objs = sorted(puts.items(), key=lambda kv: (kv[1], kv[0]))
    return objs


def assign_trace(trace, sizes, used_global):
    """Nearest-neighbor greedy (order-preserving window); returns stats."""
    tag = trace.split("Trace")[1][:3]
    objs = read_trace_objs(trace)
    n = len(objs)
    print(f"[T{tag}] {n:,} unique objects", flush=True)

    assigned = np.full(n, -1, dtype=np.int64)   # pool index per obj
    offset = np.zeros(n, dtype=np.int64)        # extent offset (0 = whole file)
    is_ext = np.zeros(n, dtype=bool)
    nfiles = len(sizes)

    # Pass 1 (whole file, objects DESCENDING):
    #   accept exact, trim <= WIN, or pad that is tiny both absolutely
    #   (<= PAD_TINY) and relatively (<= 5% of object size); defer the rest.
    PAD_TINY = 64 * 1024
    buf = np.empty(nfiles, dtype=np.int64)      # unused indices >= current s
    tail = 0
    ptr = nfiles - 1
    deferred = []                               # descending object order
    for i in range(n - 1, -1, -1):
        s = objs[i][1]
        if s == 0:
            continue  # empty payload, no file needed
        while ptr >= 0 and sizes[ptr] >= s:
            buf[tail] = ptr
            tail += 1
            ptr -= 1
        cand_a = buf[tail - 1] if tail > 0 else -1        # nearest >= s (trim)
        cand_b = ptr if ptr >= 0 else -1                  # nearest <  s (pad)
        d_a = sizes[cand_a] - s if cand_a >= 0 else None
        d_b = s - sizes[cand_b] if cand_b >= 0 else None
        if cand_b >= 0 and d_b <= PAD_TINY and d_b * 20 <= s \
                and (cand_a < 0 or d_b <= d_a):
            assigned[i] = cand_b
            ptr -= 1
        elif cand_a >= 0 and d_a <= WIN:
            assigned[i] = cand_a
            tail -= 1
        else:
            deferred.append(i)

    # Pass 2 (extent carving): serve deferred objects with disjoint byte
    # ranges carved from the largest files not whole-file-assigned here.
    miss_idx = []
    if deferred:
        wf_used = np.zeros(nfiles, dtype=bool)
        wf_used[assigned[assigned >= 0]] = True
        carve_idx = nfiles - 1
        cur = -1
        rem = 0
        off = 0
        carve_files = set()
        for i in deferred:                       # descending sizes
            s = objs[i][1]
            while rem < s and carve_idx >= 0:
                if not wf_used[carve_idx]:
                    cur = carve_idx
                    rem = sizes[cur]
                    off = 0
                    carve_idx -= 1
                    if rem >= s:
                        break
                    rem = 0                      # too small entirely, skip
                else:
                    carve_idx -= 1
            if rem < s:
                miss_idx.append(i)
                continue
            assigned[i] = cur
            offset[i] = off
            is_ext[i] = True
            carve_files.add(cur)
            off += s
            rem -= s

    # manifest: stream paths file once, visiting assigned indices ascending
    os.makedirs(MAN, exist_ok=True)
    man_path = os.path.join(MAN, trace.replace(".csv", "") + ".manifest.csv.gz")
    exact = trim = padc = extc = empty = 0
    dmax = 0
    dsum = 0
    pad_bytes = 0
    ext_bytes = 0
    pairs = sorted((int(assigned[i]), i) for i in range(n) if assigned[i] >= 0)
    with gzip.open(man_path, "wt", newline="") as gz:
        w = csv.writer(gz)
        w.writerow(["obj_key", "size_trace", "file_rel", "file_size",
                    "offset", "delta", "action"])
        for i, (k, s) in enumerate(objs):
            if s == 0:
                w.writerow([k, 0, "", 0, 0, 0, "empty"])
                empty += 1
        with open(SORTED) as f:
            fi = -1
            it = iter(pairs)
            cur = next(it, None)
            for line in f:
                fi += 1
                if cur is None:
                    break
                if fi < cur[0]:
                    continue
                sz_s, rel = line.rstrip("\n").split("\t", 1)
                fsz = int(sz_s)
                while cur is not None and cur[0] == fi:
                    oi = cur[1]
                    k, s = objs[oi]
                    if is_ext[oi]:
                        act = "extent"; extc += 1
                        ext_bytes += s
                        w.writerow([k, s, rel, fsz, int(offset[oi]), 0, act])
                    else:
                        d = fsz - s
                        if d == 0:
                            act = "exact"; exact += 1
                        elif d > 0:
                            act = "trim"; trim += 1
                        else:
                            act = "pad"; padc += 1
                            pad_bytes += -d
                        dsum += abs(d)
                        dmax = max(dmax, abs(d))
                        w.writerow([k, s, rel, fsz, 0, d, act])
                    cur = next(it, None)

    if miss_idx:
        with open(os.path.join(MAN, trace.replace(".csv", "") + ".misses.csv"),
                  "w", newline="") as mf:
            w = csv.writer(mf)
            w.writerow(["obj_key", "size_trace"])
            for i in miss_idx:
                w.writerow([objs[i][0], objs[i][1]])

    mask = assigned[assigned >= 0]
    used_global[mask] = True
    ok = len(mask) + empty
    tot_s = sum(s for _k, s in objs)
    print(f"[T{tag}] assigned={len(mask):,} empty={empty} miss={len(miss_idx)} "
          f"-> coverage {ok}/{n} ({100*ok/n:.3f}%)", flush=True)
    print(f"[T{tag}] recipes: exact={exact:,} trim={trim:,} "
          f"pad={padc:,} extent={extc:,} "
          f"|d|max={dmax/1e6:.2f}MB |d|mean={(dsum/max(1,len(mask)))/1e3:.2f}KB "
          f"pad_share={100*pad_bytes/max(1,tot_s):.4f}% "
          f"extent_share={100*ext_bytes/max(1,tot_s):.4f}%",
          flush=True)
    if miss_idx:
        big = sorted((objs[i][1] for i in miss_idx), reverse=True)[:10]
        print(f"[T{tag}] top misses: {[f'{x/1e9:.3f}G' for x in big]}", flush=True)
    return {"trace": trace, "n": n, "ok": ok, "miss": len(miss_idx),
            "exact": exact, "trim": trim, "pad": padc, "extent": extc,
            "empty": empty, "dmax": dmax}


# ---------- stage 4: usage + deletion candidates ----------

def stage4_usage(used_global, nfiles):
    print("[stage4] usage aggregation", flush=True)
    os.makedirs(REP, exist_ok=True)
    top_stat = defaultdict(lambda: [0, 0, 0, 0])  # bytes_tot,f_tot,bytes_used,f_used
    unused_tars = []
    with open(SORTED) as f:
        for fi, line in enumerate(f):
            sz_s, rel = line.rstrip("\n").split("\t", 1)
            sz = int(sz_s)
            top = rel.split("/", 1)[0]
            st = top_stat[top]
            st[0] += sz
            st[1] += 1
            if used_global[fi]:
                st[2] += sz
                st[3] += 1
            elif top == "mapped_tars":
                unused_tars.append((rel, sz))

    with open(os.path.join(REP, "pool_usage_by_topdir.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["topdir", "bytes_total", "files_total",
                    "bytes_used", "files_used", "used_pct"])
        for top, (bt, ft, bu, fu) in sorted(top_stat.items(),
                                            key=lambda kv: -kv[1][0]):
            w.writerow([top, bt, ft, bu, fu,
                        f"{100*bu/bt:.2f}" if bt else "0"])

    unused_tars.sort(key=lambda x: -x[1])
    with open(os.path.join(REP, "unused_mapped_tars.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["file_rel", "bytes"])
        for rel, sz in unused_tars:
            w.writerow([rel, sz])

    zero_dirs = [(t, s[0]) for t, s in top_stat.items() if s[3] == 0]
    zero_dirs.sort(key=lambda x: -x[1])
    tot_unused_tar = sum(sz for _r, sz in unused_tars)
    tot_zero_dir = sum(b for _t, b in zero_dirs)
    with open(os.path.join(REP, "deletion_candidates.md"), "w") as f:
        f.write("# Deletion candidates (no file referenced by any manifest)\n\n")
        f.write(f"unused mapped_tars: {len(unused_tars)} files, "
                f"{tot_unused_tar/1e9:.1f} GB (see unused_mapped_tars.csv)\n\n")
        f.write(f"top-level dirs with zero usage: {len(zero_dirs)}, "
                f"{tot_zero_dir/1e9:.1f} GB\n\n")
        for t, b in zero_dirs:
            f.write(f"- {t}  {b/1e9:.2f} GB\n")
    print(f"  unused mapped_tars: {len(unused_tars)} ({tot_unused_tar/1e9:.1f}G)",
          flush=True)
    print(f"  zero-usage topdirs: {len(zero_dirs)} ({tot_zero_dir/1e9:.1f}G)",
          flush=True)


def main():
    t0 = time.time()
    if os.path.exists(SORTED) and os.path.getsize(SORTED) > 0 \
            and "--reindex" not in sys.argv:
        r = subprocess.run(["wc", "-l", SORTED], capture_output=True, text=True)
        nfiles = int(r.stdout.split()[0])
        print(f"[stage1] reuse existing index: {nfiles:,} files", flush=True)
    else:
        nfiles = stage1_index()
    sizes = load_sizes(nfiles)
    used_global = np.zeros(nfiles, dtype=bool)

    print("[stage2/3] per-trace assignment", flush=True)
    results = []
    for trace in TRACES:
        results.append(assign_trace(trace, sizes, used_global))

    stage4_usage(used_global, nfiles)

    print("\n=== SUMMARY ===", flush=True)
    for r in results:
        print(f"  {r['trace'].split('.')[0]}: {r['ok']}/{r['n']} "
              f"miss={r['miss']} exact={r['exact']} trim={r['trim']} "
              f"pad={r['pad']} extent={r['extent']} empty={r['empty']}",
              flush=True)
    print(f"total {time.time()-t0:.0f}s", flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
