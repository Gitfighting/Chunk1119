#!/usr/bin/env python3
"""
Same-family combo + same-family real-byte padding to EXACT target size.

For each remaining T079 gap (linux family only, no cross-type mix):
  Phase A: append whole linux version trees into the tar (largest first),
           with a conservative overhead guard so we never overshoot.
  Phase B: drill L1 subdirs of reserve trees to shrink the remainder.
  Phase C: append real linux source bytes (pad stream built from an unused
           linux tree) so the final file is EXACTLY target bytes.

Final |size - target| == 0. Output: mapped_tars/fam_<target>.tar
"""
import os, sys, time, subprocess, shutil
from multiprocessing import Pool, cpu_count

ROOT = "/data/file_pool"
OUT = "/data/file_pool/mapped_tars"
PAD = "/data/pad_linux_src.bin"
PAD_MIN = 2_000_000_000          # pad stream size
TARGETS = [11390233215, 12228869939]
GUARD = 1.15                     # est tar size = du_bytes * GUARD + 8MB
GUARD_ADD = 8 * 1024 * 1024
DRILL_STOP = 32 * 1024 * 1024    # stop drilling when remainder below this


def du_sb(path, timeout=180):
    try:
        if os.path.isfile(path):
            return os.path.getsize(path)
        r = subprocess.run(["du", "-sb", "--max-depth=0", path],
                           capture_output=True, text=True, timeout=timeout)
        if r.returncode == 0 and r.stdout.strip():
            return int(r.stdout.strip().split()[0])
    except Exception:
        pass
    return 0


def list_linux_roots():
    roots = []
    for name in sorted(os.listdir(ROOT)):
        p = os.path.join(ROOT, name)
        if name.startswith("linux-") and os.path.isdir(p):
            roots.append(name)
    lk = os.path.join(ROOT, "linux_kernel")
    if os.path.isdir(lk):
        for f in sorted(os.listdir(lk)):
            if os.path.isdir(os.path.join(lk, f)):
                roots.append(os.path.join("linux_kernel", f))
    return roots


def _size_one(rel):
    return (rel, du_sb(os.path.join(ROOT, rel)))


def est_tar(du_bytes):
    return int(du_bytes * GUARD) + GUARD_ADD


def tar_append(outfile, rels):
    """Append paths to (possibly new) uncompressed tar. Returns True on success."""
    args = ["tar", "-rf", outfile, "-C", ROOT] + rels
    r = subprocess.run(args, capture_output=True, timeout=7200)
    return r.returncode == 0


def build_pad(unused_tree_rel):
    """Concatenate real source files from an unused linux tree into PAD stream."""
    if os.path.exists(PAD) and os.path.getsize(PAD) >= PAD_MIN:
        return
    print(f"[pad] building from {unused_tree_rel}", flush=True)
    base = os.path.join(ROOT, unused_tree_rel)
    written = 0
    with open(PAD, "wb") as out:
        for dirpath, _dirs, files in os.walk(base):
            for f in files:
                p = os.path.join(dirpath, f)
                try:
                    if not os.path.isfile(p):
                        continue
                    with open(p, "rb") as fin:
                        shutil.copyfileobj(fin, out, 1024 * 1024)
                    written += os.path.getsize(p)
                except Exception:
                    continue
                if written >= PAD_MIN:
                    print(f"[pad] done {written/1e9:.2f}G", flush=True)
                    return
    print(f"[pad] done (tree exhausted) {written/1e9:.2f}G", flush=True)


def pad_to_exact(outfile, target):
    cur = os.path.getsize(outfile)
    need = target - cur
    assert need >= 0, f"overshoot: {cur} > {target}"
    pad_size = os.path.getsize(PAD)
    with open(outfile, "ab") as out, open(PAD, "rb") as pin:
        while need > 0:
            pin.seek(0)
            left = min(need, pad_size)
            while left > 0:
                chunk = pin.read(min(8 * 1024 * 1024, left))
                if not chunk:
                    break
                out.write(chunk)
                need -= len(chunk)
                left -= len(chunk)
    return os.path.getsize(outfile)


def list_subdirs(rel, min_bytes):
    base = os.path.join(ROOT, rel)
    kids = []
    try:
        for name in sorted(os.listdir(base)):
            p = os.path.join(base, name)
            sz = du_sb(p, timeout=90)
            if sz >= min_bytes:
                kids.append((os.path.join(rel, name), sz))
    except Exception:
        pass
    kids.sort(key=lambda x: -x[1])
    return kids


def build_target(job):
    target, whole_trees, reserve_trees = job
    tag = f"[{target/1e9:.3f}G]"
    outfile = os.path.join(OUT, f"fam_{target}.tar")
    if os.path.exists(outfile):
        if os.path.getsize(outfile) == target:
            print(f"{tag} already exact, skip", flush=True)
            return (target, True, target)
        os.remove(outfile)
    t0 = time.time()

    # Phase A: whole trees
    cur = 0
    for rel, sz in whole_trees:
        if cur + est_tar(sz) > target:
            continue
        if not tar_append(outfile, [rel]):
            print(f"{tag} tar append failed: {rel}", flush=True)
            continue
        cur = os.path.getsize(outfile)
        print(f"{tag} +tree {rel[-50:]} -> {cur/1e9:.3f}G rem={(target-cur)/1e6:.1f}MB",
              flush=True)
        if cur > target:
            print(f"{tag} OVERSHOOT, abort", flush=True)
            os.remove(outfile)
            return (target, False, cur)

    # Phase B: drill L1 (and L2) of reserve trees
    for rel, _sz in reserve_trees:
        rem = target - (os.path.getsize(outfile) if os.path.exists(outfile) else 0)
        if rem <= DRILL_STOP:
            break
        for krel, ksz in list_subdirs(rel, 500_000):
            cur = os.path.getsize(outfile) if os.path.exists(outfile) else 0
            rem = target - cur
            if rem <= DRILL_STOP:
                break
            if cur + est_tar(ksz) <= target:
                if tar_append(outfile, [krel]):
                    cur = os.path.getsize(outfile)
                    print(f"{tag} +L1 {krel[-55:]} rem={(target-cur)/1e6:.1f}MB", flush=True)
            elif ksz > rem:
                for grel, gsz in list_subdirs(krel, 200_000):
                    cur = os.path.getsize(outfile)
                    rem = target - cur
                    if rem <= DRILL_STOP:
                        break
                    if cur + est_tar(gsz) <= target:
                        if tar_append(outfile, [grel]):
                            cur = os.path.getsize(outfile)
                            print(f"{tag} +L2 {grel[-55:]} rem={(target-cur)/1e6:.1f}MB",
                                  flush=True)

    # Phase C: exact pad with real linux bytes
    cur = os.path.getsize(outfile)
    print(f"{tag} pad {(target-cur)/1e6:.1f}MB with real linux bytes", flush=True)
    final = pad_to_exact(outfile, target)
    ok = (final == target)
    print(f"{tag} {'EXACT' if ok else 'FAIL'} {final} bytes "
          f"({(time.time()-t0)/60:.1f} min)", flush=True)
    return (target, ok, final)


def main():
    os.makedirs(OUT, exist_ok=True)
    print("Phase 0: size linux roots", flush=True)
    roots = list_linux_roots()
    with Pool(min(16, max(4, cpu_count()))) as pool:
        sized = pool.map(_size_one, roots, chunksize=4)
    trees = [(rel, sz) for rel, sz in sized if 200_000_000 <= sz <= 1_500_000_000]
    trees.sort(key=lambda x: -x[1])
    print(f"  usable trees={len(trees)} free={shutil.disk_usage('/data').free/1e9:.1f}G",
          flush=True)
    if len(trees) < 20:
        print("ERROR: not enough linux trees", flush=True)
        sys.exit(1)

    # plan disjoint assignments
    used = set()
    jobs = []
    for target in TARGETS:
        whole, acc = [], 0
        for rel, sz in trees:
            if rel in used:
                continue
            if acc + est_tar(sz) <= target:
                whole.append((rel, sz))
                used.add(rel)
                acc += est_tar(sz)
        reserve = []
        for rel, sz in trees:
            if rel not in used:
                reserve.append((rel, sz))
                used.add(rel)
            if len(reserve) >= 4:
                break
        jobs.append((target, whole, reserve))
        print(f"  plan {target/1e9:.3f}G: {len(whole)} whole trees "
              f"(est {acc/1e9:.2f}G) + {len(reserve)} reserve", flush=True)

    # pad stream from a still-unused tree (or last resort: first tree)
    pad_src = next((rel for rel, _ in trees if rel not in used), trees[0][0])
    build_pad(pad_src)

    print("Phase 1: build (2 parallel)", flush=True)
    with Pool(2) as pool:
        results = pool.map(build_target, jobs)

    print("\nPhase 2: verify", flush=True)
    all_ok = True
    for target, ok, final in results:
        print(f"  {target}: {'EXACT' if ok else 'FAIL'} ({final})", flush=True)
        all_ok &= ok
    print(subprocess.run(["df", "-h", "/data"], capture_output=True,
                         text=True).stdout.strip().split("\n")[-1], flush=True)
    print("DONE" if all_ok else "DONE_WITH_FAIL", flush=True)


if __name__ == "__main__":
    main()
