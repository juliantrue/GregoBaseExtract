#!/usr/bin/env python3
import csv
import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
CSV_DIR = BASE_DIR / "extract" / "csv"
OUTPUT_PATH = BASE_DIR / "extract" / "chants.jsonl"


def load_csv_dicts(path):
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Unify GregoBase CSVs into chants JSONL")
    parser.add_argument("--output", "-o", default=None, help="Output JSONL path (default: extract/chants.jsonl)")
    parser.add_argument("--gzip", action="store_true", help="Also gzip the output JSONL")
    parser.add_argument("--rm", action="store_true", help="Remove uncompressed JSONL after gzip")
    args = parser.parse_args()
    chants_path = CSV_DIR / "gregobase_chants.csv"
    sources_path = CSV_DIR / "gregobase_sources.csv"
    chant_sources_path = CSV_DIR / "gregobase_chant_sources.csv"

    if not chants_path.exists():
        raise FileNotFoundError(f"Missing chants CSV: {chants_path}")
    if not sources_path.exists():
        raise FileNotFoundError(f"Missing sources CSV: {sources_path}")
    if not chant_sources_path.exists():
        raise FileNotFoundError(f"Missing chant-sources CSV: {chant_sources_path}")

    chants = load_csv_dicts(chants_path)
    sources = load_csv_dicts(sources_path)
    chant_sources = load_csv_dicts(chant_sources_path)

    # Index sources by id (string key for consistency with CSV)
    sources_by_id = {}
    for s in sources:
        sid = s.get("id")
        if sid is None:
            continue
        sources_by_id[sid] = s

    # Group chant->sources
    sources_by_chant = {}
    for cs in chant_sources:
        chant_id = cs.get("chant_id")
        if chant_id is None:
            continue
        src_id = cs.get("source")
        entry = {
            "id": src_id,
            "page": cs.get("page"),
            "sequence": cs.get("sequence"),
            "extent": cs.get("extent"),
            "source": sources_by_id.get(src_id),
        }
        sources_by_chant.setdefault(chant_id, []).append(entry)

    # Resolve output path
    out_path = Path(args.output) if args.output else OUTPUT_PATH
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Stream out JSONL
    with out_path.open("w", encoding="utf-8") as out:
        for chant in chants:
            chant_id = chant.get("id")
            # Attach sources list (empty list if none)
            chant_obj = dict(chant)
            chant_obj["sources"] = sources_by_chant.get(chant_id, [])
            out.write(json.dumps(chant_obj, ensure_ascii=False) + "\n")

    print(f"Wrote {out_path}")

    if args.gzip:
        try:
            from compress_jsonl import compress_jsonl
        except Exception:
            # Fallback inline gzip if import path resolution fails
            import gzip, shutil
            gz_path = out_path.with_suffix(out_path.suffix + ".gz")
            with out_path.open("rb") as fin, gzip.open(gz_path, "wb") as fout:
                shutil.copyfileobj(fin, fout, length=1024 * 1024)
            if args.rm:
                out_path.unlink(missing_ok=True)
            print(f"Wrote {gz_path}")
        else:
            gz = compress_jsonl(out_path, keep=not args.rm)
            print(f"Wrote {gz}")


if __name__ == "__main__":
    main()
