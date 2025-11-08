# GregoBaseExtract

<img src="assets/scribe.png" alt="GregoBase Extract Scribe" width="160" align="left" />

Extracts structured data from the GregoBase SQL dump for downstream analysis and tooling. Upstream: https://github.com/gregorio-project/GregoBase

<br clear="left" />

## Contents
- `raw/gregobase_online.sql` — SQL dump copied from the GregoBase repo
- `GREGOBASE_CHECKSUM` — SHA‑256 checksum of the SQL dump
- `extract/csv/` — CSVs (`gregobase_chants.csv`, `gregobase_sources.csv`, `gregobase_chant_sources.csv`)
- `extract/jsonl/chants.jsonl.gz` — unified chants JSONL (gzipped)

## Update Workflow
- Manual workflow: `.github/workflows/create-extract.yml` (Run workflow in GitHub UI)
- Steps performed:
  - Clone `gregorio-project/GregoBase`
  - Copy `gregobase_online.sql` into `raw/`
  - Write SHA‑256 to `GREGOBASE_CHECKSUM`
  - Run extraction scripts to produce CSVs and JSONL
  - Open a pull request titled with the GregoBase commit SHA

## Local Usage
1. Ensure Python 3.11+ is available.
2. Place the dump at `raw/gregobase_online.sql`.
3. Extract CSVs:
   ```bash
   python3 scripts/extract_tables.py -i raw/gregobase_online.sql -o extract/csv
   ```
4. Build unified JSONL (gzip and remove uncompressed file):
   ```bash
   python3 scripts/unify_chants.py --output extract/jsonl/chants.jsonl --gzip --rm
   ```

## Notes
- JSONL gzip falls back to Python’s `gzip` if optional helpers are unavailable.
- Very large SQL files may exceed GitHub’s limits; this repo currently commits the dump and derived outputs.

## License

Released under the same license. See `LICENSE`.
