#!/usr/bin/env python3
"""
Extract selected GregoBase tables from a phpMyAdmin/MySQL dump into CSV files.

By default, extracts the following tables into `extract/csv/`:
  - gregobase_chants
  - gregobase_sources
  - gregobase_chant_sources

You can override the input dump path and output directory.

Usage examples:
  scripts/extract_tables.py                 # uses defaults
  scripts/extract_tables.py -i raw/gregobase_online.sql -o temp/
  scripts/extract_tables.py -t gregobase_chants -o temp/
  scripts/extract_tables.py -t gregobase_chants -t gregobase_sources
"""
from __future__ import annotations

import argparse
import csv
import os
from typing import Iterator, List, Tuple


DEFAULT_TABLES = [
    "gregobase_chants",
    "gregobase_sources",
    "gregobase_chant_sources",
]
DEFAULT_INPUT = os.path.join("raw", "gregobase_online.sql")
DEFAULT_OUTDIR = os.path.join("extract", "csv")


def iter_insert_chunks(fp: Iterator[str], table: str) -> Iterator[Tuple[List[str], str]]:
    """
    Yield (columns, values_blob) for each INSERT INTO `table` ... VALUES ...;

    Accumulates lines until the terminating semicolon. Returns the explicit
    column list parsed from the INSERT statement and the raw VALUES blob as a
    single string.
    """
    acc: List[str] = []
    capturing = False
    prefix = f"INSERT INTO `{table}`"
    for raw in fp:
        line = raw.rstrip("\n")
        if not capturing:
            if line.startswith(prefix):
                acc = [line]
                if line.strip().endswith(";"):
                    chunk = "\n".join(acc)
                    cols, values = _split_insert(chunk)
                    if cols is not None and values is not None:
                        yield cols, values
                    acc = []
                else:
                    capturing = True
            continue
        else:
            acc.append(line)
            if line.strip().endswith(";"):
                chunk = "\n".join(acc)
                cols, values = _split_insert(chunk)
                if cols is not None and values is not None:
                    yield cols, values
                acc = []
                capturing = False


def _split_insert(chunk: str) -> Tuple[List[str] | None, str | None]:
    """Split an INSERT chunk into column list and VALUES blob."""
    try:
        pos_open = chunk.index("(")
        pos_close = chunk.index(")", pos_open + 1)
        cols_str = chunk[pos_open + 1 : pos_close]

        rest = chunk[pos_close + 1 :]
        kw = "VALUES"
        idx_vals = rest.upper().find(kw)
        if idx_vals < 0:
            return None, None
        values_blob = rest[idx_vals + len(kw) :].rstrip(";\n\r ")

        cols = [c.strip().strip("`") for c in cols_str.split(",")]
        return cols, values_blob
    except ValueError:
        return None, None


def parse_values_blob(blob: str) -> List[List[str]]:
    """
    Parse a VALUES blob like: (v1,v2,...),(v1,v2,...)

    Handles:
      - Single-quoted SQL strings with backslash escapes and doubled ''
      - Unquoted NULL (becomes empty string)
      - Numbers (kept as text)
      - Newlines within quoted strings
    """
    rows: List[List[str]] = []
    cur_row: List[str] = []
    cur: List[str] = []
    depth = 0
    in_str = False
    i = 0
    n = len(blob)
    while i < n:
        ch = blob[i]
        nxt = blob[i + 1] if i + 1 < n else ""

        if in_str:
            if ch == "\\":
                if i + 1 < n:
                    esc = blob[i + 1]
                    mapping = {
                        "n": "\n",
                        "r": "\r",
                        "t": "\t",
                        "'": "'",
                        '"': '"',
                        "\\": "\\",
                    }
                    cur.append(mapping.get(esc, esc))
                    i += 2
                    continue
                else:
                    cur.append("\\")
            elif ch == "'":
                if nxt == "'":
                    cur.append("'")
                    i += 2
                    continue
                else:
                    in_str = False
            else:
                cur.append(ch)
            i += 1
            continue

        if ch == "'":
            in_str = True
            i += 1
            continue
        if ch == "(":
            if depth == 0:
                cur_row = []
                cur = []
            else:
                cur.append(ch)
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth -= 1
            if depth == 0:
                token = "".join(cur).strip()
                cur_row.append(sql_token_to_text(token))
                rows.append(cur_row)
                cur = []
                cur_row = []
            else:
                cur.append(ch)
            i += 1
            continue
        if ch == "," and depth == 1:
            token = "".join(cur).strip()
            cur_row.append(sql_token_to_text(token))
            cur = []
            i += 1
            continue
        if ch == ";" and depth == 0:
            i += 1
            continue
        cur.append(ch)
        i += 1

    return rows


def sql_token_to_text(token: str) -> str:
    if token.upper() == "NULL":
        return ""
    return token


def ensure_out_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def extract_table(input_path: str, out_dir: str, table: str) -> str:
    ensure_out_dir(out_dir)
    out_file = os.path.join(out_dir, f"{table}.csv")

    wrote_header = False
    header: List[str] = []

    with open(input_path, "r", encoding="utf-8", errors="replace") as f, \
         open(out_file, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        for cols, values_blob in iter_insert_chunks(f, table):
            if not wrote_header:
                header = cols
                writer.writerow(header)
                wrote_header = True
            rows = parse_values_blob(values_blob)
            for row in rows:
                if len(row) != len(header):
                    if len(row) < len(header):
                        row = row + [""] * (len(header) - len(row))
                    else:
                        row = row[: len(header)]
                writer.writerow(row)

    return out_file


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract GregoBase tables to CSV")
    p.add_argument(
        "-i",
        "--input",
        default=DEFAULT_INPUT,
        help=f"Path to SQL dump (default: {DEFAULT_INPUT})",
    )
    p.add_argument(
        "-o",
        "--outdir",
        default=DEFAULT_OUTDIR,
        help=f"Output directory for CSVs (default: {DEFAULT_OUTDIR})",
    )
    p.add_argument(
        "-t",
        "--table",
        dest="tables",
        action="append",
        help=(
            "Table name to extract (repeat for multiple). "
            f"Default: {', '.join(DEFAULT_TABLES)}"
        ),
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    tables = args.tables if args.tables else list(DEFAULT_TABLES)

    for t in tables:
        out_path = extract_table(args.input, args.outdir, t)
        print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

