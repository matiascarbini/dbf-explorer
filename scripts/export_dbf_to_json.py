from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from dbfread import DBF


def normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("latin1", errors="ignore")
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def export_table(path: Path, output_dir: Path, encoding: str) -> Path:
    table = DBF(
        str(path),
        load=False,
        encoding=encoding,
        char_decode_errors="ignore",
        ignore_missing_memofile=True,
    )

    fields = [
        {
            "name": field.name,
            "type": field.type,
            "length": field.length,
            "decimal_count": field.decimal_count,
        }
        for field in table.fields
    ]

    records = [
        {field.name: normalize_value(record.get(field.name)) for field in table.fields}
        for record in table
    ]

    payload = {
        "table": path.stem,
        "source_file": str(path),
        "encoding": encoding,
        "record_count": len(records),
        "fields": fields,
        "records": records,
    }

    output_path = output_dir / f"{path.stem}.json"
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Export DBF files to JSON with every field and record.")
    parser.add_argument("dbf_files", nargs="+", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("exports"))
    parser.add_argument("--encoding", default="latin1")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for dbf_file in args.dbf_files:
        output_path = export_table(dbf_file, args.output_dir, args.encoding)
        print(output_path)


if __name__ == "__main__":
    main()
