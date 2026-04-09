from __future__ import annotations

import json
import math
import os
import re
import sqlite3
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from itertools import islice
from pathlib import Path
from time import perf_counter
from typing import Any

from dbfread import DBF
from flask import Flask, abort, render_template, request


DBF_ROOT = Path(os.getenv("DBF_ROOT", "/data/dbf")).resolve()
DEFAULT_ENCODING = os.getenv("DBF_ENCODING", "latin1")
DEFAULT_PAGE_SIZE = max(1, int(os.getenv("DBF_PAGE_SIZE", "100")))
MAX_PAGE_SIZE = 500
SQLITE_CACHE_PATH = Path(os.getenv("SQLITE_CACHE_PATH", "/tmp/dbf_explorer.sqlite"))
SQL_RESULT_LIMIT = max(1, int(os.getenv("SQL_RESULT_LIMIT", "500")))

READ_ONLY_SQL = re.compile(r"^\s*(SELECT|WITH|EXPLAIN)\b", re.IGNORECASE | re.DOTALL)
FORBIDDEN_SQL = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|ATTACH|DETACH|REPLACE|PRAGMA|VACUUM|BEGIN|COMMIT)\b",
    re.IGNORECASE,
)

TABLE_HINTS = {
    "ACU_COB": "Acumulado o parametrizacion economica asociada a cobradores.",
    "ARC_ASO": "Maestro principal de asociados o socios. Tiene datos personales, plan, cobrador, zona, altas y bajas.",
    "ARC_COB": "Catalogo de cobradores o responsables de cobro.",
    "ARC_FAL": "Archivo de asociados fallecidos o historial vinculado a fallecimientos.",
    "ARC_OSO": "Catalogo corto de obras sociales o cobertura de salud.",
    "ARC_PLA": "Catalogo de planes con nombre y detalle.",
    "ARC_ZON": "Catalogo de zonas.",
    "BAJ_ASO": "Registro de bajas de asociados por codigo y fecha.",
    "DET_MOV": "Detalle historico de movimientos con datos del socio ya replicados en cada registro.",
    "MAS_EMP": "Datos maestros de la empresa o configuracion general de la entidad.",
    "MAS_LOC": "Catalogo de localidades.",
    "MOV_ASO": "Movimientos por asociado y periodo, probable cuenta corriente o cobranzas.",
    "PRE_PLA": "Escala o historico de importes por plan.",
}

TABLE_EXPORT_PRESETS = {
    "ARC_PLA": [
        {
            "key": "codigo_descripcion",
            "label": "Descargar JSON",
            "description": "Genera referencia, descripcion, activo=false e importe=0 para todos los planes.",
            "field_map": {
                "COD_PLA": "referencia",
                "NOM_PLA": "descripcion",
            },
            "static_values": {
                "activo": False,
                "importe": 0,
            },
            "skip_if_all_blank": ["COD_PLA", "NOM_PLA"],
            "filename_suffix": "codigo-descripcion",
        }
    ],
    "ARC_ASO": [
        {
            "key": "socios",
            "label": "Descargar JSON \"grupos familiares\"",
            "description": "Genera un JSON agrupado por asociado con cabecera e integrantes.",
            "group_by": "COD_ASO",
            "items_key": "items",
            "header_field_map": {
                "COD_ASO": "codigo",
                "NOM_ASO": "referencia",
                "PLA_ASO": "tipo_grupo_referencia",
            },
            "header_computed_values": {
                "activo": {
                    "source_field": "BAJ_ASO",
                    "operator": "is_blank",
                    "true_value": True,
                    "false_value": False,
                }
            },
            "header_required_not_blank": ["PLA_ASO"],
            "header_excluded_values": {
                "PLA_ASO": ["IN"],
            },
            "item_field_map": {
                "COD_ASO": "codigo",
                "ORD_ASO": "orden",
                "NOM_ASO": "nombre",
                "DIR_ASO": "direccion",
                "CPO_ASO": "codigo_postal",
                "LOC_ASO": "localidad",
                "PRO_ASO": "provincia",
                "TEL_ASO": "telefono",
                "CEL_ASO": "celular",
                "NAC_ASO": "nacimiento",
                "TDO_ASO": "tipo_documento",
                "DOC_ASO": "documento",
                "PLA_ASO": "plan",
                "COB_ASO": "cobrador",
                "ALT_ASO": "alta",
                "BAJ_ASO": "baja",
            },
            "item_required_not_blank": ["NOM_ASO"],
            "filename_suffix": "socios",
        },
        {
            "key": "individuales",
            "label": "Descargar JSON \"individuales\"",
            "description": "Genera un JSON plano de asociados individuales.",
            "field_map": {
                "COD_ASO": "codigo",
                "ORD_ASO": "orden",
                "NOM_ASO": "nombre",
                "DIR_ASO": "direccion",
                "CPO_ASO": "codigo_postal",
                "LOC_ASO": "localidad",
                "PRO_ASO": "provincia",
                "TEL_ASO": "telefono",
                "CEL_ASO": "celular",
                "NAC_ASO": "nacimiento",
                "TDO_ASO": "tipo_documento",
                "DOC_ASO": "documento",
                "PLA_ASO": "plan",
                "COB_ASO": "cobrador",
                "ALT_ASO": "alta",
                "BAJ_ASO": "baja",
            },
            "required_not_blank": ["NOM_ASO", "PLA_ASO"],
            "included_values": {
                "PLA_ASO": ["IN"],
            },
            "filename_suffix": "individuales",
        }
    ],
}

TABLE_EXPORT_PRESETS["MOV_ASO"] = [
    {
        "key": "cuotas_societarias",
        "label": "Descargar JSON \"cuotas societarias\"",
        "description": (
            "Genera el historial de cuotas societarias por grupo/socio "
            "para importar al sistema destino. Un registro por grupo por periodo."
        ),
        "sql_query": (
            "SELECT d.COD_ASO, d.ORD_ASO, d.NOM_ASO, d.NOM_PLA, "
            "d.IMP_PLA, d.IMP_MOV, d.PER_MOV, m.SAL_MOV, m.FEC_PAG "
            "FROM DET_MOV d "
            "LEFT JOIN MOV_ASO m ON m.COD_ASO = d.COD_ASO AND m.PER_MOV = d.PER_MOV "
            "WHERE d.ORD_ASO = 0 "
            "ORDER BY d.COD_ASO, d.PER_MOV"
        ),
        "field_map": {
            "COD_ASO": "grupo_referencia_externa",
            "NOM_ASO": "nombre_socio",
            "NOM_PLA": "nombre_plan",
            "IMP_PLA": "importe",
            "IMP_MOV": "importe_cobrado",
            "FEC_PAG": "fecha_pago",
        },
        "computed_values": {
            "mes": {
                "source_field": "PER_MOV",
                "operator": "str_slice",
                "start": 0,
                "end": 2,
                "cast": "int",
            },
            "anio": {
                "source_field": "PER_MOV",
                "operator": "str_slice",
                "start": 2,
                "end": 6,
                "cast": "int",
            },
            "pago": {
                "source_field": "SAL_MOV",
                "operator": "equals_value",
                "compare_to": 0,
                "true_value": True,
                "false_value": False,
            },
            "socio_referencia_externa": {
                "source_fields": ["COD_ASO", "ORD_ASO"],
                "operator": "concat",
                "separator": "/",
            },
        },
        "static_values": {
            "societaria": True,
            "forma_pago": None,
            "disciplina_id": None,
        },
        "filename_suffix": "cuotas-societarias",
    }
]

MANUAL_RELATIONSHIPS = [
    {
        "left_table": "ARC_ASO",
        "left_field": "COD_ASO",
        "right_table": "MOV_ASO",
        "right_field": "COD_ASO",
        "confidence": "alta",
        "note": "Un socio puede tener muchos movimientos.",
    },
    {
        "left_table": "ARC_ASO",
        "left_field": "COD_ASO",
        "right_table": "DET_MOV",
        "right_field": "COD_ASO",
        "confidence": "alta",
        "note": "Detalle historico por socio con datos duplicados para consulta rapida.",
    },
    {
        "left_table": "ARC_ASO",
        "left_field": "COD_ASO",
        "right_table": "BAJ_ASO",
        "right_field": "COD_ASO",
        "confidence": "alta",
        "note": "Baja del socio por codigo.",
    },
    {
        "left_table": "ARC_ASO",
        "left_field": "PLA_ASO",
        "right_table": "ARC_PLA",
        "right_field": "COD_PLA",
        "confidence": "alta",
        "note": "Plan asignado al socio.",
    },
    {
        "left_table": "ARC_ASO",
        "left_field": "COB_ASO",
        "right_table": "ARC_COB",
        "right_field": "COD_COB",
        "confidence": "alta",
        "note": "Cobrador asignado al socio.",
    },
    {
        "left_table": "ARC_ASO",
        "left_field": "COB_ASO",
        "right_table": "ACU_COB",
        "right_field": "COD_COB",
        "confidence": "media",
        "note": "Acumulado o importe adicional asociado al cobrador.",
    },
    {
        "left_table": "ARC_ASO",
        "left_field": "ZON_ASO",
        "right_table": "ARC_ZON",
        "right_field": "COD_ZON",
        "confidence": "alta",
        "note": "Zona del socio.",
    },
    {
        "left_table": "DET_MOV",
        "left_field": "PLA_ASO",
        "right_table": "ARC_PLA",
        "right_field": "COD_PLA",
        "confidence": "alta",
        "note": "El detalle de movimientos replica el plan.",
    },
    {
        "left_table": "DET_MOV",
        "left_field": "COB_ASO",
        "right_table": "ARC_COB",
        "right_field": "COD_COB",
        "confidence": "alta",
        "note": "El detalle de movimientos replica el cobrador.",
    },
    {
        "left_table": "DET_MOV",
        "left_field": "ZON_ASO",
        "right_table": "ARC_ZON",
        "right_field": "COD_ZON",
        "confidence": "alta",
        "note": "El detalle de movimientos replica la zona.",
    },
    {
        "left_table": "PRE_PLA",
        "left_field": "PLA_PRE",
        "right_table": "ARC_PLA",
        "right_field": "COD_PLA",
        "confidence": "alta",
        "note": "Escala o precio por plan.",
    },
    {
        "left_table": "MOV_ASO",
        "left_field": "COD_ASO",
        "right_table": "DET_MOV",
        "right_field": "COD_ASO",
        "confidence": "media",
        "note": "Probable cruce por socio y periodo; puede requerir ademas periodo, sucursal y numero.",
    },
]

INDEXABLE_COLUMNS = {
    "COD_ASO",
    "PLA_ASO",
    "PLA_PRE",
    "COB_ASO",
    "COD_COB",
    "COD_PLA",
    "ZON_ASO",
    "COD_ZON",
    "PER_MOV",
    "NRO_MOV",
    "DOC_ASO",
}

app = Flask(__name__)


def get_dbf_paths() -> list[Path]:
    if not DBF_ROOT.exists():
        return []
    return sorted(
        [path for path in DBF_ROOT.rglob("*") if path.is_file() and path.suffix.lower() == ".dbf"],
        key=lambda item: str(item).lower(),
    )


def get_data_signature() -> str:
    parts = []
    for path in get_dbf_paths():
        stat = path.stat()
        parts.append(f"{path.relative_to(DBF_ROOT).as_posix()}|{stat.st_size}|{int(stat.st_mtime)}")
    return "||".join(parts)


def list_dbf_files() -> list[dict[str, Any]]:
    files = []
    for path in get_dbf_paths():
        stat = path.stat()
        files.append(
            {
                "name": path.name,
                "relative_path": path.relative_to(DBF_ROOT).as_posix(),
                "directory": path.parent.relative_to(DBF_ROOT).as_posix() or ".",
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime),
            }
        )
    return files


def resolve_dbf_path(relative_path: str) -> Path:
    safe_path = (DBF_ROOT / relative_path).resolve()
    if DBF_ROOT not in safe_path.parents and safe_path != DBF_ROOT:
        abort(404)
    if safe_path.suffix.lower() != ".dbf" or not safe_path.is_file():
        abort(404)
    return safe_path


def open_table(path: Path) -> DBF:
    return DBF(
        str(path),
        load=False,
        encoding=DEFAULT_ENCODING,
        char_decode_errors="ignore",
        ignore_missing_memofile=True,
    )


def format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode(DEFAULT_ENCODING, errors="ignore")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def normalize_sql_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode(DEFAULT_ENCODING, errors="ignore")
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bool):
        return int(value)
    return value


def normalize_export_value(value: Any) -> Any:
    normalized = normalize_sql_value(value)
    if isinstance(normalized, str):
        return normalized.strip()
    return normalized


def human_size(size_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size_bytes} B"


def summarize_sample_preview(sample_preview: dict[str, str]) -> str:
    if not sample_preview:
        return "Sin muestra util todavia."

    fragments = []
    for key, value in sample_preview.items():
        clean_value = value.strip()
        if not clean_value:
            continue
        if len(clean_value) > 28:
            clean_value = f"{clean_value[:25]}..."
        fragments.append(f"{key}={clean_value}")
        if len(fragments) >= 3:
            break

    return ", ".join(fragments) if fragments else "Sin muestra util todavia."


def safe_sql_identifier(name: str) -> str:
    sanitized = re.sub(r"[^0-9A-Za-z_]+", "_", name.upper()).strip("_")
    if not sanitized:
        sanitized = "TABLE"
    if sanitized[0].isdigit():
        sanitized = f"T_{sanitized}"
    return sanitized


def get_table_export_presets(table_name: str) -> list[dict[str, Any]]:
    return TABLE_EXPORT_PRESETS.get(table_name, [])


def get_table_export_preset(table_name: str, export_key: str) -> dict[str, Any] | None:
    for preset in get_table_export_presets(table_name):
        if preset["key"] == export_key:
            return preset
    return None


def record_matches_export_filters(
    record: Any,
    required_not_blank: list[str] | None = None,
    skip_if_all_blank: list[str] | None = None,
    included_values: dict[str, list[Any]] | None = None,
    excluded_values: dict[str, list[Any]] | None = None,
) -> bool:
    required_not_blank = required_not_blank or []
    skip_if_all_blank = skip_if_all_blank or []
    included_values = included_values or {}
    excluded_values = excluded_values or {}

    if required_not_blank:
        required_values = [normalize_export_value(record[field_name]) for field_name in required_not_blank]
        if any(value in (None, "") for value in required_values):
            return False

    for field_name, allowed_values in included_values.items():
        value = normalize_export_value(record[field_name])
        normalized_allowed_values = {normalize_export_value(item) for item in allowed_values}
        if value not in normalized_allowed_values:
            return False

    for field_name, blocked_values in excluded_values.items():
        value = normalize_export_value(record[field_name])
        normalized_blocked_values = {normalize_export_value(item) for item in blocked_values}
        if value in normalized_blocked_values:
            return False

    if skip_if_all_blank:
        raw_values = [normalize_export_value(record[field_name]) for field_name in skip_if_all_blank]
        if all(value in (None, "") for value in raw_values):
            return False

    return True


def map_export_record(
    record: Any,
    field_map: dict[str, str],
    static_values: dict[str, Any] | None = None,
    computed_values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item = {target_key: normalize_export_value(record[source_field]) for source_field, target_key in field_map.items()}
    item.update(build_computed_export_values(record, computed_values or {}))
    item.update(static_values or {})
    return item


def build_computed_export_values(record: Any, computed_values: dict[str, Any]) -> dict[str, Any]:
    resolved_values = {}

    for target_key, config in computed_values.items():
        operator = config.get("operator")
        source_field = config.get("source_field")
        source_value = normalize_export_value(record[source_field]) if source_field else None

        if operator == "is_blank":
            resolved_values[target_key] = config.get("true_value") if source_value in (None, "") else config.get(
                "false_value"
            )
            continue

        if operator == "is_not_blank":
            resolved_values[target_key] = config.get("true_value") if source_value not in (None, "") else config.get(
                "false_value"
            )
            continue

        if operator == "str_slice":
            s = str(source_value) if source_value is not None else ""
            sliced = s[config.get("start", 0):config.get("end")]
            if config.get("cast") == "int":
                try:
                    resolved_values[target_key] = int(sliced)
                except (ValueError, TypeError):
                    resolved_values[target_key] = None
            else:
                resolved_values[target_key] = sliced
            continue

        if operator == "concat":
            source_fields = config.get("source_fields", [])
            separator = config.get("separator", "")
            parts = [str(normalize_export_value(record[f])) for f in source_fields if record.get(f) is not None]
            resolved_values[target_key] = separator.join(parts)
            continue

        if operator == "equals_value":
            resolved_values[target_key] = (
                config.get("true_value") if source_value == config.get("compare_to") else config.get("false_value")
            )
            continue

        if "value" in config:
            resolved_values[target_key] = config["value"]

    return resolved_values


def build_flat_export_payload(table: DBF, preset: dict[str, Any]) -> list[dict[str, Any]]:
    payload = []
    field_map = preset.get("field_map", {})
    static_values = preset.get("static_values", {})
    computed_values = preset.get("computed_values", {})
    required_not_blank = preset.get("required_not_blank", [])
    skip_if_all_blank = preset.get("skip_if_all_blank", [])
    included_values = preset.get("included_values", {})
    excluded_values = preset.get("excluded_values", {})

    for record in table:
        if not record_matches_export_filters(
            record,
            required_not_blank,
            skip_if_all_blank,
            included_values,
            excluded_values,
        ):
            continue

        payload.append(map_export_record(record, field_map, static_values, computed_values))

    return payload


def build_grouped_export_payload(table: DBF, preset: dict[str, Any]) -> list[dict[str, Any]]:
    payload = []
    groups: dict[Any, dict[str, Any]] = {}
    pending_items: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    group_by = preset["group_by"]
    items_key = preset.get("items_key", "items")
    header_field_map = preset.get("header_field_map", {})
    header_static_values = preset.get("header_static_values", {})
    header_computed_values = preset.get("header_computed_values", {})
    header_required_not_blank = preset.get("header_required_not_blank", [])
    header_skip_if_all_blank = preset.get("header_skip_if_all_blank", [])
    header_included_values = preset.get("header_included_values", {})
    header_excluded_values = preset.get("header_excluded_values", {})
    item_field_map = preset.get("item_field_map", {})
    item_static_values = preset.get("item_static_values", {})
    item_computed_values = preset.get("item_computed_values", {})
    item_required_not_blank = preset.get("item_required_not_blank", [])
    item_skip_if_all_blank = preset.get("item_skip_if_all_blank", [])
    item_included_values = preset.get("item_included_values", {})
    item_excluded_values = preset.get("item_excluded_values", {})

    for record in table:
        group_value = normalize_export_value(record[group_by])
        if group_value in (None, ""):
            continue

        if record_matches_export_filters(
            record,
            item_required_not_blank,
            item_skip_if_all_blank,
            item_included_values,
            item_excluded_values,
        ):
            item = map_export_record(record, item_field_map, item_static_values, item_computed_values)
            if group_value in groups:
                groups[group_value][items_key].append(item)
            else:
                pending_items[group_value].append(item)

        if group_value in groups:
            continue

        if not record_matches_export_filters(
            record,
            header_required_not_blank,
            header_skip_if_all_blank,
            header_included_values,
            header_excluded_values,
        ):
            continue

        group_item = map_export_record(record, header_field_map, header_static_values, header_computed_values)
        group_item[items_key] = pending_items.pop(group_value, [])
        groups[group_value] = group_item
        payload.append(group_item)

    return payload


def build_sql_export_payload(preset: dict[str, Any]) -> list[dict[str, Any]]:
    build_sqlite_cache()
    sql_query = preset["sql_query"]
    field_map = preset.get("field_map", {})
    static_values = preset.get("static_values", {})
    computed_values = preset.get("computed_values", {})
    payload = []
    with sqlite3.connect(SQLITE_CACHE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(sql_query):
            record = {k: normalize_export_value(v) for k, v in dict(row).items()}
            item = {target_key: record.get(source_field) for source_field, target_key in field_map.items()}
            item.update(build_computed_export_values(record, computed_values))
            item.update(static_values)
            payload.append(item)
    return payload


def build_export_payload(table: DBF, preset: dict[str, Any]) -> list[dict[str, Any]]:
    if preset.get("sql_query"):
        return build_sql_export_payload(preset)
    if preset.get("group_by"):
        return build_grouped_export_payload(table, preset)
    return build_flat_export_payload(table, preset)


def sqlite_type_for_field(field: Any) -> str:
    if field.type in {"N", "F", "B", "Y"}:
        return "REAL" if field.decimal_count else "INTEGER"
    if field.type == "L":
        return "INTEGER"
    return "TEXT"


def guess_table_role(table_name: str, record_count: int) -> str:
    if table_name.startswith("ARC_"):
        return "maestro" if record_count > 100 else "catalogo"
    if table_name.startswith("MOV_") or table_name.startswith("DET_"):
        return "transaccional"
    if table_name.startswith("PRE_"):
        return "parametrizacion"
    if table_name.startswith("MAS_"):
        return "configuracion"
    if table_name.startswith("BAJ_"):
        return "historial"
    return "indefinido"


@lru_cache(maxsize=4)
def collect_schema(signature: str) -> list[dict[str, Any]]:
    schema = []
    used_sql_names: set[str] = set()

    for path in get_dbf_paths():
        table = open_table(path)
        sql_name = safe_sql_identifier(path.stem)
        suffix = 2
        while sql_name in used_sql_names:
            sql_name = f"{safe_sql_identifier(path.stem)}_{suffix}"
            suffix += 1
        used_sql_names.add(sql_name)

        fields = [
            {
                "name": field.name,
                "type": field.type,
                "length": field.length,
                "decimal_count": field.decimal_count,
            }
            for field in table.fields
        ]

        sample_preview = {}
        try:
            first_record = next(iter(table), None)
            if first_record:
                for key, value in first_record.items():
                    formatted = format_cell(value)
                    if formatted:
                        sample_preview[key] = formatted
                    if len(sample_preview) >= 6:
                        break
        except Exception:
            sample_preview = {}

        schema.append(
            {
                "file_name": path.name,
                "sql_name": sql_name,
                "relative_path": path.relative_to(DBF_ROOT).as_posix(),
                "record_count": len(table),
                "field_count": len(fields),
                "fields": fields,
                "field_names": [field["name"] for field in fields],
                "description": TABLE_HINTS.get(sql_name, "Sin descripcion cargada; revisar columnas y muestra."),
                "role": guess_table_role(sql_name, len(table)),
                "sample_preview": sample_preview,
            }
        )

    return schema


def get_schema() -> list[dict[str, Any]]:
    return collect_schema(get_data_signature())


def get_schema_map() -> dict[str, dict[str, Any]]:
    return {table["sql_name"]: table for table in get_schema()}


def infer_relationships(schema: list[dict[str, Any]]) -> list[dict[str, Any]]:
    schema_map = {table["sql_name"]: table for table in schema}
    relationships = []

    for relation in MANUAL_RELATIONSHIPS:
        left = schema_map.get(relation["left_table"])
        right = schema_map.get(relation["right_table"])
        if not left or not right:
            continue
        if relation["left_field"] not in left["field_names"]:
            continue
        if relation["right_field"] not in right["field_names"]:
            continue
        relationships.append(relation)

    return relationships


def get_analysis_summary() -> dict[str, Any]:
    schema = get_schema()
    return {
        "table_count": len(schema),
        "record_count": sum(table["record_count"] for table in schema),
        "largest_tables": sorted(schema, key=lambda table: table["record_count"], reverse=True)[:5],
        "relationships": infer_relationships(schema),
        "tables": schema,
    }


def enrich_files_with_schema(files: list[dict[str, Any]], schema: list[dict[str, Any]]) -> list[dict[str, Any]]:
    schema_by_path = {table["relative_path"]: table for table in schema}
    enriched = []

    for file in files:
        table = schema_by_path.get(file["relative_path"])
        enriched.append(
            {
                **file,
                "record_count": table["record_count"] if table else None,
                "description": table["description"] if table else "Sin descripcion deducida.",
                "preview_text": summarize_sample_preview(table["sample_preview"]) if table else "",
                "role": table["role"] if table else "",
            }
        )

    return enriched


def validate_sql_query(query: str) -> str | None:
    cleaned = query.strip()
    if not cleaned:
        return "Escribi una consulta SQL."
    if cleaned.rstrip(";").count(";") > 0:
        return "Solo se permite una sentencia por vez."
    if not READ_ONLY_SQL.match(cleaned):
        return "Solo se permiten consultas de solo lectura: SELECT, WITH o EXPLAIN."
    if FORBIDDEN_SQL.search(cleaned):
        return "La consulta contiene instrucciones no permitidas."
    return None


def iter_sql_rows(path: Path) -> Any:
    table = open_table(path)
    for row_number, record in enumerate(table, start=1):
        values = [row_number]
        values.extend(normalize_sql_value(record[field.name]) for field in table.fields)
        yield values


def build_sqlite_cache(force: bool = False) -> dict[str, Any]:
    schema = get_schema()
    signature = get_data_signature()
    cache_exists = SQLITE_CACHE_PATH.exists()

    if cache_exists and not force:
        try:
            with sqlite3.connect(SQLITE_CACHE_PATH) as conn:
                current_signature_row = conn.execute(
                    "SELECT value FROM __meta WHERE key = 'signature'"
                ).fetchone()
            if current_signature_row and current_signature_row[0] == signature:
                return {"db_path": SQLITE_CACHE_PATH, "rebuilt": False}
        except sqlite3.Error:
            pass

    SQLITE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if SQLITE_CACHE_PATH.exists():
        SQLITE_CACHE_PATH.unlink()

    with sqlite3.connect(SQLITE_CACHE_PATH) as conn:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-200000")
        conn.execute("CREATE TABLE __meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")

        for table in schema:
            field_columns = [
                f'"{field["name"]}" {sqlite_type_for_field(type("Field", (), field))}'
                for field in table["fields"]
            ]
            columns_sql = ", ".join(['"__rownum" INTEGER'] + field_columns)
            conn.execute(f'CREATE TABLE "{table["sql_name"]}" ({columns_sql})')

            insert_columns = ", ".join(['"__rownum"'] + [f'"{field["name"]}"' for field in table["fields"]])
            placeholders = ", ".join(["?"] * (len(table["fields"]) + 1))
            batch = []
            path = resolve_dbf_path(table["relative_path"])
            for row in iter_sql_rows(path):
                batch.append(row)
                if len(batch) >= 5000:
                    conn.executemany(
                        f'INSERT INTO "{table["sql_name"]}" ({insert_columns}) VALUES ({placeholders})',
                        batch,
                    )
                    batch.clear()
            if batch:
                conn.executemany(
                    f'INSERT INTO "{table["sql_name"]}" ({insert_columns}) VALUES ({placeholders})',
                    batch,
                )

            for field in table["fields"]:
                if field["name"] in INDEXABLE_COLUMNS:
                    index_name = f'idx_{table["sql_name"]}_{field["name"]}'.lower()
                    conn.execute(
                        f'CREATE INDEX "{index_name}" ON "{table["sql_name"]}" ("{field["name"]}")'
                    )

        conn.executemany(
            "INSERT INTO __meta (key, value) VALUES (?, ?)",
            [
                ("signature", signature),
                ("built_at", datetime.now().isoformat(timespec="seconds")),
                ("table_count", str(len(schema))),
            ],
        )
        conn.commit()

    return {"db_path": SQLITE_CACHE_PATH, "rebuilt": True}


def get_cache_meta() -> dict[str, str]:
    if not SQLITE_CACHE_PATH.exists():
        return {}
    try:
        with sqlite3.connect(SQLITE_CACHE_PATH) as conn:
            rows = conn.execute("SELECT key, value FROM __meta").fetchall()
        return {key: value for key, value in rows}
    except sqlite3.Error:
        return {}


def run_sql_query(query: str, rebuild_cache: bool = False) -> dict[str, Any]:
    cache_info = build_sqlite_cache(force=rebuild_cache)
    started_at = perf_counter()
    with sqlite3.connect(SQLITE_CACHE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(query)
        raw_rows = cursor.fetchmany(SQL_RESULT_LIMIT + 1)
        column_names = list(raw_rows[0].keys()) if raw_rows else [item[0] for item in cursor.description or []]

    elapsed_ms = round((perf_counter() - started_at) * 1000, 1)
    truncated = len(raw_rows) > SQL_RESULT_LIMIT
    rows = [list(row) for row in raw_rows[:SQL_RESULT_LIMIT]]

    return {
        "columns": column_names,
        "rows": rows,
        "row_count": len(rows),
        "truncated": truncated,
        "elapsed_ms": elapsed_ms,
        "cache_info": cache_info,
        "cache_meta": get_cache_meta(),
    }


def get_sql_examples() -> list[dict[str, str]]:
    return [
        {
            "title": "Socios con plan y cobrador",
            "sql": (
                "SELECT a.COD_ASO, a.NOM_ASO, p.NOM_PLA, c.NOM_COB\n"
                "FROM ARC_ASO a\n"
                "LEFT JOIN ARC_PLA p ON p.COD_PLA = a.PLA_ASO\n"
                "LEFT JOIN ARC_COB c ON c.COD_COB = a.COB_ASO\n"
                "ORDER BY a.NOM_ASO\n"
                "LIMIT 100;"
            ),
        },
        {
            "title": "Movimientos por socio",
            "sql": (
                "SELECT m.COD_ASO, a.NOM_ASO, m.PER_MOV, m.NOM_MOV, m.IMP_MOV, m.SAL_MOV\n"
                "FROM MOV_ASO m\n"
                "LEFT JOIN ARC_ASO a ON a.COD_ASO = m.COD_ASO\n"
                "ORDER BY m.PER_MOV DESC, m.COD_ASO\n"
                "LIMIT 100;"
            ),
        },
        {
            "title": "Resumen por plan",
            "sql": (
                "SELECT a.PLA_ASO, p.NOM_PLA, COUNT(*) AS socios\n"
                "FROM ARC_ASO a\n"
                "LEFT JOIN ARC_PLA p ON p.COD_PLA = a.PLA_ASO\n"
                "GROUP BY a.PLA_ASO, p.NOM_PLA\n"
                "ORDER BY socios DESC;"
            ),
        },
        {
            "title": "Socios dados de baja",
            "sql": (
                "SELECT b.COD_ASO, a.NOM_ASO, b.FEC_BAJ\n"
                "FROM BAJ_ASO b\n"
                "LEFT JOIN ARC_ASO a ON a.COD_ASO = b.COD_ASO\n"
                "ORDER BY b.FEC_BAJ DESC;"
            ),
        },
    ]


@app.context_processor
def inject_helpers() -> dict[str, Any]:
    return {"human_size": human_size}


@app.get("/")
def index():
    files = list_dbf_files()
    summary = get_analysis_summary() if files else {"table_count": 0, "record_count": 0, "tables": []}
    files = enrich_files_with_schema(files, summary["tables"]) if files else files
    return render_template(
        "index.html",
        files=files,
        dbf_root=DBF_ROOT,
        dbf_root_exists=DBF_ROOT.exists(),
        summary=summary,
    )


@app.get("/analysis")
def analysis():
    summary = get_analysis_summary()
    return render_template("analysis.html", summary=summary)


@app.route("/sql", methods=["GET", "POST"])
def sql_console():
    schema = get_schema()
    relationships = infer_relationships(schema)
    examples = get_sql_examples()
    query = request.form.get("query", examples[0]["sql"] if examples else "")
    sql_error = None
    result = None
    rebuild_requested = request.form.get("rebuild_cache") == "1"

    if request.method == "POST":
        sql_error = validate_sql_query(query)
        if not sql_error:
            try:
                result = run_sql_query(query, rebuild_cache=rebuild_requested)
            except Exception as exc:
                sql_error = str(exc)

    return render_template(
        "sql.html",
        schema=schema,
        relationships=relationships,
        examples=examples,
        query=query,
        sql_error=sql_error,
        result=result,
        cache_meta=get_cache_meta(),
    )


@app.get("/table/<path:relative_path>")
def table_detail(relative_path: str):
    page = max(1, request.args.get("page", default=1, type=int))
    page_size = request.args.get("page_size", default=DEFAULT_PAGE_SIZE, type=int)
    page_size = min(MAX_PAGE_SIZE, max(1, page_size))

    table_path = resolve_dbf_path(relative_path)
    read_error = None
    fields = []
    rows = []
    total_records = 0
    total_pages = 1
    sql_name = safe_sql_identifier(table_path.stem)
    export_presets = get_table_export_presets(sql_name)

    try:
        table = open_table(table_path)
        total_records = len(table)
        total_pages = max(1, math.ceil(total_records / page_size)) if total_records else 1
        if page > total_pages:
            page = total_pages

        offset = (page - 1) * page_size
        for row_number, record in enumerate(islice(iter(table), offset, offset + page_size), start=offset + 1):
            rows.append(
                {
                    "row_number": row_number,
                    "cells": [format_cell(value) for value in record.values()],
                }
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
    except Exception as exc:
        read_error = str(exc)

    return render_template(
        "table.html",
        table_name=table_path.name,
        sql_name=sql_name,
        relative_path=relative_path,
        fields=fields,
        rows=rows,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        total_records=total_records,
        read_error=read_error,
        export_presets=export_presets,
    )


@app.get("/table/<path:relative_path>/export/<export_key>")
def table_export(relative_path: str, export_key: str):
    table_path = resolve_dbf_path(relative_path)
    table_name = safe_sql_identifier(table_path.stem)
    preset = get_table_export_preset(table_name, export_key)
    if not preset:
        abort(404)

    table = open_table(table_path)
    payload = build_export_payload(table, preset)
    file_name = f"{table_name.lower()}-{preset.get('filename_suffix', export_key)}.json"

    response = app.response_class(
        response=json.dumps(payload, ensure_ascii=False, indent=2),
        mimetype="application/json",
    )
    response.headers["Content-Disposition"] = f'attachment; filename="{file_name}"'
    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=False)
