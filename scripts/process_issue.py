from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INVENTORY_CSV = DATA_DIR / "inventory.csv"
INVENTORY_MERCADO_CSV = DATA_DIR / "inventory_mercado.csv"
SALES_CSV = DATA_DIR / "sales.csv"
SALES_MERCADO_CSV = DATA_DIR / "sales_mercado.csv"
PRODUCTION_CSV = DATA_DIR / "production.csv"
TRANSFER_MERCADO_CSV = DATA_DIR / "transfer_mercado.csv"
PROCESSED_EVENTS_CSV = DATA_DIR / "processed_events.csv"


@dataclass
class ParsedPayload:
    issue_number: str
    issue_title: str
    issue_body: str
    issue_author: str
    labels: list[str]
    raw: dict[str, Any]
    payload_type: str
    fecha: str
    metodo_pago: str
    txn_id: str
    issue_ref: str
    accion: str
    modo: str
    valor: str
    sku: str
    descripcion: str
    razon: str
    detalle: str
    notas: str
    items: list[dict[str, Any]]


def log(msg: str) -> None:
    print(msg, flush=True)


def normalize_payment(value: str) -> str:
    v = str(value or "").strip().lower()
    if v in {"tarjeta", "transferencia", "merma"}:
        return v
    return "efectivo"


def normalize_type(value: str) -> str:
    v = str(value or "").strip().lower().replace(" ", "_").replace("-", "_")
    aliases = {
        "venta": "venta",
        "venta_normal": "venta",
        "venta_mkt": "venta_mkt",
        "venta_mercado": "venta_mkt",
        "prod": "prod",
        "produccion": "prod",
        "abasto_mkt": "abasto_mkt",
        "abasto_mercado": "abasto_mkt",
        "correccion_venta": "correccion_venta",
        "correccion_venta_mkt": "correccion_venta_mkt",
        "ajuste_inv": "ajuste_inv",
        "ajuste_inv_mkt": "ajuste_inv_mkt",
        "merma": "merma",
        "merma_mkt": "merma_mkt",
        "regalada": "merma",
        "regaladas": "merma",
        "regalada_mkt": "merma_mkt",
        "regaladas_mkt": "merma_mkt",
    }
    return aliases.get(v, v)


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip() or default))
    except Exception:
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip() or default)
    except Exception:
        return default


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def csv_read(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [{str(k).strip(): (v if v is not None else "") for k, v in raw.items()} for raw in reader]


def csv_write(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = rows or []
    if fieldnames is None:
        fieldnames = []
        seen: set[str] = set()
        for row in rows:
            for k in row.keys():
                if k not in seen:
                    seen.add(k)
                    fieldnames.append(k)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def append_csv_row(path: Path, row: dict[str, Any], preferred_fieldnames: list[str] | None = None) -> None:
    rows = csv_read(path)
    rows.append({k: str(v) if v is not None else "" for k, v in row.items()})
    csv_write(path, rows, fieldnames=preferred_fieldnames)


def ensure_csv_if_missing(path: Path, fieldnames: list[str]) -> None:
    if not path.exists():
        csv_write(path, [], fieldnames=fieldnames)


def ensure_core_files() -> None:
    ensure_csv_if_missing(INVENTORY_CSV, ["item", "descripcion", "stock", "precio", "product_id"])
    ensure_csv_if_missing(INVENTORY_MERCADO_CSV, ["item", "descripcion", "stock", "precio", "product_id"])
    sales_fields = ["txn_id", "fecha", "item", "cantidad", "precio_unit", "importe", "issue", "metodo_pago", "source_id", "descripcion", "status", "correction_ref", "notas"]
    ensure_csv_if_missing(SALES_CSV, sales_fields)
    ensure_csv_if_missing(SALES_MERCADO_CSV, sales_fields)
    ensure_csv_if_missing(PRODUCTION_CSV, ["fecha", "item", "cantidad", "issue", "source_id", "descripcion"])
    ensure_csv_if_missing(TRANSFER_MERCADO_CSV, ["fecha", "item", "cantidad", "issue", "source_id", "descripcion"])
    ensure_csv_if_missing(PROCESSED_EVENTS_CSV, ["issue_number", "event_hash", "payload_type", "fecha", "status"])


def inventory_find_row(rows: list[dict[str, str]], sku: str) -> dict[str, str] | None:
    sku = str(sku).strip()
    for row in rows:
        if str(row.get("item", "")).strip() == sku:
            return row
    return None


def inventory_get_desc(rows: list[dict[str, str]], sku: str, fallback: str = "") -> str:
    row = inventory_find_row(rows, sku)
    return str(row.get("descripcion", "")).strip() if row else fallback


def inventory_get_price(rows: list[dict[str, str]], sku: str, fallback: float = 0.0) -> float:
    row = inventory_find_row(rows, sku)
    return to_float(row.get("precio", ""), fallback) if row else fallback


def inventory_adjust(path: Path, sku: str, descripcion: str, mode: str, value: int) -> None:
    rows = csv_read(path)
    row = inventory_find_row(rows, sku)
    if row is None:
        row = {"item": sku, "descripcion": descripcion, "stock": "0", "precio": "0", "product_id": ""}
        rows.append(row)
    current_stock = to_int(row.get("stock", 0), 0)
    row["stock"] = str(value if mode == "set" else current_stock + value)
    if descripcion and not str(row.get("descripcion", "")).strip():
        row["descripcion"] = descripcion
    csv_write(path, rows, fieldnames=["item", "descripcion", "stock", "precio", "product_id"])


def inventory_move_between(normal_sku: str, descripcion: str, qty: int) -> None:
    if qty <= 0:
        raise ValueError("La cantidad para abasto mercado debe ser mayor a 0")
    inventory_adjust(INVENTORY_CSV, normal_sku, descripcion, "delta", -qty)
    inventory_adjust(INVENTORY_MERCADO_CSV, normal_sku, descripcion, "delta", qty)


def parse_issue_event() -> dict[str, Any]:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        raise RuntimeError("Falta GITHUB_EVENT_PATH")
    with open(event_path, "r", encoding="utf-8") as f:
        return json.load(f)


def strip_conflict_lines(text: str) -> str:
    out = []
    for line in str(text or "").splitlines():
        if line.startswith(("<<<<<<<", "=======", ">>>>>>>")) or line.strip() in {"Updated upstream", "Stashed changes"}:
            continue
        out.append(line)
    return "\n".join(out).strip()


def extract_multiline_sections(body: str) -> dict[str, str]:
    body = strip_conflict_lines(body)
    keys = {"detalle": [], "razon": [], "items_raw": [], "notas": []}
    current: str | None = None

    for raw in body.splitlines():
        line = raw.rstrip()
        low = line.strip().lower()

        if re.match(r"^detalle\s*:?\s*$", low):
            current = "detalle"
            continue
        if re.match(r"^razon\s*:?\s*$", low):
            current = "razon"
            continue
        if re.match(r"^(items|productos)\s*:?\s*$", low):
            current = "items_raw"
            continue
        if re.match(r"^notas\s*:?\s*$", low):
            current = "notas"
            continue
        if re.match(r"^[a-záéíóúüñ_ ]+\s*:\s*$", low) and current:
            current = None
        if current:
            keys[current].append(line)

    return {k: "\n".join(v).strip() for k, v in keys.items()}


def extract_simple_fields(body: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for raw in strip_conflict_lines(body).splitlines():
        line = raw.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        k = key.strip().lower().replace(" ", "_")
        if k in {"tipo", "type", "fecha", "txn_id", "client_txn_id", "issue_ref", "accion", "metodo_pago", "modo", "valor", "sku", "descripcion", "notas", "items"}:
            fields[k] = value.strip()
    return fields


def parse_markdown_table_items(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or "|" not in line:
            continue
        if line.startswith("|"):
            line = line[1:]
        if line.endswith("|"):
            line = line[:-1]
        if not line or ("---" in line and "|" in line):
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 2:
            continue
        sku = parts[0]
        if not sku or sku.lower() in {"sku", "item", "producto"}:
            continue
        rows.append({
            "item": sku,
            "cantidad": to_int(parts[1], 0),
            "precio": to_float(parts[2], 0.0) if len(parts) >= 3 else 0.0,
            "descripcion": parts[3] if len(parts) >= 4 else "",
        })
    return [r for r in rows if r["item"] and to_int(r["cantidad"], 0) != 0]


def parse_json_body(body: str) -> dict[str, Any] | None:
    text = strip_conflict_lines(body).strip()
    if not text.startswith("{") or not text.endswith("}"):
        return None
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def parse_items_from_any(body: str, sections: dict[str, str], json_body: dict[str, Any] | None, fields: dict[str, str]) -> list[dict[str, Any]]:
    if json_body and isinstance(json_body.get("items"), list):
        out = []
        for item in json_body["items"]:
            if isinstance(item, dict):
                out.append({
                    "item": str(item.get("item", "")).strip(),
                    "cantidad": to_int(item.get("cantidad", 0), 0),
                    "precio": to_float(item.get("precio", 0), 0.0),
                    "descripcion": str(item.get("descripcion", "")).strip(),
                })
        out = [r for r in out if r["item"] and to_int(r["cantidad"], 0) != 0]
        if out:
            return out

    raw_items = str(fields.get("items", "")).strip()
    if raw_items:
        try:
            parsed = json.loads(raw_items)
            if isinstance(parsed, list):
                out = []
                for item in parsed:
                    if isinstance(item, dict):
                        out.append({
                            "item": str(item.get("item", "")).strip(),
                            "cantidad": to_int(item.get("cantidad", 0), 0),
                            "precio": to_float(item.get("precio", 0), 0.0),
                            "descripcion": str(item.get("descripcion", "")).strip(),
                        })
                out = [r for r in out if r["item"] and to_int(r["cantidad"], 0) != 0]
                if out:
                    return out
        except Exception:
            pass

    # extra robusto: buscar en TODAS las líneas del body con pipes, no solo en la sección Items.
    rows = parse_markdown_table_items(strip_conflict_lines(body))
    if rows:
        return rows

    source = sections.get("items_raw", "") or sections.get("detalle", "")
    rows = parse_markdown_table_items(source)
    if rows:
        return rows

    parsed: list[dict[str, Any]] = []
    for raw in source.splitlines():
        line = raw.strip()
        if not line:
            continue
        m = re.match(r"^(?P<sku>[A-Z0-9\-_]+)\s+[,;]?\s*(?P<qty>-?\d+)(?:\s+[,;]?\s*(?P<price>-?\d+(?:\.\d+)?))?$", line, re.IGNORECASE)
        if m:
            parsed.append({"item": m.group("sku").strip(), "cantidad": to_int(m.group("qty"), 0), "precio": to_float(m.group("price") or 0, 0.0), "descripcion": ""})
    return [r for r in parsed if r["item"] and to_int(r["cantidad"], 0) != 0]


def guess_type_from_title(title: str) -> str:
    t = str(title or "").strip().lower().replace(" ", "_").replace("-", "_")
    if "venta_mkt" in t or "venta_mercado" in t:
        return "venta_mkt"
    if "abasto_mkt" in t or "abasto_mercado" in t:
        return "abasto_mkt"
    if "produccion" in t or "prod" in t:
        return "prod"
    if "correccion_venta_mkt" in t:
        return "correccion_venta_mkt"
    if "correccion_venta" in t:
        return "correccion_venta"
    if "ajuste_inv_mkt" in t:
        return "ajuste_inv_mkt"
    if "ajuste_inv" in t:
        return "ajuste_inv"
    if "merma_mkt" in t:
        return "merma_mkt"
    if "merma" in t or "regalada" in t or "regaladas" in t:
        return "merma"
    if "venta" in t:
        return "venta"
    return ""


def guess_type_from_labels(labels: list[str]) -> str:
    normalized = [normalize_type(x) for x in labels]
    for p in ["correccion_venta_mkt", "correccion_venta", "ajuste_inv_mkt", "ajuste_inv", "abasto_mkt", "venta_mkt", "merma_mkt", "merma", "prod", "venta"]:
        if p in normalized:
            return p
    return ""


def parse_issue_payload(event: dict[str, Any]) -> ParsedPayload:
    issue = event.get("issue", {}) or {}
    title = str(issue.get("title", "")).strip()
    body = str(issue.get("body", "")).strip()
    author = str((issue.get("user") or {}).get("login", "")).strip()
    number = str(issue.get("number", "")).strip()
    labels = [str((x or {}).get("name", "")).strip() for x in (issue.get("labels") or [])]

    json_body = parse_json_body(body)
    sections = extract_multiline_sections(body)
    fields = extract_simple_fields(body)
    payload_type = normalize_type((json_body or {}).get("type") or fields.get("type") or fields.get("tipo") or guess_type_from_title(title) or guess_type_from_labels(labels))
    items = parse_items_from_any(body, sections, json_body, fields)

    fecha = str((json_body or {}).get("fecha", "")).strip() or fields.get("fecha", "").strip() or ""
    metodo_pago = normalize_payment(str((json_body or {}).get("metodo_pago", "")).strip() or fields.get("metodo_pago", "").strip())
    txn_id = str((json_body or {}).get("txn_id", "")).strip() or str((json_body or {}).get("client_txn_id", "")).strip() or fields.get("txn_id", "").strip() or fields.get("client_txn_id", "").strip()
    issue_ref = str((json_body or {}).get("issue_ref", "")).strip() or fields.get("issue_ref", "").strip()
    accion = str((json_body or {}).get("accion", "")).strip().lower() or fields.get("accion", "").strip().lower()
    modo = str((json_body or {}).get("modo", "")).strip().lower() or fields.get("modo", "").strip().lower()
    valor = str((json_body or {}).get("valor", "")).strip() or fields.get("valor", "").strip()
    sku = str((json_body or {}).get("sku", "")).strip() or fields.get("sku", "").strip()
    descripcion = str((json_body or {}).get("descripcion", "")).strip() or fields.get("descripcion", "").strip()
    razon = sections.get("razon", "") or str((json_body or {}).get("razon", "")).strip()
    detalle = sections.get("detalle", "") or str((json_body or {}).get("detalle", "")).strip()
    notas = sections.get("notas", "") or str((json_body or {}).get("notas", "")).strip() or fields.get("notas", "").strip()

    if not txn_id and payload_type in {"venta", "venta_mkt", "merma", "merma_mkt"}:
        txn_id = f"txn-issue-{number}"

    return ParsedPayload(number, title, body, author, labels, event, payload_type, fecha, metodo_pago, txn_id, issue_ref, accion, modo, valor, sku, descripcion, razon, detalle, notas, items)


def processed_event_exists(issue_number: str, event_hash: str) -> bool:
    for row in csv_read(PROCESSED_EVENTS_CSV):
        if str(row.get("issue_number", "")).strip() == issue_number and str(row.get("event_hash", "")).strip() == event_hash:
            return True
    return False


def mark_processed(issue_number: str, event_hash: str, payload_type: str, fecha: str, status: str) -> None:
    append_csv_row(PROCESSED_EVENTS_CSV, {"issue_number": issue_number, "event_hash": event_hash, "payload_type": payload_type, "fecha": fecha, "status": status}, ["issue_number", "event_hash", "payload_type", "fecha", "status"])


def add_github_comment(issue_number: str, body: str) -> None:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if not token or not repo or not issue_number:
        return
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/issues/{issue_number}/comments",
        data=json.dumps({"body": body}).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "Content-Type": "application/json", "User-Agent": "inventory-bot"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=20).read()
    except Exception as exc:
        log(f"No se pudo comentar en issue #{issue_number}: {exc}")


def close_github_issue(issue_number: str) -> None:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if not token or not repo or not issue_number:
        return
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/issues/{issue_number}",
        data=json.dumps({"state": "closed"}).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "Content-Type": "application/json", "User-Agent": "inventory-bot"},
        method="PATCH",
    )
    try:
        urllib.request.urlopen(req, timeout=20).read()
    except Exception as exc:
        log(f"No se pudo cerrar issue #{issue_number}: {exc}")


def resolve_items_with_inventory(items: list[dict[str, Any]], inventory_path: Path, zero_price: bool = False) -> list[dict[str, Any]]:
    inventory_rows = csv_read(inventory_path)
    resolved = []
    for item in items:
        sku = str(item.get("item", "")).strip()
        qty = to_int(item.get("cantidad", 0), 0)
        if not sku or qty == 0:
            continue
        desc = str(item.get("descripcion", "")).strip() or inventory_get_desc(inventory_rows, sku, "")
        price = 0.0 if zero_price else to_float(item.get("precio", 0), 0.0)
        if not zero_price and price == 0:
            price = inventory_get_price(inventory_rows, sku, 0.0)
        resolved.append({"item": sku, "cantidad": qty, "precio": price, "descripcion": desc})
    return resolved


def register_sale(payload: ParsedPayload, mercado: bool, zero_price: bool = False) -> str:
    inventory_path = INVENTORY_MERCADO_CSV if mercado else INVENTORY_CSV
    sales_path = SALES_MERCADO_CSV if mercado else SALES_CSV
    txn_id = payload.txn_id or f"txn-issue-{payload.issue_number}"
    items = resolve_items_with_inventory(payload.items, inventory_path, zero_price=zero_price)
    if not items:
        raise ValueError("La operación no trae items válidos")

    for item in items:
        qty = to_int(item["cantidad"], 0)
        price = 0.0 if zero_price else to_float(item["precio"], 0.0)
        inventory_adjust(inventory_path, item["item"], item["descripcion"], "delta", -qty)
        append_csv_row(
            sales_path,
            {
                "txn_id": txn_id, "fecha": payload.fecha, "item": item["item"], "cantidad": qty,
                "precio_unit": f"{price:.2f}", "importe": f"{qty * price:.2f}",
                "issue": payload.issue_number, "metodo_pago": normalize_payment(payload.metodo_pago),
                "source_id": payload.issue_number, "descripcion": item["descripcion"],
                "status": "activa", "correction_ref": "", "notas": payload.notas,
            },
            ["txn_id", "fecha", "item", "cantidad", "precio_unit", "importe", "issue", "metodo_pago", "source_id", "descripcion", "status", "correction_ref", "notas"],
        )
    return txn_id


def register_production(payload: ParsedPayload) -> None:
    items = resolve_items_with_inventory(payload.items, INVENTORY_CSV)
    if not items:
        raise ValueError("Producción sin items válidos")
    for item in items:
        qty = to_int(item["cantidad"], 0)
        if qty <= 0:
            continue
        inventory_adjust(INVENTORY_CSV, item["item"], item["descripcion"], "delta", qty)
        append_csv_row(PRODUCTION_CSV, {"fecha": payload.fecha, "item": item["item"], "cantidad": qty, "issue": payload.issue_number, "source_id": payload.issue_number, "descripcion": item["descripcion"]}, ["fecha", "item", "cantidad", "issue", "source_id", "descripcion"])


def register_abasto_mercado(payload: ParsedPayload) -> None:
    items = resolve_items_with_inventory(payload.items, INVENTORY_CSV)
    if not items:
        raise ValueError("Abasto mercado sin items válidos")
    for item in items:
        qty = to_int(item["cantidad"], 0)
        if qty <= 0:
            continue
        inventory_move_between(item["item"], item["descripcion"], qty)
        append_csv_row(TRANSFER_MERCADO_CSV, {"fecha": payload.fecha, "item": item["item"], "cantidad": qty, "issue": payload.issue_number, "source_id": payload.issue_number, "descripcion": item["descripcion"]}, ["fecha", "item", "cantidad", "issue", "source_id", "descripcion"])


def parse_correction_detail_lines(text: str) -> list[dict[str, Any]]:
    rows = parse_markdown_table_items(text)
    return rows


def find_sale_rows(sales_path: Path, txn_id: str, issue_ref: str) -> tuple[list[dict[str, str]], list[int]]:
    rows = csv_read(sales_path)
    matches, indexes = [], []
    for idx, row in enumerate(rows):
        status = str(row.get("status", "activa")).strip().lower() or "activa"
        if status in {"cancelada", "corregida"}:
            continue
        if txn_id and str(row.get("txn_id", "")).strip() == txn_id:
            matches.append(row); indexes.append(idx)
        elif issue_ref and str(row.get("issue", "")).strip() == issue_ref:
            matches.append(row); indexes.append(idx)
    return matches, indexes


def mark_sale_rows_status(sales_path: Path, indexes: list[int], status: str, correction_ref: str) -> None:
    rows = csv_read(sales_path)
    for idx in indexes:
        if 0 <= idx < len(rows):
            rows[idx]["status"] = status
            rows[idx]["correction_ref"] = correction_ref
    csv_write(sales_path, rows, ["txn_id", "fecha", "item", "cantidad", "precio_unit", "importe", "issue", "metodo_pago", "source_id", "descripcion", "status", "correction_ref", "notas"])


def revert_sale_rows_to_inventory(matches: list[dict[str, str]], inventory_path: Path) -> None:
    for row in matches:
        qty = to_int(row.get("cantidad", 0), 0)
        if qty > 0:
            inventory_adjust(inventory_path, str(row.get("item", "")).strip(), str(row.get("descripcion", "")).strip(), "delta", qty)


def append_corrected_sale(sales_path: Path, inventory_path: Path, payload: ParsedPayload, base_matches: list[dict[str, str]]) -> str:
    fecha = payload.fecha or (base_matches[0].get("fecha", "") if base_matches else "")
    metodo = normalize_payment(payload.metodo_pago or (base_matches[0].get("metodo_pago", "") if base_matches else ""))
    new_txn = f"corr-{payload.issue_number}"
    items = resolve_items_with_inventory(parse_correction_detail_lines(payload.detalle), inventory_path)
    if not items:
        raise ValueError("La corrección no trae detalle válido")
    for item in items:
        qty = to_int(item["cantidad"], 0)
        if qty <= 0:
            continue
        price = to_float(item["precio"], 0.0)
        inventory_adjust(inventory_path, item["item"], item["descripcion"], "delta", -qty)
        append_csv_row(sales_path, {
            "txn_id": new_txn, "fecha": fecha, "item": item["item"], "cantidad": qty,
            "precio_unit": f"{price:.2f}", "importe": f"{qty * price:.2f}",
            "issue": payload.issue_number, "metodo_pago": metodo, "source_id": payload.issue_number,
            "descripcion": item["descripcion"], "status": "activa", "correction_ref": "", "notas": payload.razon,
        }, ["txn_id", "fecha", "item", "cantidad", "precio_unit", "importe", "issue", "metodo_pago", "source_id", "descripcion", "status", "correction_ref", "notas"])
    return new_txn


def handle_sale_correction(payload: ParsedPayload, mercado: bool) -> str:
    inventory_path = INVENTORY_MERCADO_CSV if mercado else INVENTORY_CSV
    sales_path = SALES_MERCADO_CSV if mercado else SALES_CSV
    matches, indexes = find_sale_rows(sales_path, payload.txn_id, payload.issue_ref)
    if not matches:
        raise ValueError("No encontré la venta a corregir")
    revert_sale_rows_to_inventory(matches, inventory_path)
    action = str(payload.accion or "").strip().lower()
    if action == "cancelar":
        mark_sale_rows_status(sales_path, indexes, "cancelada", payload.issue_number)
        return "cancelada"
    if action == "ajustar":
        mark_sale_rows_status(sales_path, indexes, "corregida", payload.issue_number)
        return append_corrected_sale(sales_path, inventory_path, payload, matches)
    raise ValueError("Acción de corrección no válida. Usa cancelar o ajustar")


def handle_inventory_adjust(payload: ParsedPayload, mercado: bool) -> None:
    inventory_path = INVENTORY_MERCADO_CSV if mercado else INVENTORY_CSV
    mode = str(payload.modo or "delta").strip().lower()
    if mode not in {"delta", "set"}:
        raise ValueError("Modo inválido, usa delta o set")
    if not str(payload.sku or "").strip():
        raise ValueError("Falta sku para ajuste de inventario")
    inventory_adjust(inventory_path, str(payload.sku).strip(), str(payload.descripcion or "").strip(), mode, to_int(payload.valor, 0))


def build_event_hash(payload: ParsedPayload) -> str:
    return sha1_text(json.dumps({
        "type": payload.payload_type, "fecha": payload.fecha, "metodo_pago": payload.metodo_pago, "txn_id": payload.txn_id,
        "issue_ref": payload.issue_ref, "accion": payload.accion, "modo": payload.modo, "valor": payload.valor,
        "sku": payload.sku, "descripcion": payload.descripcion, "razon": payload.razon, "detalle": payload.detalle,
        "notas": payload.notas, "items": payload.items, "body": payload.issue_body,
    }, ensure_ascii=False, sort_keys=True))


def process_payload(payload: ParsedPayload) -> str:
    ptype = payload.payload_type
    if not ptype:
        raise ValueError("No pude detectar el tipo de operación")
    if ptype == "venta":
        return f"Venta normal registrada. txn={register_sale(payload, mercado=False)}"
    if ptype == "venta_mkt":
        return f"Venta mercado registrada. txn={register_sale(payload, mercado=True)}"
    if ptype == "merma":
        return f"Merma/regaladas normal registrada. txn={register_sale(payload, mercado=False, zero_price=True)}"
    if ptype == "merma_mkt":
        return f"Merma/regaladas mercado registrada. txn={register_sale(payload, mercado=True, zero_price=True)}"
    if ptype == "prod":
        register_production(payload); return "Producción aplicada a inventory.csv"
    if ptype == "abasto_mkt":
        register_abasto_mercado(payload); return "Abasto mercado aplicado (normal -> mercado)"
    if ptype == "ajuste_inv":
        handle_inventory_adjust(payload, mercado=False); return "Ajuste de inventario normal aplicado"
    if ptype == "ajuste_inv_mkt":
        handle_inventory_adjust(payload, mercado=True); return "Ajuste de inventario mercado aplicado"
    if ptype == "correccion_venta":
        return f"Corrección de venta normal aplicada: {handle_sale_correction(payload, mercado=False)}"
    if ptype == "correccion_venta_mkt":
        return f"Corrección de venta mercado aplicada: {handle_sale_correction(payload, mercado=True)}"
    raise ValueError(f"Tipo no soportado: {ptype}")


def main() -> None:
    ensure_core_files()
    payload = parse_issue_payload(parse_issue_event())
    if not payload.payload_type:
        log("No hay tipo de operación en este evento. Se omite sin error.")
        return

    event_hash = build_event_hash(payload)
    if processed_event_exists(payload.issue_number, event_hash):
        log(f"Issue #{payload.issue_number} ya estaba procesado. No hago nada.")
        return

    try:
        result = process_payload(payload)
        mark_processed(payload.issue_number, event_hash, payload.payload_type, payload.fecha, "ok")
        log(result)
        add_github_comment(payload.issue_number, f"✅ Procesado correctamente.\n\n{result}")
        close_github_issue(payload.issue_number)
    except Exception as exc:
        err = f"❌ Error procesando issue #{payload.issue_number}: {exc}"
        log(err)
        mark_processed(payload.issue_number, event_hash, payload.payload_type, payload.fecha, "error")
        add_github_comment(payload.issue_number, err)
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        if "No pude detectar el tipo de operación" in str(exc):
            log(f"Sin operación para procesar: {exc}")
            sys.exit(0)
        log(f"Fallo fatal: {exc}")
        sys.exit(1)
