#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from build_reports import build_reports
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from build_reports import build_reports

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DOCS = ROOT / "docs"
DIARIO_DIR = DOCS / "diario"
MKT_DIR = DOCS / "mercado"
MKT_DIARIO_DIR = MKT_DIR / "diario"

INVENTORY_CSV = DATA / "inventory.csv"
SALES_CSV = DATA / "sales.csv"
PROD_CSV = DATA / "production.csv"

INVENTORY_MKT_CSV = DATA / "inventory_mercado.csv"
SALES_MKT_CSV = DATA / "sales_mercado.csv"
TRANSFER_MKT_CSV = DATA / "transfer_mercado.csv"

PROCESSED_EVENTS_CSV = DATA / "processed_events.csv"

MENU_JSON = DOCS / "menu.json"
REPORT_JSON = DOCS / "report.json"
INV_OUT_CSV = DOCS / "inventario_actual.csv"
INV_MKT_OUT_CSV = MKT_DIR / "inventario_actual.csv"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def ensure_files() -> None:
    ensure_dir(DATA)
    ensure_dir(DOCS)
    ensure_dir(DIARIO_DIR)
    ensure_dir(MKT_DIR)
    ensure_dir(MKT_DIARIO_DIR)

    if not INVENTORY_CSV.exists():
        INVENTORY_CSV.write_text("item,descripcion,stock,precio,product_id\n", encoding="utf-8")

    if not SALES_CSV.exists():
        SALES_CSV.write_text(
            "txn_id,fecha,item,cantidad,precio_unit,importe,issue,metodo_pago,source_id\n",
            encoding="utf-8"
        )

    if not PROD_CSV.exists():
        PROD_CSV.write_text(
            "txn_id,fecha,item,cantidad,issue,source_id\n",
            encoding="utf-8"
        )

    if not INVENTORY_MKT_CSV.exists():
        INVENTORY_MKT_CSV.write_text("item,descripcion,stock,precio,product_id\n", encoding="utf-8")

    if not SALES_MKT_CSV.exists():
        SALES_MKT_CSV.write_text(
            "txn_id,fecha,item,cantidad,precio_unit,importe,issue,metodo_pago,source_id\n",
            encoding="utf-8"
        )

    if not TRANSFER_MKT_CSV.exists():
        TRANSFER_MKT_CSV.write_text(
            "txn_id,fecha,item,cantidad,issue,source_id\n",
            encoding="utf-8"
        )

    if not PROCESSED_EVENTS_CSV.exists():
        PROCESSED_EVENTS_CSV.write_text(
            "event_key,issue_url,issue_number,type,processed_at\n",
            encoding="utf-8"
        )


def grab_field(body: str, key: str) -> str:
    pattern = rf"^\s*(?:\*\*\s*{re.escape(key)}\s*\*\*|{re.escape(key)})\s*:\s*(.*)$"
    match = re.search(pattern, body or "", re.IGNORECASE | re.MULTILINE)
    return match.group(1).strip() if match else ""


def safe_parse_date(value: str, issue: dict | None = None) -> str:
    raw = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except Exception:
            pass

    created = (issue or {}).get("created_at", "")
    if created:
        return str(created)[:10]

    return datetime.now(timezone.utc).date().isoformat()


def normalize_payment(value: str) -> str:
    txt = str(value or "").strip().lower()
    if txt in {"tarjeta", "transferencia"}:
        return txt
    return "efectivo"


def extract_client_txn_id(body: str) -> str:
    field_value = grab_field(body, "ClientTxnId")
    if field_value:
        return field_value.strip()

    match = re.search(r"VENTAS-TXN:([A-Za-z0-9:_-]+)", body or "")
    return match.group(1).strip() if match else ""


def parse_items_table(body: str, require_price: bool) -> list[dict]:
    lines = (body or "").splitlines()
    start_idx = None

    for idx, line in enumerate(lines):
        normalized = re.sub(r"[*\s]", "", line).lower()
        if normalized == "items":
            start_idx = idx + 1
            break

    if start_idx is None:
        return []

    items: list[dict] = []
    for line in lines[start_idx:]:
        stripped = line.strip()
        if not stripped:
            continue
        if "|" not in stripped:
            continue
        if stripped.lower().startswith("sku |"):
            continue
        if stripped.startswith("---"):
            continue

        parts = [part.strip() for part in stripped.split("|")]
        if len(parts) < 2:
            continue

        sku = parts[0].strip()
        qty_raw = parts[1].strip()
        price_raw = parts[2].strip() if len(parts) >= 3 else ""

        if not sku:
            continue

        qty = pd.to_numeric(qty_raw, errors="coerce")
        if pd.isna(qty) or int(qty) <= 0:
            continue

        item = {"item": sku, "cantidad": int(qty)}
        if require_price:
            item["precio_unit"] = price_raw
        items.append(item)

    return items


def parse_single_item(body: str, require_price: bool) -> list[dict]:
    sku = grab_field(body, "Item")
    qty_raw = grab_field(body, "Cantidad")
    price_raw = (
        grab_field(body, "Precio unitario (opcional)")
        or grab_field(body, "Precio unitario")
        or grab_field(body, "Precio")
    )

    qty = pd.to_numeric(qty_raw, errors="coerce")
    if not sku or pd.isna(qty) or int(qty) <= 0:
        return []

    item = {"item": sku.strip(), "cantidad": int(qty)}
    if require_price:
        item["precio_unit"] = price_raw.strip()
    return [item]


def _clean_items(items: list[dict], require_price: bool) -> list[dict]:
    clean: list[dict] = []

    for raw in items or []:
        sku = str(raw.get("item", "")).strip()
        qty = pd.to_numeric(raw.get("cantidad"), errors="coerce")
        if not sku or pd.isna(qty) or int(qty) <= 0:
            continue

        item = {"item": sku, "cantidad": int(qty)}
        if require_price:
            price_value = raw.get("precio_unit", "")
            item["precio_unit"] = "" if price_value is None else str(price_value).strip()

        clean.append(item)

    return clean


def load_event_issue() -> dict | None:
    path = os.environ.get("GITHUB_EVENT_PATH")
    if not path or not os.path.exists(path):
        return None

    with open(path, "r", encoding="utf-8") as fh:
        event = json.load(fh)

    return event.get("issue")


def _read_csv_safe(path: Path, columns: list[str]) -> pd.DataFrame:
    if path.exists() and path.stat().st_size > 0:
        df = pd.read_csv(path, dtype=str).fillna("")
    else:
        df = pd.DataFrame(columns=columns)

    for col in columns:
        if col not in df.columns:
            df[col] = ""

    return df[columns].copy()


def load_inventory(path: Path) -> pd.DataFrame:
    columns = ["item", "descripcion", "stock", "precio", "product_id"]
    df = _read_csv_safe(path, columns)

    df["item"] = df["item"].astype(str).str.strip()
    df["descripcion"] = df["descripcion"].astype(str)
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
    df["precio"] = pd.to_numeric(df["precio"], errors="coerce")
    df["product_id"] = df["product_id"].astype(str)

    df = df[df["item"] != ""].copy()
    df = df.drop_duplicates(subset=["item"], keep="last").reset_index(drop=True)
    return df


def save_inventory(df: pd.DataFrame, path: Path) -> None:
    export = df.copy()
    export["stock"] = pd.to_numeric(export["stock"], errors="coerce").fillna(0).astype(int)
    export["precio"] = pd.to_numeric(export["precio"], errors="coerce")
    export = export[["item", "descripcion", "stock", "precio", "product_id"]].sort_values("item")
    export.to_csv(path, index=False)


def load_inventory_general() -> pd.DataFrame:
    return load_inventory(INVENTORY_CSV)


def load_inventory_mkt() -> pd.DataFrame:
    return load_inventory(INVENTORY_MKT_CSV)


def make_event_key(data: dict) -> str:
    client_txn_id = str(data.get("client_txn_id", "") or "").strip()
    if client_txn_id:
        return f"client:{client_txn_id}"

    issue_url = str(data.get("issue_url", "") or "").strip()
    return f"issue:{issue_url}" if issue_url else ""


def load_processed_events() -> pd.DataFrame:
    return _read_csv_safe(
        PROCESSED_EVENTS_CSV,
        ["event_key", "issue_url", "issue_number", "type", "processed_at"]
    )


def is_event_processed(event_key: str) -> bool:
    if not event_key:
        return False

    df = load_processed_events()
    return not df[df["event_key"].astype(str) == str(event_key)].empty


def mark_event_processed(event_key: str, data: dict) -> None:
    if not event_key:
        return

    df = load_processed_events()
    df = df[df["event_key"].astype(str) != str(event_key)].copy()

    row = pd.DataFrame([{
        "event_key": event_key,
        "issue_url": str(data.get("issue_url", "") or ""),
        "issue_number": str(data.get("issue_number", "") or ""),
        "type": str(data.get("type", "") or ""),
        "processed_at": _now_iso()
    }])

    df = pd.concat([df, row], ignore_index=True)
    df.to_csv(PROCESSED_EVENTS_CSV, index=False)


def next_txn_id(prefix: str, *paths: Path) -> str:
    max_n = 0
    for path in paths:
        if not path.exists() or path.stat().st_size == 0:
            continue
        try:
            df = pd.read_csv(path, dtype=str).fillna("")
        except Exception:
            continue
        if "txn_id" not in df.columns:
            continue
        for value in df["txn_id"].astype(str).tolist():
            match = re.match(rf"^{re.escape(prefix)}(\d+)$", value.strip())
            if match:
                max_n = max(max_n, int(match.group(1)))
    return f"{prefix}{max_n + 1:06d}"


def parse_freeform_field(body: str, key: str) -> str:
    lines = (body or "").splitlines()
    start = None
    for idx, line in enumerate(lines):
        if re.match(rf"^\s*{re.escape(key)}\s*:\s*$", line, re.IGNORECASE):
            start = idx + 1
            break
    if start is None:
        return ""
    chunks: list[str] = []
    for line in lines[start:]:
        if re.match(r"^\s*[A-Za-z_][A-Za-z0-9_ ]*\s*:\s*", line):
            break
        chunks.append(line.rstrip())
    return "\n".join(chunks).strip()


def infer_issue_type(title: str, labels: list[str], body: str) -> str:
    lower_title = str(title or "").lower()
    lower_body = str(body or "").lower()
    labels_set = {str(x or "").strip().lower() for x in labels}

    explicit_type = (
        grab_field(body, "tipo")
        or grab_field(body, "type")
        or ""
    ).strip().lower()
    aliases = {
        "venta": "venta",
        "venta_mkt": "venta_mkt",
        "venta-mercado": "venta_mkt",
        "venta mercado": "venta_mkt",
        "abasto_mkt": "abasto_mkt",
        "abasto-mercado": "abasto_mkt",
        "abasto mercado": "abasto_mkt",
        "prod": "prod",
        "produccion": "prod",
        "producción": "prod",
        "ajuste_inv": "ajuste_inv",
        "ajuste inv": "ajuste_inv",
        "ajuste-inv": "ajuste_inv",
        "ajuste_inv_mkt": "ajuste_inv_mkt",
        "ajuste inv mkt": "ajuste_inv_mkt",
        "ajuste_inv_mercado": "ajuste_inv_mkt",
        "ajuste-inv-mkt": "ajuste_inv_mkt",
        "correccion_venta": "correccion_venta",
        "corrección_venta": "correccion_venta",
        "correccion venta": "correccion_venta",
        "corrección venta": "correccion_venta",
        "correccion_venta_mkt": "correccion_venta_mkt",
        "corrección_venta_mkt": "correccion_venta_mkt",
        "correccion venta mkt": "correccion_venta_mkt",
        "corrección venta mkt": "correccion_venta_mkt",
        "correccion_venta_mercado": "correccion_venta_mkt",
        "corrección_venta_mercado": "correccion_venta_mkt",
    }
    if explicit_type in aliases:
        return aliases[explicit_type]

    if "venta-mercado" in labels_set or "venta mercado" in lower_title:
        return "venta_mkt"
    if "abasto-mercado" in labels_set or "abasto mercado" in lower_title:
        return "abasto_mkt"
    if "produccion" in labels_set or "producción" in lower_title or "produccion" in lower_title:
        return "prod"
    if "venta" in labels_set or lower_title.startswith("venta"):
        return "venta"
    if "ajuste-inventario" in labels_set or "ajuste inventario" in lower_title or "ajuste_inv" in lower_body:
        return "ajuste_inv"
    if "ajuste-inventario,mercado" in labels_set or "ajuste inventario mercado" in lower_title or "ajuste_inv_mkt" in lower_body:
        return "ajuste_inv_mkt"
    if "correccion-venta,mercado" in labels_set or "correccion venta mercado" in lower_title or "correccion_venta_mkt" in lower_body:
        return "correccion_venta_mkt"
    if "correccion-venta" in labels_set or "correccion venta" in lower_title or "correccion_venta" in lower_body:
        return "correccion_venta"
    return "none"


def parse_issue(issue: dict) -> dict:
    title = str((issue or {}).get("title", "") or "")
    body = str((issue or {}).get("body", "") or "")
    labels = [str(x.get("name", "")).strip().lower() for x in (issue or {}).get("labels", [])]

    issue_type = infer_issue_type(title, labels, body)
    require_price = issue_type in {"venta", "venta_mkt"}

    fecha = safe_parse_date(grab_field(body, "Fecha"), issue)
    notas = grab_field(body, "Notas")
    metodo_pago = normalize_payment(
        grab_field(body, "Método de pago")
        or grab_field(body, "Metodo de pago")
        or grab_field(body, "metodo_pago")
    )
    client_txn_id = extract_client_txn_id(body)

    items = parse_items_table(body, require_price=require_price)
    if not items:
        items = parse_single_item(body, require_price=require_price)

    detalle_text = parse_freeform_field(body, "detalle")
    razon_text = parse_freeform_field(body, "razon")
    accion = (grab_field(body, "accion") or "").strip().lower()
    modo = (grab_field(body, "modo") or "").strip().lower()
    sku = (grab_field(body, "sku") or grab_field(body, "item") or "").strip()
    descripcion = (grab_field(body, "descripcion") or "").strip()
    valor = (grab_field(body, "valor") or "").strip()
    issue_ref = (grab_field(body, "issue_ref") or grab_field(body, "issue") or "").strip()
    txn_ref = (grab_field(body, "txn_id") or grab_field(body, "txn") or client_txn_id).strip()

    base = {
        "type": issue_type,
        "fecha": fecha,
        "items": items,
        "issue_url": (issue or {}).get("html_url", ""),
        "issue_number": (issue or {}).get("number", ""),
        "labels": labels,
        "notas": notas,
        "metodo_pago": metodo_pago,
        "client_txn_id": client_txn_id,
        "accion": accion,
        "modo": modo,
        "sku": sku,
        "descripcion": descripcion,
        "valor": valor,
        "issue_ref": issue_ref,
        "txn_ref": txn_ref,
        "detalle": detalle_text,
        "razon": razon_text,
    }
    return base


def find_inventory_row(inv: pd.DataFrame, sku: str) -> pd.Series | None:
    row = inv[inv["item"].astype(str) == str(sku)]
    if row.empty:
        return None
    return row.iloc[0]


def apply_stock(inv: pd.DataFrame, items: list[dict], sign: int, path: Path) -> pd.DataFrame:
    inv = inv.copy()

    for raw in _clean_items(items, require_price=False):
        sku = raw["item"]
        qty = int(raw["cantidad"])

        mask = inv["item"].astype(str) == str(sku)
        if mask.any():
            inv.loc[mask, "stock"] = pd.to_numeric(inv.loc[mask, "stock"], errors="coerce").fillna(0).astype(int) + (sign * qty)
        else:
            inv = pd.concat([
                inv,
                pd.DataFrame([{
                    "item": sku,
                    "descripcion": "",
                    "stock": sign * qty,
                    "precio": pd.NA,
                    "product_id": ""
                }])
            ], ignore_index=True)

    inv["stock"] = pd.to_numeric(inv["stock"], errors="coerce").fillna(0).astype(int)
    save_inventory(inv, path)
    return inv


def _upsert_rows(path: Path, rows: list[dict], key: str) -> None:
    if not rows:
        return

    incoming = pd.DataFrame(rows)
    if path.exists() and path.stat().st_size > 0:
        current = pd.read_csv(path, dtype=str).fillna("")
    else:
        current = pd.DataFrame(columns=incoming.columns.tolist())

    for col in incoming.columns:
        if col not in current.columns:
            current[col] = ""

    if key not in current.columns:
        current[key] = ""

    current = current[current[key].astype(str).isin(incoming[key].astype(str)) == False].copy()
    merged = pd.concat([current, incoming], ignore_index=True)
    merged.to_csv(path, index=False)


def append_sales_general(
    inv: pd.DataFrame,
    fecha: str,
    items: list[dict],
    issue_url: str,
    metodo_pago: str,
    txn_id: str,
    source_base: str
) -> None:
    clean = _clean_items(items, require_price=True)
    rows = []

    for idx, it in enumerate(clean):
        sku = it["item"]
        qty = int(it["cantidad"])
        price_text = str(it.get("precio_unit", "")).strip()
        price_num = pd.to_numeric(price_text, errors="coerce")

        if pd.isna(price_num):
            row = find_inventory_row(inv, sku)
            price_num = float(row["precio"]) if row is not None and pd.notna(row["precio"]) else 0.0

        importe = float(qty) * float(price_num)
        rows.append({
            "txn_id": txn_id,
            "fecha": fecha,
            "item": sku,
            "cantidad": qty,
            "precio_unit": f"{float(price_num):.2f}",
            "importe": f"{importe:.2f}",
            "issue": issue_url,
            "metodo_pago": metodo_pago or "efectivo",
            "source_id": f"{source_base}#{idx}"
        })

    _upsert_rows(SALES_CSV, rows, key="source_id")


def append_sales_mkt(
    inv: pd.DataFrame,
    fecha: str,
    items: list[dict],
    issue_url: str,
    metodo_pago: str,
    txn_id: str,
    source_base: str
) -> None:
    clean = _clean_items(items, require_price=True)
    rows = []

    for idx, it in enumerate(clean):
        sku = it["item"]
        qty = int(it["cantidad"])
        price_text = str(it.get("precio_unit", "")).strip()
        price_num = pd.to_numeric(price_text, errors="coerce")

        if pd.isna(price_num):
            row = find_inventory_row(inv, sku)
            price_num = float(row["precio"]) if row is not None and pd.notna(row["precio"]) else 0.0

        importe = float(qty) * float(price_num)
        rows.append({
            "txn_id": txn_id,
            "fecha": fecha,
            "item": sku,
            "cantidad": qty,
            "precio_unit": f"{float(price_num):.2f}",
            "importe": f"{importe:.2f}",
            "issue": issue_url,
            "metodo_pago": metodo_pago or "efectivo",
            "source_id": f"{source_base}#{idx}"
        })

    _upsert_rows(SALES_MKT_CSV, rows, key="source_id")


def append_production(fecha: str, items: list[dict], issue_url: str, txn_id: str, source_base: str) -> None:
    clean = _clean_items(items, require_price=False)
    rows = []

    for idx, it in enumerate(clean):
        rows.append({
            "txn_id": txn_id,
            "fecha": fecha,
            "item": it["item"],
            "cantidad": int(it["cantidad"]),
            "issue": issue_url,
            "source_id": f"{source_base}#{idx}"
        })

    _upsert_rows(PROD_CSV, rows, key="source_id")


def append_transfer_mkt(fecha: str, items: list[dict], issue_url: str, txn_id: str, source_base: str) -> None:
    clean = _clean_items(items, require_price=False)
    rows = []

    for idx, it in enumerate(clean):
        rows.append({
            "txn_id": txn_id,
            "fecha": fecha,
            "item": it["item"],
            "cantidad": int(it["cantidad"]),
            "issue": issue_url,
            "source_id": f"{source_base}#{idx}"
        })

    _upsert_rows(TRANSFER_MKT_CSV, rows, key="source_id")




def parse_adjustment_items(text: str) -> list[dict]:
    rows: list[dict] = []
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line or "|" not in line:
            continue
        if line.lower().startswith("sku |"):
            continue
        if line.startswith("---"):
            continue
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 2:
            continue
        sku = parts[0]
        qty = pd.to_numeric(parts[1], errors="coerce")
        if not sku or pd.isna(qty) or int(qty) <= 0:
            continue
        price_raw = parts[2] if len(parts) >= 3 else ""
        item = {"item": sku, "cantidad": int(qty), "precio_unit": price_raw}
        rows.append(item)
    return rows


def load_sales(path: Path) -> pd.DataFrame:
    columns = ["txn_id", "fecha", "item", "cantidad", "precio_unit", "importe", "issue", "metodo_pago", "source_id"]
    df = _read_csv_safe(path, columns)
    df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)
    df["precio_unit"] = pd.to_numeric(df["precio_unit"], errors="coerce").fillna(0.0)
    df["importe"] = pd.to_numeric(df["importe"], errors="coerce").fillna(0.0)
    df["metodo_pago"] = df["metodo_pago"].astype(str).replace("", "efectivo")
    return df


def save_sales(df: pd.DataFrame, path: Path) -> None:
    export = df.copy()
    for col in ["txn_id", "fecha", "item", "issue", "metodo_pago", "source_id"]:
        if col not in export.columns:
            export[col] = ""
    export["cantidad"] = pd.to_numeric(export["cantidad"], errors="coerce").fillna(0).astype(int)
    export["precio_unit"] = pd.to_numeric(export["precio_unit"], errors="coerce").fillna(0.0)
    export["importe"] = pd.to_numeric(export["importe"], errors="coerce").fillna(0.0)
    export = export[["txn_id", "fecha", "item", "cantidad", "precio_unit", "importe", "issue", "metodo_pago", "source_id"]]
    export.to_csv(path, index=False)


def resolve_sales_mask(df: pd.DataFrame, txn_ref: str, issue_ref: str) -> pd.Series:
    mask = pd.Series(False, index=df.index)
    txn_ref = str(txn_ref or "").strip()
    issue_ref = str(issue_ref or "").strip()
    if txn_ref:
        mask = mask | (df["txn_id"].astype(str).str.strip() == txn_ref)
    if issue_ref:
        issue_series = df["issue"].astype(str)
        mask = mask | issue_series.str.strip().eq(issue_ref) | issue_series.str.contains(fr"/{re.escape(issue_ref)}$", regex=True)
    return mask


def apply_inventory_adjustment(inv: pd.DataFrame, sku: str, descripcion: str, modo: str, valor: str, path: Path) -> pd.DataFrame:
    inv = inv.copy()
    sku = str(sku or "").strip()
    if not sku:
        raise ValueError("Falta SKU para ajuste de inventario")

    value_num = pd.to_numeric(valor, errors="coerce")
    if pd.isna(value_num):
        raise ValueError("Valor de ajuste inválido")

    mask = inv["item"].astype(str) == sku
    if not mask.any():
        inv = pd.concat([
            inv,
            pd.DataFrame([{
                "item": sku,
                "descripcion": str(descripcion or ""),
                "stock": 0,
                "precio": pd.NA,
                "product_id": ""
            }])
        ], ignore_index=True)
        mask = inv["item"].astype(str) == sku

    if modo == "set":
        inv.loc[mask, "stock"] = int(value_num)
    else:
        inv.loc[mask, "stock"] = pd.to_numeric(inv.loc[mask, "stock"], errors="coerce").fillna(0).astype(int) + int(value_num)

    if str(descripcion or "").strip():
        inv.loc[mask, "descripcion"] = str(descripcion).strip()

    save_inventory(inv, path)
    return inv


def apply_sale_correction(
    sales_path: Path,
    inventory_path: Path,
    fecha: str,
    txn_ref: str,
    issue_ref: str,
    accion: str,
    detalle: str,
    metodo_pago: str,
    issue_url: str,
    source_base: str,
    txn_prefix: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    sales = load_sales(sales_path)
    inv = load_inventory(inventory_path)
    mask = resolve_sales_mask(sales, txn_ref, issue_ref)
    target = sales[mask].copy()

    if target.empty:
        raise ValueError("No encontré la venta a corregir")

    original_items = [{"item": row["item"], "cantidad": int(row["cantidad"])} for _, row in target.iterrows()]
    inv = apply_stock(inv, original_items, sign=+1, path=inventory_path)
    sales = sales[~mask].copy()

    accion = str(accion or "").strip().lower()
    if accion == "cancelar":
        save_sales(sales, sales_path)
        return inv, sales

    new_items = parse_adjustment_items(detalle)
    if not new_items:
        raise ValueError("No encontré detalle válido para la corrección")

    txn_id = next_txn_id(txn_prefix, sales_path)
    payment = normalize_payment(metodo_pago or str(target["metodo_pago"].iloc[0] if not target.empty else "efectivo"))

    rows = []
    for idx, it in enumerate(_clean_items(new_items, require_price=True)):
        sku = it["item"]
        qty = int(it["cantidad"])
        price_text = str(it.get("precio_unit", "")).strip()
        price_num = pd.to_numeric(price_text, errors="coerce")
        if pd.isna(price_num):
            row = find_inventory_row(inv, sku)
            price_num = float(row["precio"]) if row is not None and pd.notna(row["precio"]) else 0.0
        importe = float(qty) * float(price_num)
        rows.append({
            "txn_id": txn_id,
            "fecha": fecha,
            "item": sku,
            "cantidad": qty,
            "precio_unit": f"{float(price_num):.2f}",
            "importe": f"{importe:.2f}",
            "issue": issue_url,
            "metodo_pago": payment,
            "source_id": f"{source_base}#corr#{idx}"
        })

    sales = pd.concat([sales, pd.DataFrame(rows)], ignore_index=True)
    save_sales(sales, sales_path)
    inv = apply_stock(inv, new_items, sign=-1, path=inventory_path)
    return inv, sales

def build_sales_detail(sales_path: Path, inv: pd.DataFrame) -> pd.DataFrame:
    columns = ["txn_id", "fecha", "item", "cantidad", "precio_unit", "importe", "issue", "metodo_pago", "source_id"]
    sales = _read_csv_safe(sales_path, columns)

    if sales.empty:
        return pd.DataFrame(columns=[
            "txn_id", "fecha", "item", "cantidad", "precio_unit", "importe",
            "issue", "metodo_pago", "payment", "source_id", "descripcion", "product_id"
        ])

    sales["cantidad"] = pd.to_numeric(sales["cantidad"], errors="coerce").fillna(0).astype(int)
    sales["precio_unit"] = pd.to_numeric(sales["precio_unit"], errors="coerce")
    sales["importe"] = pd.to_numeric(sales["importe"], errors="coerce")
    sales["metodo_pago"] = sales["metodo_pago"].astype(str).replace("", "efectivo")

    lookup = inv[["item", "descripcion", "product_id"]].drop_duplicates(subset=["item"], keep="last")
    detail = sales.merge(lookup, how="left", on="item")
    detail["payment"] = detail["metodo_pago"]
    return detail


def build_prod_detail(prod_path: Path, inv: pd.DataFrame) -> pd.DataFrame:
    columns = ["txn_id", "fecha", "item", "cantidad", "issue", "source_id"]
    prod = _read_csv_safe(prod_path, columns)
    if prod.empty:
        return pd.DataFrame(columns=["txn_id", "fecha", "item", "cantidad", "issue", "source_id", "descripcion", "product_id"])

    prod["cantidad"] = pd.to_numeric(prod["cantidad"], errors="coerce").fillna(0).astype(int)
    lookup = inv[["item", "descripcion", "product_id"]].drop_duplicates(subset=["item"], keep="last")
    return prod.merge(lookup, how="left", on="item")


def write_menu_json(inv: pd.DataFrame) -> None:
    menu_df = inv.copy()
    menu_df["precio"] = pd.to_numeric(menu_df["precio"], errors="coerce")
    menu_df["stock"] = pd.to_numeric(menu_df["stock"], errors="coerce").fillna(0).astype(int)
    menu_df = menu_df[menu_df["item"].astype(str) != ""].copy()
    menu_df = menu_df.sort_values(["item"]).reset_index(drop=True)

    menu = []
    for _, row in menu_df.iterrows():
        price = None if pd.isna(row["precio"]) else float(row["precio"])
        menu.append({
            "product_id": str(row.get("product_id", "") or ""),
            "item": str(row["item"]),
            "descripcion": str(row.get("descripcion", "") or ""),
            "precio": price if price is not None else "",
            "stock": int(row.get("stock", 0) or 0)
        })

    MENU_JSON.write_text(json.dumps(menu, ensure_ascii=False, indent=2), encoding="utf-8")


def write_inventory_exports(inv_gen: pd.DataFrame, inv_mkt: pd.DataFrame) -> None:
    inv_gen.sort_values("item").to_csv(INV_OUT_CSV, index=False)
    inv_mkt.sort_values("item").to_csv(INV_MKT_OUT_CSV, index=False)

    report = {
        "generated_at": _now_iso(),
        "general_items": int(len(inv_gen)),
        "general_stock_total": int(pd.to_numeric(inv_gen["stock"], errors="coerce").fillna(0).sum()) if not inv_gen.empty else 0,
        "mercado_items": int(len(inv_mkt)),
        "mercado_stock_total": int(pd.to_numeric(inv_mkt["stock"], errors="coerce").fillna(0).sum()) if not inv_mkt.empty else 0,
    }
    REPORT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def build_all_reports(inv_gen: pd.DataFrame, inv_mkt: pd.DataFrame) -> None:
    write_menu_json(inv_gen)
    write_inventory_exports(inv_gen, inv_mkt)

    general_sales_detail = build_sales_detail(SALES_CSV, inv_gen)
    general_prod_detail = build_prod_detail(PROD_CSV, inv_gen)
    build_reports(inv_gen, inv_mkt, general_sales_detail, general_prod_detail, out_dir=DOCS)

    market_sales_detail = build_sales_detail(SALES_MKT_CSV, inv_mkt)
    empty_prod = pd.DataFrame(columns=["fecha", "item", "cantidad", "descripcion", "product_id"])
    build_reports(inv_gen, inv_mkt, market_sales_detail, empty_prod, out_dir=MKT_DIR)


def source_base_from_data(data: dict) -> str:
    return make_event_key(data)


def main() -> None:
    ensure_files()

    inv_gen = load_inventory_general()
    inv_mkt = load_inventory_mkt()

    issue = load_event_issue()

    if issue is None:
        build_all_reports(inv_gen, inv_mkt)
        return

    data = parse_issue(issue)
    event_key = make_event_key(data)
    movement_type = data.get("type", "none")

    if movement_type != "none" and is_event_processed(event_key):
        build_all_reports(inv_gen, inv_mkt)
        return

    if movement_type == "venta_mkt":
        txn_id = next_txn_id("SM", SALES_MKT_CSV)
        source_base = source_base_from_data(data)

        append_sales_mkt(
            inv_mkt,
            data["fecha"],
            data["items"],
            data["issue_url"],
            data.get("metodo_pago", "efectivo"),
            txn_id,
            source_base
        )
        inv_mkt = apply_stock(inv_mkt, data["items"], sign=-1, path=INVENTORY_MKT_CSV)
        mark_event_processed(event_key, data)

    elif movement_type == "abasto_mkt":
        txn_id = next_txn_id("TM", TRANSFER_MKT_CSV)
        source_base = source_base_from_data(data)

        inv_gen = apply_stock(inv_gen, data["items"], sign=-1, path=INVENTORY_CSV)
        inv_mkt = apply_stock(inv_mkt, data["items"], sign=+1, path=INVENTORY_MKT_CSV)
        append_transfer_mkt(data["fecha"], data["items"], data["issue_url"], txn_id, source_base)
        mark_event_processed(event_key, data)

    elif movement_type == "venta":
        txn_id = next_txn_id("S", SALES_CSV)
        source_base = source_base_from_data(data)

        append_sales_general(
            inv_gen,
            data["fecha"],
            data["items"],
            data["issue_url"],
            data.get("metodo_pago", "efectivo"),
            txn_id,
            source_base
        )
        inv_gen = apply_stock(inv_gen, data["items"], sign=-1, path=INVENTORY_CSV)
        mark_event_processed(event_key, data)

    elif movement_type == "prod":
        txn_id = next_txn_id("P", PROD_CSV)
        source_base = source_base_from_data(data)

        append_production(data["fecha"], data["items"], data["issue_url"], txn_id, source_base)
        inv_gen = apply_stock(inv_gen, data["items"], sign=+1, path=INVENTORY_CSV)
        mark_event_processed(event_key, data)

    elif movement_type == "ajuste_inv":
        inv_gen = apply_inventory_adjustment(
            inv_gen,
            data.get("sku", ""),
            data.get("descripcion", ""),
            data.get("modo", "delta"),
            data.get("valor", ""),
            INVENTORY_CSV,
        )
        mark_event_processed(event_key, data)

    elif movement_type == "ajuste_inv_mkt":
        inv_mkt = apply_inventory_adjustment(
            inv_mkt,
            data.get("sku", ""),
            data.get("descripcion", ""),
            data.get("modo", "delta"),
            data.get("valor", ""),
            INVENTORY_MKT_CSV,
        )
        mark_event_processed(event_key, data)

    elif movement_type == "correccion_venta":
        source_base = source_base_from_data(data)
        inv_gen, _ = apply_sale_correction(
            SALES_CSV,
            INVENTORY_CSV,
            data["fecha"],
            data.get("txn_ref", ""),
            data.get("issue_ref", ""),
            data.get("accion", ""),
            data.get("detalle", ""),
            data.get("metodo_pago", ""),
            data["issue_url"],
            source_base,
            "S",
        )
        mark_event_processed(event_key, data)

    elif movement_type == "correccion_venta_mkt":
        source_base = source_base_from_data(data)
        inv_mkt, _ = apply_sale_correction(
            SALES_MKT_CSV,
            INVENTORY_MKT_CSV,
            data["fecha"],
            data.get("txn_ref", ""),
            data.get("issue_ref", ""),
            data.get("accion", ""),
            data.get("detalle", ""),
            data.get("metodo_pago", ""),
            data["issue_url"],
            source_base,
            "SM",
        )
        mark_event_processed(event_key, data)

    build_all_reports(inv_gen, inv_mkt)


if __name__ == "__main__":
    main()
