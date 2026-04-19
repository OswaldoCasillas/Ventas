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
    return "tarjeta" if txt == "tarjeta" else "efectivo"


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


def parse_issue(issue: dict) -> dict:
    title = str((issue or {}).get("title", "") or "")
    body = str((issue or {}).get("body", "") or "")
    labels = [str(x.get("name", "")).strip().lower() for x in (issue or {}).get("labels", [])]

    lower_title = title.lower()
    issue_type = "none"
    require_price = False

    if "venta-mercado" in labels or "venta mercado" in lower_title:
        issue_type = "venta_mkt"
        require_price = True
    elif "abasto-mercado" in labels or "abasto mercado" in lower_title:
        issue_type = "abasto_mkt"
    elif "produccion" in labels or "producción" in lower_title or "produccion" in lower_title:
        issue_type = "prod"
    elif "venta" in labels or lower_title.startswith("venta"):
        issue_type = "venta"
        require_price = True

    fecha = safe_parse_date(grab_field(body, "Fecha"), issue)
    notas = grab_field(body, "Notas")
    metodo_pago = normalize_payment(grab_field(body, "Método de pago") or grab_field(body, "Metodo de pago"))
    client_txn_id = extract_client_txn_id(body)

    items = parse_items_table(body, require_price=require_price)
    if not items:
        items = parse_single_item(body, require_price=require_price)

    base = {
        "type": issue_type,
        "fecha": fecha,
        "items": items,
        "issue_url": (issue or {}).get("html_url", ""),
        "issue_number": (issue or {}).get("number", ""),
        "labels": labels,
        "notas": notas,
        "metodo_pago": metodo_pago,
        "client_txn_id": client_txn_id
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

    build_all_reports(inv_gen, inv_mkt)


if __name__ == "__main__":
    main()
