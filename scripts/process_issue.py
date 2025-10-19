#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json, os, re, secrets, hashlib
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DOCS = ROOT / "docs"
DIARIO_DIR = DOCS / "diario"
MKT_DIR = DOCS / "mercado"
MKT_DIARIO_DIR = MKT_DIR / "diario"

# --- archivos (general) ---
INVENTORY_CSV = DATA / "inventory.csv"
SALES_CSV     = DATA / "sales.csv"
PROD_CSV      = DATA / "production.csv"

# --- archivos (mercado) ---
INVENTORY_MKT_CSV = DATA / "inventory_mercado.csv"
SALES_MKT_CSV     = DATA / "sales_mercado.csv"
TRANSFER_MKT_CSV  = DATA / "transfer_mercado.csv"

# --- outputs (general) ---
MENU_JSON        = DOCS / "menu.json"
REPORT_JSON      = DOCS / "report.json"
INV_OUT_CSV      = DOCS / "inventario_actual.csv"
SALES_ITEM_CSV   = DOCS / "ventas_por_item.csv"
SALES_DAY_CSV    = DOCS / "ventas_por_dia.csv"
SALES_DETAIL_CSV = DOCS / "ventas_detalle.csv"
PROD_DETAIL_CSV  = DOCS / "produccion_detalle.csv"
REPORT_HTML      = DOCS / "reporte.html"

# --- outputs (mercado) ---
INV_MKT_OUT_CSV      = MKT_DIR / "inventario_actual.csv"
SALES_MKT_DETAIL_CSV = MKT_DIR / "ventas_detalle.csv"

MX_TZ = ZoneInfo("America/Mexico_City")

# ====================== utilidades ======================

def ensure_files():
    DATA.mkdir(parents=True, exist_ok=True)
    DOCS.mkdir(parents=True, exist_ok=True)
    DIARIO_DIR.mkdir(parents=True, exist_ok=True)
    MKT_DIR.mkdir(parents=True, exist_ok=True)
    MKT_DIARIO_DIR.mkdir(parents=True, exist_ok=True)

    if not INVENTORY_CSV.exists():
        INVENTORY_CSV.write_text("item,descripcion,stock,precio,product_id\n", encoding="utf-8")
    if not SALES_CSV.exists():
        SALES_CSV.write_text("txn_id,fecha,item,cantidad,precio_unit,importe,issue,metodo_pago,source_id,descripcion\n", encoding="utf-8")
    if not PROD_CSV.exists():
        PROD_CSV.write_text("txn_id,fecha,item,cantidad,issue,source_id,descripcion\n", encoding="utf-8")

    if not INVENTORY_MKT_CSV.exists():
        INVENTORY_MKT_CSV.write_text("item,descripcion,stock,precio,product_id\n", encoding="utf-8")
    if not SALES_MKT_CSV.exists():
        SALES_MKT_CSV.write_text("txn_id,fecha,item,cantidad,precio_unit,importe,issue,metodo_pago,source_id,descripcion\n", encoding="utf-8")
    if not TRANSFER_MKT_CSV.exists():
        TRANSFER_MKT_CSV.write_text("txn_id,fecha,item,cantidad,issue,source_id,descripcion\n", encoding="utf-8")

def load_event_issue():
    path = os.environ.get("GITHUB_EVENT_PATH")
    if not path or not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        evt = json.load(fh)
    return evt.get("issue")

def grab_field(body: str, key: str) -> str:
    # Acepta "**Fecha**: ..." o "Fecha: ..."
    pat = rf"^\s*(?:\*\*\s*{re.escape(key)}\s*\*\*|{re.escape(key)})\s*:\s*(.*)$"
    m = re.search(pat, body, re.IGNORECASE | re.MULTILINE)
    return (m.group(1) if m else "").strip()

def safe_parse_date(s: str, issue: dict) -> str:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    created = (issue or {}).get("created_at") or ""
    if created and len(created) >= 10:
        # created_at viene en UTC -> conviértelo a MX solo para fecha
        try:
            dt_utc = datetime.fromisoformat(created.replace("Z","+00:00"))
            dt_mx  = dt_utc.astimezone(MX_TZ)
            return dt_mx.date().isoformat()
        except Exception:
            return created[:10]
    return datetime.now(MX_TZ).date().isoformat()

# ---------- Validación/parseo robusto de Items ----------

def is_valid_sku(s: str) -> bool:
    if not s or not isinstance(s, str):
        return False
    s = s.strip()
    if s.startswith("**"):           # evita "**Cantidad**:" y similares
        return False
    if " " in s:                     # no espacios
        return False
    return bool(re.match(r"^[A-Z0-9\-:]{3,}$", s))

def parse_items_section(body: str) -> str | None:
    lines = body.splitlines()
    for i, ln in enumerate(lines):
        if re.match(r"^\s*(\*\*\s*)?items(\s*\*\*)?\s*$", ln, re.IGNORECASE):
            return "\n".join(lines[i+1:])
    return None

def parse_items_table(body: str, has_price: bool):
    """
    Lee tabla tipo:
      SKU | Cantidad [| Precio]
    """
    section = parse_items_section(body)
    if not section:
        return []
    out = []
    for raw in section.splitlines():
        ln = raw.strip()
        if not ln or "|" not in ln:
            if out:
                break
            else:
                continue
        parts = [p.strip() for p in ln.split("|")]
        # ignora encabezados/separadores
        head = "|".join(p.lower() for p in parts[:3])
        if ("sku" in head and "cantidad" in head) or ln.startswith("---"):
            continue
        try:
            if has_price:
                if len(parts) < 3: continue
                sku, qty, price = parts[0], parts[1], parts[2]
                if not is_valid_sku(sku): continue
                qty_i = int(str(qty).strip())
                if qty_i <= 0: continue
                out.append({"item": sku, "cantidad": qty_i, "precio_unit": str(price).strip()})
            else:
                if len(parts) < 2: continue
                sku, qty = parts[0], parts[1]
                if not is_valid_sku(sku): continue
                qty_i = int(str(qty).strip())
                if qty_i <= 0: continue
                out.append({"item": sku, "cantidad": qty_i})
        except Exception:
            continue
    return out

def short_id_from_sku(sku: str) -> str:
    return hashlib.sha1(sku.encode("utf-8")).hexdigest()[:8].upper()

def new_txn_id(prefix: str) -> str:
    now = datetime.now(MX_TZ).strftime("%Y%m%d-%H%M%S")
    rand = secrets.token_hex(3).upper()
    return f"{prefix}-{now}-{rand}"

# ====================== inventario / menú ======================

def _load_inventory_file(path: Path) -> pd.DataFrame:
    if path.exists() and path.stat().st_size > 0:
        inv = pd.read_csv(path, dtype=str)
    else:
        inv = pd.DataFrame(columns=["item","descripcion","stock","precio","product_id"])
    # sanea columnas
    for col in ["item","descripcion","stock","precio","product_id"]:
        if col not in inv.columns:
            inv[col] = ""
    inv["precio"] = pd.to_numeric(inv["precio"], errors="coerce")
    inv["stock"]  = pd.to_numeric(inv["stock"], errors="coerce").fillna(0).astype(int)
    inv["item"]   = inv["item"].astype(str)
    if "product_id" not in inv.columns or inv["product_id"].isna().any():
        inv["product_id"] = inv["item"].apply(short_id_from_sku)
    return inv

def load_inventory_general():  return _load_inventory_file(INVENTORY_CSV)
def load_inventory_mkt():      return _load_inventory_file(INVENTORY_MKT_CSV)

def write_inventory(path: Path, inv: pd.DataFrame):
    inv.to_csv(path, index=False)

def write_menu_json(inv_general: pd.DataFrame):
    inv_ok = inv_general[inv_general["item"].apply(is_valid_sku)].copy()
    cols = [c for c in ["product_id","item","descripcion","precio"] if c in inv_ok.columns]
    MENU_JSON.write_text(
        json.dumps(inv_ok[cols].fillna("").to_dict(orient="records"), ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

# ====================== helpers CSV (UPSERT por issue) ======================

def _upsert_rows(path: Path, rows: list, issue_url: str):
    """Elimina filas del mismo issue y luego agrega 'rows'."""
    df = pd.read_csv(path, dtype=str) if path.exists() and path.stat().st_size>0 else pd.DataFrame()
    if "issue" in df.columns:
        df = df[df["issue"].astype(str) != str(issue_url)]
    df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    df.to_csv(path, index=False)

def _clean_items(items, require_price: bool = False):
    clean = []
    for it in (items or []):
        sku = str(it.get("item","")).strip()
        if not is_valid_sku(sku):
            continue
        try:
            qty = int(it.get("cantidad", 0))
        except Exception:
            qty = 0
        if qty <= 0:
            continue
        rec = {"item": sku, "cantidad": qty}
        if require_price:
            rec["precio_unit"] = str(it.get("precio_unit","") or it.get("precio","")).strip()
        clean.append(rec)
    return clean

# ====================== guardar movimientos ======================

def append_sales_general(inv: pd.DataFrame, fecha: str, items: list, issue_url: str, metodo_pago: str, txn_prefix="S"):
    items = _clean_items(items, require_price=True)
    if not items:
        return
    rows = []
    for it in items:
        sku = it["item"]; qty = int(it["cantidad"])
        precio_s = it.get("precio_unit","")
        precio = pd.to_numeric(precio_s, errors="coerce")
        if pd.isna(precio):
            row = inv.loc[inv["item"]==sku]
            precio = float(row["precio"].iloc[0]) if not row.empty and pd.notna(row["precio"].iloc[0]) else 0.0
        imp = float(qty) * float(precio)
        txn_id = new_txn_id(txn_prefix)
        source_id = hashlib.sha1(f"{issue_url}|general|{sku}|{qty}|{precio}|{fecha}".encode("utf-8")).hexdigest()
        desc = inv.loc[inv["item"]==sku, "descripcion"].iloc[0] if not inv.loc[inv["item"]==sku].empty else ""
        rows.append({
            "txn_id": txn_id, "fecha": fecha, "item": sku, "cantidad": qty,
            "precio_unit": f"{float(precio):.2f}", "importe": f"{imp:.2f}",
            "issue": issue_url, "metodo_pago": metodo_pago or "efectivo",
            "source_id": source_id, "descripcion": desc
        })
    _upsert_rows(SALES_CSV, rows, issue_url)

def append_sales_mkt(inv_mkt: pd.DataFrame, fecha: str, items: list, issue_url: str, metodo_pago: str, txn_prefix="SM"):
    items = _clean_items(items, require_price=True)
    if not items:
        return
    rows = []
    for it in items:
        sku = it["item"]; qty = int(it["cantidad"])
        precio_s = it.get("precio_unit","")
        precio = pd.to_numeric(precio_s, errors="coerce")
        if pd.isna(precio):
            row = inv_mkt.loc[inv_mkt["item"]==sku]
            precio = float(row["precio"].iloc[0]) if not row.empty and pd.notna(row["precio"].iloc[0]) else 0.0
        imp = float(qty) * float(precio)
        txn_id = new_txn_id(txn_prefix)
        source_id = hashlib.sha1(f"{issue_url}|mercado|{sku}|{qty}|{precio}|{fecha}".encode("utf-8")).hexdigest()
        desc = inv_mkt.loc[inv_mkt["item"]==sku, "descripcion"].iloc[0] if not inv_mkt.loc[inv_mkt["item"]==sku].empty else ""
        rows.append({
            "txn_id": txn_id, "fecha": fecha, "item": sku, "cantidad": qty,
            "precio_unit": f"{float(precio):.2f}", "importe": f"{imp:.2f}",
            "issue": issue_url, "metodo_pago": metodo_pago or "efectivo",
            "source_id": source_id, "descripcion": desc
        })
    _upsert_rows(SALES_MKT_CSV, rows, issue_url)

def append_production(inv: pd.DataFrame, fecha: str, items: list, issue_url: str, txn_prefix="P"):
    items = _clean_items(items, require_price=False)
    if not items:
        return
    rows = []
    for it in items:
        sku = it["item"]; qty = int(it["cantidad"])
        txn_id = new_txn_id(txn_prefix)
        source_id = hashlib.sha1(f"{issue_url}|prod|{sku}|{qty}|{fecha}".encode("utf-8")).hexdigest()
        desc = inv.loc[inv["item"]==sku, "descripcion"].iloc[0] if not inv.loc[inv["item"]==sku].empty else ""
        rows.append({
            "txn_id": txn_id, "fecha": fecha, "item": sku, "cantidad": qty,
            "issue": issue_url, "source_id": source_id, "descripcion": desc
        })
    _upsert_rows(PROD_CSV, rows, issue_url)

def append_transfer_mkt(inv_gen: pd.DataFrame, inv_mkt: pd.DataFrame, fecha: str, items: list, issue_url: str, txn_prefix="TM"):
    items = _clean_items(items, require_price=False)
    if not items:
        return
    rows = []
    for it in items:
        sku = it["item"]; qty = int(it["cantidad"])
        txn_id = new_txn_id(txn_prefix)
        source_id = hashlib.sha1(f"{issue_url}|transfer|{sku}|{qty}|{fecha}".encode("utf-8")).hexdigest()
        desc = inv_gen.loc[inv_gen["item"]==sku, "descripcion"].iloc[0] if not inv_gen.loc[inv_gen["item"]==sku].empty else ""
        rows.append({
            "txn_id": txn_id, "fecha": fecha, "item": sku, "cantidad": qty,
            "issue": issue_url, "source_id": source_id, "descripcion": desc
        })
    _upsert_rows(TRANSFER_MKT_CSV, rows, issue_url)

def apply_stock(inv: pd.DataFrame, items: list, sign: int, path: Path) -> pd.DataFrame:
    items = _clean_items(items, require_price=False)
    for it in items:
        sku = it["item"]; delta = sign * int(it["cantidad"])
        mask = inv["item"] == sku
        if not mask.any():
            inv = pd.concat([inv, pd.DataFrame([{
                "item": sku, "descripcion":"", "stock":0, "precio":"", "product_id": short_id_from_sku(sku)
            }])], ignore_index=True)
            mask = inv["item"] == sku
        inv.loc[mask, "stock"] = inv.loc[mask, "stock"].fillna(0).astype(int) + delta
    inv["stock"] = inv["stock"].astype(int)
    write_inventory(path, inv)
    return inv

# ====================== reportes ======================

def _sanitize_sales(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # columnas mínimas
    for c in ["txn_id","fecha","item","cantidad","precio_unit","importe","issue","metodo_pago","source_id","descripcion"]:
        if c not in df.columns:
            df[c] = ""
    # solo SKUs válidos
    df = df[df["item"].apply(is_valid_sku)].copy()
    # numéricos
    df["cantidad"]    = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)
    df["precio_unit"] = pd.to_numeric(df["precio_unit"], errors="coerce").fillna(0.0)
    df["importe"]     = pd.to_numeric(df["importe"], errors="coerce").fillna(0.0)
    # método de pago default
    df["metodo_pago"] = df["metodo_pago"].replace({"": "efectivo"}).fillna("efectivo")
    # dedupe por txn_id + item (en práctica txn_id ya es único)
    df = df.drop_duplicates(subset=["txn_id","item"], keep="last")
    return df

def build_reports(inv_gen: pd.DataFrame, inv_mkt: pd.DataFrame):
    # Sanea inventarios
    inv_gen = inv_gen[inv_gen["item"].apply(is_valid_sku)].copy()
    inv_mkt = inv_mkt[inv_mkt["item"].apply(is_valid_sku)].copy()
    inv_gen["stock"] = pd.to_numeric(inv_gen["stock"], errors="coerce").fillna(0).astype(int)
    inv_mkt["stock"] = pd.to_numeric(inv_mkt["stock"], errors="coerce").fillna(0).astype(int)

    # ----- general -----
    sales = pd.read_csv(SALES_CSV, dtype=str) if SALES_CSV.exists() else pd.DataFrame()
    sales = _sanitize_sales(sales)
    prod  = pd.read_csv(PROD_CSV, dtype=str) if PROD_CSV.exists() else pd.DataFrame()
    if not prod.empty:
        for c in ["cantidad"]:
            if c not in prod.columns: prod[c] = 0
        prod["cantidad"] = pd.to_numeric(prod["cantidad"], errors="coerce").fillna(0).astype(int)

    inv_key = inv_gen[["item","product_id","descripcion"]]
    sales_detail = sales.merge(inv_key, on="item", how="left", suffixes=("",""))
    prod_detail  = prod.merge(inv_key, on="item", how="left", suffixes=("",""))

    inv_out = inv_gen[["product_id","item","descripcion","precio","stock"]].sort_values("item")
    inv_out.to_csv(INV_OUT_CSV, index=False)
    sales_detail.to_csv(SALES_DETAIL_CSV, index=False)
    prod_detail.to_csv(PROD_DETAIL_CSV, index=False)

    by_item = (sales.groupby("item", as_index=False)[["cantidad","importe"]]
               .sum().sort_values(["cantidad","importe"], ascending=False))
    by_item.to_csv(SALES_ITEM_CSV, index=False)
    by_day = (sales.groupby("fecha", as_index=False)[["cantidad","importe"]]
              .sum().sort_values("fecha"))
    by_day.to_csv(SALES_DAY_CSV, index=False)

    report = {
        "generated_at": datetime.now(MX_TZ).isoformat(),
        "summary": {
            "items_distintos": int(inv_gen["item"].nunique()) if not inv_gen.empty else 0,
            "items_low_stock": int((inv_gen["stock"]<=5).sum()) if not inv_gen.empty else 0,
            "total_ventas": int(sales["cantidad"].sum()) if not sales.empty else 0,
            "total_importe": float(sales["importe"].sum()) if not sales.empty else 0.0,
            "total_producido": int(prod["cantidad"].sum()) if not prod.empty else 0
        }
    }
    REPORT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # HTML compacto
    # (si columna metodo_pago no existe por CSV viejos, ya está creada en _sanitize_sales)
    html = f"""<!doctype html><html lang="es"><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Reporte Inventario/Ventas</title>
    <style>body{{font-family:system-ui;margin:20px}} table{{border-collapse:collapse;width:100%}}
    th,td{{border:1px solid #ddd;padding:6px;text-align:left}} th{{background:#f7f7f7}} .kpi{{margin:0 0 6px}}</style>
    <h1>Reporte (General)</h1>
    <p class="kpi">Generado: {report['generated_at']}</p>
    <h2>Inventario actual</h2>
    {inv_out.to_html(index=False)}
    <h2>Ventas (detalle)</h2>
    {sales_detail[["txn_id","fecha","product_id","item","descripcion_x","cantidad","precio_unit","importe","metodo_pago","issue"]].rename(columns={"descripcion_x":"descripcion"}).to_html(index=False)}
    <h2>Producción (detalle)</h2>
    {prod_detail[["txn_id","fecha","product_id","item","descripcion_x","cantidad","issue"]].rename(columns={"descripcion_x":"descripcion"}).to_html(index=False)}
    <h2>Ventas por día</h2>
    {by_day.to_html(index=False)}
    <h2>Ventas por item</h2>
    {by_item.to_html(index=False)}
    </html>"""
    REPORT_HTML.write_text(html, encoding="utf-8")

    # diarios general (sobrescribir + dedupe)
    if not sales_detail.empty:
        sales_detail = sales_detail.drop_duplicates(subset=["txn_id","item"], keep="last")
        for fecha, group in sales_detail.groupby("fecha"):
            (DIARIO_DIR / f"{fecha}-ventas.csv").write_text(group.to_csv(index=False), encoding="utf-8")
    # índice de diarios
    fechas = sorted(set(sales_detail.get("fecha", pd.Series([], dtype=str)).dropna().tolist()) |
                    set(prod_detail.get("fecha", pd.Series([], dtype=str)).dropna().tolist()))
    idx_html = "<!doctype html><meta charset='utf-8'><title>Reportes diarios</title><h1>Reportes diarios</h1><ul>"
    for d in fechas:
        if not d: continue
        link = (f"<a href='{d}-ventas.csv'>ventas</a>") if (DIARIO_DIR / f"{d}-ventas.csv").exists() else ""
        idx_html += f"<li>{d}: {link}</li>"
    idx_html += "</ul>"
    (DIARIO_DIR/"index.html").write_text(idx_html, encoding="utf-8")

    # ----- mercado -----
    inv_mkt_out = inv_mkt[["product_id","item","descripcion","precio","stock"]].sort_values("item")
    inv_mkt_out.to_csv(INV_MKT_OUT_CSV, index=False)

    sales_mkt = pd.read_csv(SALES_MKT_CSV, dtype=str) if SALES_MKT_CSV.exists() else pd.DataFrame()
    sales_mkt = _sanitize_sales(sales_mkt)
    sales_mkt_detail = sales_mkt.merge(inv_mkt[["item","product_id","descripcion"]], on="item", how="left")
    sales_mkt_detail.to_csv(SALES_MKT_DETAIL_CSV, index=False)

    # diarios mercado
    if not sales_mkt_detail.empty:
        sales_mkt_detail = sales_mkt_detail.drop_duplicates(subset=["txn_id","item"], keep="last")
        for fecha, group in sales_mkt_detail.groupby("fecha"):
            (MKT_DIARIO_DIR / f"{fecha}-ventas.csv").write_text(group.to_csv(index=False), encoding="utf-8")

# ====================== parseo del issue ======================

def parse_issue(issue):
    body = (issue or {}).get("body","")
    labels = {l.get("name","").lower() for l in (issue or {}).get("labels", [])}

    fecha_raw = grab_field(body, "Fecha")
    notas = grab_field(body, "Notas")
    metodo_pago = (grab_field(body, "Método de pago") or grab_field(body, "Metodo de pago") or "").lower().strip()
    if metodo_pago not in ("tarjeta","efectivo"): metodo_pago = "efectivo"

    fecha = safe_parse_date(fecha_raw, issue)
    base = {
        "fecha": fecha,
        "issue_url": (issue or {}).get("html_url",""),
        "labels": labels,
        "notas": notas,
        "metodo_pago": metodo_pago
    }

    if "venta-mercado" in labels:
        table_items = parse_items_table(body, has_price=True)
        if table_items: return {"type":"venta_mkt", **base, "items": table_items}
        # fallback a campos sueltos
        sku = grab_field(body, "Item"); cant = grab_field(body, "Cantidad"); precio = grab_field(body, "Precio unitario (opcional)")
        try: cant_i = int(cant)
        except: cant_i = 1
        return {"type":"venta_mkt", **base, "items":[{"item":sku,"cantidad":cant_i,"precio_unit":precio}]}

    if "venta" in labels:
        table_items = parse_items_table(body, has_price=True)
        if table_items: return {"type":"venta", **base, "items": table_items}
        sku = grab_field(body, "Item"); cant = grab_field(body, "Cantidad"); precio = grab_field(body, "Precio unitario (opcional)")
        try: cant_i = int(cant)
        except: cant_i = 1
        return {"type":"venta", **base, "items":[{"item":sku,"cantidad":cant_i,"precio_unit":precio}]}

    if "produccion" in labels or "producción" in labels:
        table_items = parse_items_table(body, has_price=False)
        if table_items: return {"type":"prod", **base, "items": table_items}
        sku = grab_field(body, "Item"); cant = grab_field(body, "Cantidad")
        try: cant_i = int(cant)
        except: cant_i = 1
        return {"type":"prod", **base, "items":[{"item":sku,"cantidad":cant_i}]}

    if "abasto-mercado" in labels or "traspaso-mercado" in labels:
        table_items = parse_items_table(body, has_price=False)
        if table_items: return {"type":"abasto_mkt", **base, "items": table_items}
        sku = grab_field(body, "Item"); cant = grab_field(body, "Cantidad")
        try: cant_i = int(cant)
        except: cant_i = 1
        return {"type":"abasto_mkt", **base, "items":[{"item":sku,"cantidad":cant_i}]}

    return {"type":"none", **base, "items":[]}

# ====================== main ======================

def main():
    ensure_files()
    inv_gen = load_inventory_general()
    inv_mkt = load_inventory_mkt()
    write_menu_json(inv_gen)  # menú desde inventario general

    issue = load_event_issue()
    if issue is None:
        build_reports(inv_gen, inv_mkt)
        return

    data = parse_issue(issue)
    t = data["type"]

    if t == "venta_mkt":
        append_sales_mkt(inv_mkt, data["fecha"], data["items"], data["issue_url"], data.get("metodo_pago","efectivo"))
        inv_mkt = apply_stock(inv_mkt, data["items"], sign=-1, path=INVENTORY_MKT_CSV)
        build_reports(inv_gen, inv_mkt); return

    if t == "abasto_mkt":
        # resta en general, suma en mercado
        inv_gen = apply_stock(inv_gen, data["items"], sign=-1, path=INVENTORY_CSV)
        inv_mkt = apply_stock(inv_mkt, data["items"], sign=+1, path=INVENTORY_MKT_CSV)
        append_transfer_mkt(inv_gen, inv_mkt, data["fecha"], data["items"], data["issue_url"])
        build_reports(inv_gen, inv_mkt); return

    if t == "venta":
        append_sales_general(inv_gen, data["fecha"], data["items"], data["issue_url"], data.get("metodo_pago","efectivo"))
        inv_gen = apply_stock(inv_gen, data["items"], sign=-1, path=INVENTORY_CSV)
        build_reports(inv_gen, inv_mkt); return

    if t == "prod":
        append_production(inv_gen, data["fecha"], data["items"], data["issue_url"])
        inv_gen = apply_stock(inv_gen, data["items"], sign=+1, path=INVENTORY_CSV)
        build_reports(inv_gen, inv_mkt); return

    # por defecto solo reconstruye reportes
    build_reports(inv_gen, inv_mkt)

if __name__ == "__main__":
    main()