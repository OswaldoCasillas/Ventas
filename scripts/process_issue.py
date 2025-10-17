import json, os, re, secrets, hashlib
from datetime import datetime
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DOCS = ROOT / "docs"
DIARIO_DIR = DOCS / "diario"

INVENTORY_CSV = DATA / "inventory.csv"
SALES_CSV     = DATA / "sales.csv"
PROD_CSV      = DATA / "production.csv"

MENU_JSON       = DOCS / "menu.json"
REPORT_JSON     = DOCS / "report.json"
INV_OUT_CSV     = DOCS / "inventario_actual.csv"
SALES_ITEM_CSV  = DOCS / "ventas_por_item.csv"
SALES_DAY_CSV   = DOCS / "ventas_por_dia.csv"
SALES_DETAIL_CSV= DOCS / "ventas_detalle.csv"
PROD_DETAIL_CSV = DOCS / "produccion_detalle.csv"
REPORT_HTML     = DOCS / "reporte.html"

def ensure_files():
    DATA.mkdir(parents=True, exist_ok=True)
    DOCS.mkdir(parents=True, exist_ok=True)
    DIARIO_DIR.mkdir(parents=True, exist_ok=True)
    if not INVENTORY_CSV.exists():
        INVENTORY_CSV.write_text("item,descripcion,stock,precio\n", encoding="utf-8")
    if not SALES_CSV.exists():
        SALES_CSV.write_text("txn_id,fecha,item,cantidad,precio_unit,importe,issue\n", encoding="utf-8")
    if not PROD_CSV.exists():
        PROD_CSV.write_text("txn_id,fecha,item,cantidad,issue\n", encoding="utf-8")

def load_event_issue():
    path = os.environ.get("GITHUB_EVENT_PATH")
    if not path or not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        evt = json.load(fh)
    return evt.get("issue")

def grab_field(body: str, key: str) -> str:
    pat = rf"(?:\*\*\s*{re.escape(key)}\s*\*\*|{re.escape(key)})\s*:\s*(.+)"
    m = re.search(pat, body, re.IGNORECASE)
    return m.group(1).strip() if m else ""

def parse_items_table(body: str, has_price: bool):
    """
    Lee una sección:
      **Items**
      SKU | Cantidad | Precio   (si has_price=True)
      PALETA-AGUA-MANGO | 2 | 25
    o:
      SKU | Cantidad            (si has_price=False)
      PALETA-AGUA-MANGO | 10
    Devuelve lista de dicts.
    """
    if "**Items**" not in body:
        return []
    after = body.split("**Items**",1)[1].strip()
    lines = [ln.strip() for ln in after.splitlines() if ln.strip()]
    out = []
    for ln in lines:
        if "|" not in ln:      # hasta que se acaben las filas tipo tabla
            break
        parts = [p.strip() for p in ln.split("|")]
        if has_price and len(parts) >= 3:
            sku, qty, price = parts[0], parts[1], parts[2]
            if not sku: continue
            try: qty_i = int(qty)
            except: continue
            out.append({"item": sku, "cantidad": qty_i, "precio_unit": price})
        elif not has_price and len(parts) >= 2:
            sku, qty = parts[0], parts[1]
            if not sku: continue
            try: qty_i = int(qty)
            except: continue
            out.append({"item": sku, "cantidad": qty_i})
        else:
            continue
    return out

def short_id_from_sku(sku: str) -> str:
    return hashlib.sha1(sku.encode("utf-8")).hexdigest()[:8].upper()

def new_txn_id(prefix: str) -> str:
    now = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    rand = secrets.token_hex(3).upper()
    return f"{prefix}-{now}-{rand}"

def load_inventory():
    if INVENTORY_CSV.exists() and INVENTORY_CSV.stat().st_size>0:
        inv = pd.read_csv(INVENTORY_CSV)
    else:
        inv = pd.DataFrame(columns=["item","descripcion","stock","precio"])
    for col in ["descripcion","precio","stock"]:
        if col not in inv.columns:
            inv[col] = "" if col!="stock" else 0
    inv["precio"] = pd.to_numeric(inv["precio"], errors="coerce")
    inv["stock"]  = pd.to_numeric(inv["stock"], errors="coerce").fillna(0).astype(int)
    inv["item"]   = inv["item"].astype(str)
    if "product_id" not in inv.columns:
        inv["product_id"] = inv["item"].apply(short_id_from_sku)
    else:
        inv["product_id"] = inv["product_id"].astype(str)
        inv.loc[inv["product_id"].isna() | (inv["product_id"]==""), "product_id"] = inv["item"].apply(short_id_from_sku)
    return inv

def write_inventory(inv: pd.DataFrame):
    inv.to_csv(INVENTORY_CSV, index=False)

def write_menu_json(inv: pd.DataFrame):
    cols = [c for c in ["product_id","item","descripcion","precio"] if c in inv.columns]
    MENU_JSON.write_text(json.dumps(inv[cols].fillna("").to_dict(orient="records"),
                                    ensure_ascii=False, indent=2),
                         encoding="utf-8")

def append_sales_rows(inv: pd.DataFrame, fecha: str, items: list, issue_url: str, base_txn_id: str):
    """
    items: [{item, cantidad, precio_unit?}]
    Crea filas en sales.csv (una por item), mismas txn_id (lote).
    """
    df = pd.read_csv(SALES_CSV) if SALES_CSV.exists() and SALES_CSV.stat().st_size>0 else pd.DataFrame()
    for it in items:
        sku = it["item"]
        qty = int(it["cantidad"])
        precio_s = it.get("precio_unit","")
        precio = pd.to_numeric(precio_s, errors="coerce")
        if pd.isna(precio):
            row = inv.loc[inv["item"]==sku]
            precio = float(row["precio"].iloc[0]) if not row.empty and pd.notna(row["precio"].iloc[0]) else 0.0
        importe = float(qty) * float(precio)
        df = pd.concat([df, pd.DataFrame([{
            "txn_id": base_txn_id,
            "fecha": fecha,
            "item": sku,
            "cantidad": qty,
            "precio_unit": f"{precio:.2f}",
            "importe": f"{importe:.2f}",
            "issue": issue_url
        }])], ignore_index=True)
    df.to_csv(SALES_CSV, index=False)

def append_production_rows(fecha: str, items: list, issue_url: str, base_txn_id: str):
    df = pd.read_csv(PROD_CSV) if PROD_CSV.exists() and PROD_CSV.stat().st_size>0 else pd.DataFrame()
    for it in items:
        sku = it["item"]
        qty = int(it["cantidad"])
        df = pd.concat([df, pd.DataFrame([{
            "txn_id": base_txn_id,
            "fecha": fecha,
            "item": sku,
            "cantidad": qty,
            "issue": issue_url
        }])], ignore_index=True)
    df.to_csv(PROD_CSV, index=False)

def apply_stock(inv: pd.DataFrame, items: list, sign: int):
    # sign = -1 ventas, +1 producción
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
    write_inventory(inv)
    return inv

def build_reports(inv: pd.DataFrame):
    sales = pd.read_csv(SALES_CSV) if SALES_CSV.exists() else pd.DataFrame(
        columns=["txn_id","fecha","item","cantidad","precio_unit","importe","issue"])
    if not sales.empty:
        sales["cantidad"] = pd.to_numeric(sales["cantidad"], errors="coerce").fillna(0).astype(int)
        sales["precio_unit"] = pd.to_numeric(sales["precio_unit"], errors="coerce").fillna(0.0)
        sales["importe"] = pd.to_numeric(sales["importe"], errors="coerce").fillna(0.0)

    prod = pd.read_csv(PROD_CSV) if PROD_CSV.exists() else pd.DataFrame(
        columns=["txn_id","fecha","item","cantidad","issue"])
    if not prod.empty:
        prod["cantidad"] = pd.to_numeric(prod["cantidad"], errors="coerce").fillna(0).astype(int)

    inv_key = inv[["item","product_id","descripcion"]]
    sales_detail = sales.merge(inv_key, on="item", how="left")
    prod_detail  = prod.merge(inv_key, on="item", how="left")

    # CSVs para imprimir
    inv_out = inv[["product_id","item","descripcion","precio","stock"]].sort_values("item")
    inv_out.to_csv(INV_OUT_CSV, index=False)
    sales_detail.to_csv(SALES_DETAIL_CSV, index=False)
    prod_detail.to_csv(PROD_DETAIL_CSV, index=False)

    by_item = (sales.groupby("item", as_index=False)[["cantidad","importe"]]
               .sum().sort_values(["cantidad","importe"], ascending=False))
    by_item.to_csv(SALES_ITEM_CSV, index=False)
    by_day = (sales.groupby("fecha", as_index=False)[["cantidad","importe"]]
              .sum().sort_values("fecha"))
    by_day.to_csv(SALES_DAY_CSV, index=False)

    # JSON
    low = inv[inv["stock"] <= 5].sort_values("stock")
    prod_by_day = prod.groupby("fecha", as_index=False)["cantidad"].sum().sort_values("fecha") if not prod.empty else pd.DataFrame(columns=["fecha","cantidad"])
    report = {
        "generated_at": datetime.utcnow().isoformat()+"Z",
        "summary": {
            "items_distintos": int(inv["item"].nunique()) if not inv.empty else 0,
            "items_low_stock": int((inv["stock"]<=5).sum()) if not inv.empty else 0,
            "total_ventas": int(sales["cantidad"].sum()) if not sales.empty else 0,
            "total_importe": float(sales["importe"].sum()) if not sales.empty else 0.0,
            "total_producido": int(prod["cantidad"].sum()) if not prod.empty else 0
        },
        "low_stock": low[["product_id","item","descripcion","stock"]].to_dict(orient="records"),
        "ventas_por_dia": by_day.to_dict(orient="records"),
        "ventas_por_item": by_item.to_dict(orient="records"),
        "produccion_por_dia": prod_by_day.to_dict(orient="records")
    }
    REPORT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # HTML imprimible
    html = f"""<!doctype html><html lang="es"><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Reporte Inventario/Ventas</title>
    <style>body{{font-family:system-ui;margin:20px}} table{{border-collapse:collapse;width:100%}}
    th,td{{border:1px solid #ddd;padding:6px;text-align:left}} th{{background:#f7f7f7}} .kpi{{margin:0 0 6px}}</style>
    <h1>Reporte</h1>
    <p class="kpi">Generado: {report['generated_at']}</p>
    <ul>
      <li class="kpi">Items: {report['summary']['items_distintos']}</li>
      <li class="kpi">Low stock (≤5): {report['summary']['items_low_stock']}</li>
      <li class="kpi">Unidades vendidas: {report['summary']['total_ventas']}</li>
      <li class="kpi">Importe total: ${report['summary']['total_importe']:.2f}</li>
      <li class="kpi">Total producido: {report['summary']['total_producido']}</li>
    </ul>
    <h2>Inventario actual</h2>
    {inv_out.to_html(index=False)}
    <h2>Ventas (detalle)</h2>
    {sales_detail[["txn_id","fecha","product_id","item","descripcion","cantidad","precio_unit","importe","issue"]].to_html(index=False)}
    <h2>Producción (detalle)</h2>
    {prod_detail[["txn_id","fecha","product_id","item","descripcion","cantidad","issue"]].to_html(index=False)}
    <h2>Ventas por día</h2>
    {by_day.to_html(index=False)}
    <h2>Ventas por item</h2>
    {by_item.to_html(index=False)}
    </html>"""
    REPORT_HTML.write_text(html, encoding="utf-8")

    # ---------- Reporte diario (un CSV por fecha) ----------
    # Ventas detalle por fecha (según 'fecha' capturada en el issue)
    if not sales_detail.empty:
        for fecha, group in sales_detail.groupby("fecha"):
            out = DIARIO_DIR / f"{fecha}-ventas.csv"
            group.to_csv(out, index=False)
    # Índice de días disponibles
    dates = sorted({*sales_detail.get("fecha",[]).tolist(), *prod_detail.get("fecha",[]).tolist()})
    idx_html = "<!doctype html><meta charset='utf-8'><title>Reportes diarios</title><h1>Reportes diarios</h1><ul>"
    for d in dates:
        if not d: continue
        links = []
        if (DIARIO_DIR / f"{d}-ventas.csv").exists(): links.append(f"<a href='{d}-ventas.csv'>ventas</a>")
        idx_html += f"<li>{d}: {' | '.join(links)}</li>"
    idx_html += "</ul>"
    (DIARIO_DIR/"index.html").write_text(idx_html, encoding="utf-8")

def parse_issue(issue):
    body = (issue or {}).get("body","")
    labels = {l.get("name","").lower() for l in (issue or {}).get("labels", [])}
    fecha = grab_field(body,"Fecha")
    notas = grab_field(body,"Notas")
    fecha_dt = datetime.strptime(fecha,"%Y-%m-%d").date()
    base = {"fecha": str(fecha_dt), "issue_url": (issue or {}).get("html_url",""), "labels": labels, "notas": notas}

    # ¿Viene tabla de items?
    if "venta" in labels:
        table_items = parse_items_table(body, has_price=True)
        if table_items:
            return {"type":"venta_multi", **base, "items": table_items}
        # fallback a un solo item
        item = grab_field(body,"Item"); cant = grab_field(body,"Cantidad"); precio = grab_field(body,"Precio unitario (opcional)")
        return {"type":"venta_single", **base, "items":[{"item":item,"cantidad":int(cant),"precio_unit":precio}]}

    if "produccion" in labels:
        table_items = parse_items_table(body, has_price=False)
        if table_items:
            return {"type":"prod_multi", **base, "items": table_items}
        item = grab_field(body,"Item"); cant = grab_field(body,"Cantidad")
        return {"type":"prod_single", **base, "items":[{"item":item,"cantidad":int(cant)}]}

    return {"type":"none", **base}

def main():
    ensure_files()
    inv = load_inventory()
    write_menu_json(inv)

    issue = load_event_issue()
    if issue is None:
        build_reports(inv)
        return

    data = parse_issue(issue)
    t = data["type"]

    if t.startswith("venta"):
        txn = new_txn_id("S")
        append_sales_rows(inv, data["fecha"], data["items"], data["issue_url"], txn)
        inv = apply_stock(inv, data["items"], sign=-1)
        build_reports(inv); return

    if t.startswith("prod"):
        txn = new_txn_id("P")
        append_production_rows(data["fecha"], data["items"], data["issue_url"], txn)
        inv = apply_stock(inv, data["items"], sign=+1)
        build_reports(inv); return

    build_reports(inv)

if __name__ == "__main__":
    main()
