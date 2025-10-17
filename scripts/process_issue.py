import json, os, re, secrets, hashlib
from datetime import datetime
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DOCS = ROOT / "docs"

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

# --------------------- utilidades ---------------------

def ensure_files():
    DATA.mkdir(parents=True, exist_ok=True)
    DOCS.mkdir(parents=True, exist_ok=True)
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

def parse_issue(issue):
    body = (issue or {}).get("body") or ""
    fecha = grab_field(body, "Fecha")
    item  = grab_field(body, "Item")  # sku
    cantidad = grab_field(body, "Cantidad")
    precio = grab_field(body, "Precio unitario (opcional)")
    notas  = grab_field(body, "Notas")

    fecha_dt = datetime.strptime(fecha, "%Y-%m-%d").date()
    cantidad_i = int(cantidad)
    if cantidad_i <= 0: raise ValueError("Cantidad debe ser > 0")

    precio_f = None
    if precio:
        try: precio_f = float(precio)
        except ValueError: precio_f = None

    return {
        "fecha": str(fecha_dt),
        "item": item.strip(),               # SKU
        "cantidad": cantidad_i,
        "precio_unit": (f"{precio_f:.2f}" if precio_f is not None else ""),
        "notas": notas.strip(),
        "issue_url": (issue or {}).get("html_url", ""),
        "labels": {l.get("name","").lower() for l in (issue or {}).get("labels", [])}
    }

def short_id_from_sku(sku: str) -> str:
    """hash determinístico de 8 chars para product_id."""
    h = hashlib.sha1(sku.encode("utf-8")).hexdigest()[:8]
    return h.upper()

def new_txn_id(prefix: str) -> str:
    """S-... para ventas, P-... para producción."""
    now = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    rand = secrets.token_hex(3).upper()   # 6 hex
    return f"{prefix}-{now}-{rand}"

# --------------------- inventario / menú ---------------------

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

    # product_id (si no existe, lo calculamos)
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

# --------------------- movimientos ---------------------

def append_sale(inv: pd.DataFrame, sale):
    txn_id = new_txn_id("S")
    precio_unit = pd.to_numeric(sale["precio_unit"], errors="coerce")
    if pd.isna(precio_unit):
        # si no viene precio, intenta tomarlo del inventario
        row = inv.loc[inv["item"]==sale["item"]]
        precio_unit = float(row["precio"].iloc[0]) if not row.empty and pd.notna(row["precio"].iloc[0]) else 0.0
    importe = float(sale["cantidad"]) * float(precio_unit)

    row = {
        "txn_id": txn_id,
        "fecha": sale["fecha"],
        "item": sale["item"],
        "cantidad": sale["cantidad"],
        "precio_unit": f"{precio_unit:.2f}",
        "importe": f"{importe:.2f}",
        "issue": sale["issue_url"],
    }
    df = pd.read_csv(SALES_CSV) if SALES_CSV.exists() and SALES_CSV.stat().st_size>0 else pd.DataFrame()
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(SALES_CSV, index=False)
    return txn_id, importe

def append_production(entry):
    txn_id = new_txn_id("P")
    row = {
        "txn_id": txn_id,
        "fecha": entry["fecha"],
        "item": entry["item"],
        "cantidad": entry["cantidad"],
        "issue": entry["issue_url"],
    }
    df = pd.read_csv(PROD_CSV) if PROD_CSV.exists() and PROD_CSV.stat().st_size>0 else pd.DataFrame()
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(PROD_CSV, index=False)
    return txn_id

def update_stock(inv: pd.DataFrame, item: str, delta: int):
    mask = inv["item"] == item
    if not mask.any():
        # si no existe, lo creamos con product_id
        inv = pd.concat([inv, pd.DataFrame([{
            "item": item, "descripcion":"", "stock":0, "precio":"", "product_id": short_id_from_sku(item)
        }])], ignore_index=True)
        mask = inv["item"] == item
    inv.loc[mask, "stock"] = inv.loc[mask, "stock"].fillna(0).astype(int) + int(delta)
    inv["stock"] = inv["stock"].astype(int)
    write_inventory(inv)
    return inv

# --------------------- reportes ---------------------

def build_reports(inv: pd.DataFrame):
    # Ventas
    sales = pd.read_csv(SALES_CSV) if SALES_CSV.exists() else pd.DataFrame(
        columns=["txn_id","fecha","item","cantidad","precio_unit","importe","issue"])
    if not sales.empty:
        sales["cantidad"] = pd.to_numeric(sales["cantidad"], errors="coerce").fillna(0).astype(int)
        sales["precio_unit"] = pd.to_numeric(sales["precio_unit"], errors="coerce").fillna(0.0)
        sales["importe"] = pd.to_numeric(sales["importe"], errors="coerce").fillna(0.0)
    # Producción
    prod = pd.read_csv(PROD_CSV) if PROD_CSV.exists() else pd.DataFrame(
        columns=["txn_id","fecha","item","cantidad","issue"])
    if not prod.empty:
        prod["cantidad"] = pd.to_numeric(prod["cantidad"], errors="coerce").fillna(0).astype(int)

    # Agrega product_id/descripcion al detalle de ventas y producción
    inv_key = inv[["item","product_id","descripcion"]]
    sales_detail = sales.merge(inv_key, on="item", how="left")
    prod_detail  = prod.merge(inv_key, on="item", how="left")

    # CSVs
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

    # JSON para dashboards
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

# --------------------- flujo principal ---------------------

def main():
    ensure_files()
    inv = load_inventory()
    write_menu_json(inv)   # para que el selector de productos funcione

    issue = load_event_issue()
    if issue is None:
        build_reports(inv)
        return

    data = parse_issue(issue)
    labels = data["labels"]

    if "venta" in labels:
        txn_id, _importe = append_sale(inv, data)
        inv = update_stock(inv, data["item"], delta=-data["cantidad"])
        # (Opcional: podríamos comentar en el issue el txn_id)

    elif "produccion" in labels:
        txn_id = append_production(data)
        inv = update_stock(inv, data["item"], delta=+data["cantidad"])

    # genera reportes para ambos casos
    build_reports(inv)

if __name__ == "__main__":
    main()
