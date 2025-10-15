import json, os, re
from datetime import datetime
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DOCS = ROOT / "docs"
INVENTORY_CSV = DATA / "inventory.csv"
SALES_CSV = DATA / "sales.csv"
REPORT_JSON = DOCS / "report.json"
MENU_JSON = DOCS / "menu.json"
INDEX_HTML = DOCS / "index.html"  # dashboard (opcional, puedes usar el que te di antes)

def ensure_files():
    DATA.mkdir(parents=True, exist_ok=True)
    DOCS.mkdir(parents=True, exist_ok=True)
    if not INVENTORY_CSV.exists():
        INVENTORY_CSV.write_text("item,descripcion,stock,precio\n", encoding="utf-8")
    if not SALES_CSV.exists():
        SALES_CSV.write_text("fecha,item,cantidad,precio_unit,issue\n", encoding="utf-8")

def load_event_issue():
    path = os.environ.get("GITHUB_EVENT_PATH")
    if not path or not os.path.exists(path): return None
    with open(path, "r", encoding="utf-8") as fh:
        evt = json.load(fh)
    return evt.get("issue")

def parse_issue(issue):
    """
    Acepta cuerpo tipo:
      **Fecha**: 2025-10-15
      **Item**: PALETA-MANGO
      **Cantidad**: 2
      **Precio unitario (opcional)**: 25
      **Notas**: cliente X
    """
    body = (issue or {}).get("body") or ""
    def grab(key):
        # busca "**Key**: valor" o "Key: valor"
        pat = rf"(?:\*\*\s*{re.escape(key)}\s*\*\*|{re.escape(key)})\s*:\s*(.+)"
        m = re.search(pat, body, re.IGNORECASE)
        return m.group(1).strip() if m else ""
    fecha = grab("Fecha")
    item = grab("Item")
    cantidad = grab("Cantidad")
    precio = grab("Precio unitario (opcional)")
    notas = grab("Notas")

    # normaliza y valida
    try:
        fecha_dt = datetime.strptime(fecha, "%Y-%m-%d").date()
    except Exception as e:
        # Si es un push del inventario, no hay issue que procesar
        if issue is None:
            return None
        raise ValueError(f"Fecha inválida: {fecha!r}") from e
    try:
        cantidad_i = int(cantidad)
        if cantidad_i <= 0: raise ValueError
    except Exception as e:
        raise ValueError(f"Cantidad inválida: {cantidad!r}") from e
    try:
        precio_f = float(precio) if precio else None
    except Exception:
        precio_f = None

    return {
        "fecha": str(fecha_dt),
        "item": item,
        "cantidad": cantidad_i,
        "precio_unit": (f"{precio_f:.2f}" if precio_f is not None else ""),
        "notas": notas,
        "issue_url": (issue or {}).get("html_url", "")
    }

def load_inventory():
    if INVENTORY_CSV.exists() and INVENTORY_CSV.stat().st_size>0:
        df = pd.read_csv(INVENTORY_CSV)
    else:
        df = pd.DataFrame(columns=["item","descripcion","stock","precio"])
    for col in ["descripcion","precio","stock"]:
        if col not in df.columns:
            df[col] = "" if col!="stock" else 0
    if "precio" in df.columns:
        df["precio"] = pd.to_numeric(df["precio"], errors="coerce")
    if "stock" in df.columns:
        df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
    df["item"] = df["item"].astype(str)
    return df

def write_menu_json(inv: pd.DataFrame):
    # publicamos menú con {item, descripcion, precio}
    cols = [c for c in ["item","descripcion","precio"] if c in inv.columns]
    menu = inv[cols].fillna("").to_dict(orient="records")
    MENU_JSON.write_text(json.dumps(menu, ensure_ascii=False, indent=2), encoding="utf-8")

def append_sale(sale):
    row = {
        "fecha": sale["fecha"],
        "item": sale["item"],
        "cantidad": sale["cantidad"],
        "precio_unit": sale["precio_unit"],
        "issue": sale["issue_url"],
    }
    if SALES_CSV.exists() and SALES_CSV.stat().st_size>0:
        df = pd.read_csv(SALES_CSV)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])
    df.to_csv(SALES_CSV, index=False)

def update_stock(inv: pd.DataFrame, sale):
    mask = inv["item"] == sale["item"]
    if not mask.any():
        inv = pd.concat([inv, pd.DataFrame([{
            "item": sale["item"], "descripcion":"", "stock":0, "precio": ""
        }])], ignore_index=True)
        mask = inv["item"] == sale["item"]
    inv.loc[mask, "stock"] = inv.loc[mask, "stock"].fillna(0).astype(int) - int(sale["cantidad"])
    inv["stock"] = inv["stock"].astype(int)
    inv.to_csv(INVENTORY_CSV, index=False)
    return inv

def build_report(inv: pd.DataFrame):
    sales = pd.read_csv(SALES_CSV) if SALES_CSV.exists() else pd.DataFrame(
        columns=["fecha","item","cantidad","precio_unit","issue"]
    )
    if not sales.empty:
        sales["cantidad"] = pd.to_numeric(sales["cantidad"], errors="coerce").fillna(0).astype(int)
        sales["precio_unit"] = pd.to_numeric(sales["precio_unit"], errors="coerce")
        sales["importe"] = (sales["cantidad"] * sales["precio_unit"]).fillna(0.0)
    else:
        sales["importe"] = []
    low = inv[inv["stock"] <= 5].sort_values("stock")
    by_day = sales.groupby("fecha", as_index=False)[["cantidad","importe"]].sum().sort_values("fecha").to_dict(orient="records") if not sales.empty else []
    by_item = sales.groupby("item", as_index=False)[["cantidad","importe"]].sum().sort_values(["cantidad","importe"], ascending=False).to_dict(orient="records") if not sales.empty else []
    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "summary": {
            "items_distintos": int(inv["item"].nunique()) if not inv.empty else 0,
            "items_low_stock": int((inv["stock"] <= 5).sum()) if not inv.empty else 0,
            "total_ventas": int(sales["cantidad"].sum()) if not sales.empty else 0,
            "total_importe": float(sales["importe"].sum()) if not sales.empty else 0.0,
        },
        "low_stock": low[["item","descripcion","stock"]].to_dict(orient="records"),
        "ventas_por_dia": by_day,
        "ventas_por_item": by_item,
    }
    REPORT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    ensure_files()
    inv = load_inventory()
    # Siempre actualizamos el menú si cambió inventario.csv (por evento push) o si hay venta
    write_menu_json(inv)
    issue = load_event_issue()
    # Si vino por "push" (sin issue), solo regeneramos menu.json y terminamos
    if issue is None:
        return
    # Procesar venta
    if not any(lbl.get("name")=="venta" for lbl in issue.get("labels", [])):
        # No es venta: nada que hacer
        return
    sale = parse_issue(issue)
    append_sale(sale)
    inv = update_stock(inv, sale)
    build_report(inv)

if __name__ == "__main__":
    main()
import json, os, re
from datetime import datetime
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DOCS = ROOT / "docs"
INVENTORY_CSV = DATA / "inventory.csv"
SALES_CSV = DATA / "sales.csv"
REPORT_JSON = DOCS / "report.json"
MENU_JSON = DOCS / "menu.json"
INDEX_HTML = DOCS / "index.html"  # dashboard (opcional, puedes usar el que te di antes)

def ensure_files():
    DATA.mkdir(parents=True, exist_ok=True)
    DOCS.mkdir(parents=True, exist_ok=True)
    if not INVENTORY_CSV.exists():
        INVENTORY_CSV.write_text("item,descripcion,stock,precio\n", encoding="utf-8")
    if not SALES_CSV.exists():
        SALES_CSV.write_text("fecha,item,cantidad,precio_unit,issue\n", encoding="utf-8")

def load_event_issue():
    path = os.environ.get("GITHUB_EVENT_PATH")
    if not path or not os.path.exists(path): return None
    with open(path, "r", encoding="utf-8") as fh:
        evt = json.load(fh)
    return evt.get("issue")

def parse_issue(issue):
    """
    Acepta cuerpo tipo:
      **Fecha**: 2025-10-15
      **Item**: PALETA-MANGO
      **Cantidad**: 2
      **Precio unitario (opcional)**: 25
      **Notas**: cliente X
    """
    body = (issue or {}).get("body") or ""
    def grab(key):
        # busca "**Key**: valor" o "Key: valor"
        pat = rf"(?:\*\*\s*{re.escape(key)}\s*\*\*|{re.escape(key)})\s*:\s*(.+)"
        m = re.search(pat, body, re.IGNORECASE)
        return m.group(1).strip() if m else ""
    fecha = grab("Fecha")
    item = grab("Item")
    cantidad = grab("Cantidad")
    precio = grab("Precio unitario (opcional)")
    notas = grab("Notas")

    # normaliza y valida
    try:
        fecha_dt = datetime.strptime(fecha, "%Y-%m-%d").date()
    except Exception as e:
        # Si es un push del inventario, no hay issue que procesar
        if issue is None:
            return None
        raise ValueError(f"Fecha inválida: {fecha!r}") from e
    try:
        cantidad_i = int(cantidad)
        if cantidad_i <= 0: raise ValueError
    except Exception as e:
        raise ValueError(f"Cantidad inválida: {cantidad!r}") from e
    try:
        precio_f = float(precio) if precio else None
    except Exception:
        precio_f = None

    return {
        "fecha": str(fecha_dt),
        "item": item,
        "cantidad": cantidad_i,
        "precio_unit": (f"{precio_f:.2f}" if precio_f is not None else ""),
        "notas": notas,
        "issue_url": (issue or {}).get("html_url", "")
    }

def load_inventory():
    if INVENTORY_CSV.exists() and INVENTORY_CSV.stat().st_size>0:
        df = pd.read_csv(INVENTORY_CSV)
    else:
        df = pd.DataFrame(columns=["item","descripcion","stock","precio"])
    for col in ["descripcion","precio","stock"]:
        if col not in df.columns:
            df[col] = "" if col!="stock" else 0
    if "precio" in df.columns:
        df["precio"] = pd.to_numeric(df["precio"], errors="coerce")
    if "stock" in df.columns:
        df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
    df["item"] = df["item"].astype(str)
    return df

def write_menu_json(inv: pd.DataFrame):
    # publicamos menú con {item, descripcion, precio}
    cols = [c for c in ["item","descripcion","precio"] if c in inv.columns]
    menu = inv[cols].fillna("").to_dict(orient="records")
    MENU_JSON.write_text(json.dumps(menu, ensure_ascii=False, indent=2), encoding="utf-8")

def append_sale(sale):
    row = {
        "fecha": sale["fecha"],
        "item": sale["item"],
        "cantidad": sale["cantidad"],
        "precio_unit": sale["precio_unit"],
        "issue": sale["issue_url"],
    }
    if SALES_CSV.exists() and SALES_CSV.stat().st_size>0:
        df = pd.read_csv(SALES_CSV)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])
    df.to_csv(SALES_CSV, index=False)

def update_stock(inv: pd.DataFrame, sale):
    mask = inv["item"] == sale["item"]
    if not mask.any():
        inv = pd.concat([inv, pd.DataFrame([{
            "item": sale["item"], "descripcion":"", "stock":0, "precio": ""
        }])], ignore_index=True)
        mask = inv["item"] == sale["item"]
    inv.loc[mask, "stock"] = inv.loc[mask, "stock"].fillna(0).astype(int) - int(sale["cantidad"])
    inv["stock"] = inv["stock"].astype(int)
    inv.to_csv(INVENTORY_CSV, index=False)
    return inv

def build_report(inv: pd.DataFrame):
    sales = pd.read_csv(SALES_CSV) if SALES_CSV.exists() else pd.DataFrame(
        columns=["fecha","item","cantidad","precio_unit","issue"]
    )
    if not sales.empty:
        sales["cantidad"] = pd.to_numeric(sales["cantidad"], errors="coerce").fillna(0).astype(int)
        sales["precio_unit"] = pd.to_numeric(sales["precio_unit"], errors="coerce")
        sales["importe"] = (sales["cantidad"] * sales["precio_unit"]).fillna(0.0)
    else:
        sales["importe"] = []
    low = inv[inv["stock"] <= 5].sort_values("stock")
    by_day = sales.groupby("fecha", as_index=False)[["cantidad","importe"]].sum().sort_values("fecha").to_dict(orient="records") if not sales.empty else []
    by_item = sales.groupby("item", as_index=False)[["cantidad","importe"]].sum().sort_values(["cantidad","importe"], ascending=False).to_dict(orient="records") if not sales.empty else []
    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "summary": {
            "items_distintos": int(inv["item"].nunique()) if not inv.empty else 0,
            "items_low_stock": int((inv["stock"] <= 5).sum()) if not inv.empty else 0,
            "total_ventas": int(sales["cantidad"].sum()) if not sales.empty else 0,
            "total_importe": float(sales["importe"].sum()) if not sales.empty else 0.0,
        },
        "low_stock": low[["item","descripcion","stock"]].to_dict(orient="records"),
        "ventas_por_dia": by_day,
        "ventas_por_item": by_item,
    }
    REPORT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    ensure_files()
    inv = load_inventory()
    # Siempre actualizamos el menú si cambió inventario.csv (por evento push) o si hay venta
    write_menu_json(inv)
    issue = load_event_issue()
    # Si vino por "push" (sin issue), solo regeneramos menu.json y terminamos
    if issue is None:
        return
    # Procesar venta
    if not any(lbl.get("name")=="venta" for lbl in issue.get("labels", [])):
        # No es venta: nada que hacer
        return
    sale = parse_issue(issue)
    append_sale(sale)
    inv = update_stock(inv, sale)
    build_report(inv)

if __name__ == "__main__":
    main()
