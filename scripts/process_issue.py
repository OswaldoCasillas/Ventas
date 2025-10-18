#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Procesa issues de ventas/producción y genera CSVs diarios para las páginas.
Soporta cuerpo de issue con formato:
  Fecha: YYYY-MM-DD
  Notas: ...
  Items
  SKU | Cantidad | Precio
  PALETA-AGUA-FRESA | 2 | 25.00

También soporta el formato con negritas:
  **Fecha**: YYYY-MM-DD
y, si no encuentra la fecha en el cuerpo, la toma del título:
  Venta: N items @ YYYY-MM-DD
"""

import os
import re
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

def log(*a): print("[inventory]", *a)

# ----------- utilidades de catálogo ----------
def load_menu():
    """
    Carga menu.json para mapear SKU -> descripción y precio por defecto.
    Busca primero docs/menu.json (GitHub Pages), luego menu.json raíz.
    """
    for p in [DOCS / "menu.json", ROOT / "menu.json"]:
        if p.exists():
            try:
                with p.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                # normaliza a dict por item
                return {row["item"]: row for row in data if "item" in row}
            except Exception as e:
                log(f"no pude leer {p}: {e}")
    log("⚠️  no encontré menu.json; descripciones saldrán con el SKU")
    return {}

MENU = load_menu()

def desc_for(sku):
    row = MENU.get(sku)
    if not row:
        return sku
    # si el registro ya trae 'descripcion' úsala; si no, intenta derivar
    return (row.get("descripcion") or row.get("item") or sku)

def price_for(sku):
    row = MENU.get(sku)
    if not row:
        return ""
    return row.get("precio", "")

# ---------- parseo robusto del issue ----------
DATE_RX_BODY = re.compile(
    r"^\s*\*{0,2}\s*fecha\s*\*{0,2}\s*[:：]\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*$",
    re.IGNORECASE | re.MULTILINE,
)
DATE_RX_TITLE = re.compile(r"@?\s*(\d{4}-\d{2}-\d{2})\s*$")
ITEMS_HEADER_RX = re.compile(r"^\s*items\s*$", re.IGNORECASE)

def parse_issue_payload(issue: dict):
    """
    Devuelve dict con:
      fecha: 'YYYY-MM-DD'
      items: [{'item': sku, 'cantidad': int, 'precio': str}]
      labels: set([...])
      title, body
    """
    title = issue.get("title") or ""
    body  = issue.get("body")  or ""
    labels = {lbl.get("name","").lower() for lbl in issue.get("labels", [])}

    # Fecha: intenta en cuerpo (con y sin ** **)
    m = DATE_RX_BODY.search(body or "")
    fecha = m.group(1) if m else None
    # Fallback al título (… @ YYYY-MM-DD)
    if not fecha:
        m2 = DATE_RX_TITLE.search(title)
        if m2:
            fecha = m2.group(1)

    # Localiza sección Items
    lines = (body or "").splitlines()
    items = []
    start_idx = None
    for i, ln in enumerate(lines):
        if ITEMS_HEADER_RX.match(ln.strip()):
            start_idx = i + 1
            break
    if start_idx is None:
        # a veces no ponen la línea "Items"; intenta leer cualquier línea con pipes
        start_idx = 0

    for ln in lines[start_idx:]:
        if "|" not in ln:
            continue
        row = [c.strip() for c in ln.split("|")]
        if len(row) < 2:
            continue

        # salta encabezados/separadores
        joined = " ".join(row).lower()
        if "sku" in joined and "cantidad" in joined:
            continue
        if set("".join(row)) <= {"-", " "}:
            continue

        # columnas esperadas: SKU | Cantidad | Precio?
        sku = row[0]
        if not sku or sku.lower() in ("sku",):
            continue
        try:
            cantidad = int(row[1])
        except Exception:
            # si viene vacío o no numérico, ignora esa línea
            continue

        precio = ""
        if len(row) >= 3 and row[2]:
            precio = row[2]
        items.append({"item": sku, "cantidad": cantidad, "precio": precio})

    return {
        "fecha": fecha,
        "items": items,
        "labels": labels,
        "title": title,
        "body": body,
    }

# ---------- escritura de CSV diario ----------
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def write_daily_csv(base_dir: Path, fecha: str, rows: list):
    """
    rows: lista de dicts con claves:
      fecha, sku, descripcion, cantidad, precio, importe
    """
    ensure_dir(base_dir)
    out = base_dir / f"{fecha}.csv"
    new_file = not out.exists()
    with out.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["fecha", "sku", "descripcion", "cantidad", "precio", "importe"])
        for r in rows:
            w.writerow([r["fecha"], r["sku"], r["descripcion"], r["cantidad"], r["precio"], r["importe"]])
    log(f"✓ actualizado {out.relative_to(ROOT)}")

# ---------- flujo principal ----------
def process_issue_event(event_path: Path):
    with event_path.open("r", encoding="utf-8") as f:
        event = json.load(f)

    issue = event.get("issue") or {}
    if not issue:
        log("no es evento de issue; nada que hacer")
        return

    parsed = parse_issue_payload(issue)
    fecha = parsed["fecha"]
    items = parsed["items"]
    labels = parsed["labels"]

    if not fecha:
        log("⚠️  issue sin fecha, titulo:", issue.get("title"))
        return
    if not items:
        log("⚠️  issue sin items; body:\n", issue.get("body"))
        return

    # ¿Es mercado?
    is_mercado = any("mercado" in lbl for lbl in labels)
    base_dir = DOCS / ("mercado/diario" if is_mercado else "diario")

    # Normaliza filas para CSV
    rows = []
    for it in items:
        sku = it["item"]
        cant = int(it.get("cantidad") or 0)
        # precio: usa el del item; si no viene, intenta del menú
        p_unit = it.get("precio")
        if p_unit in (None, "", "0", 0):
            p_unit = price_for(sku)
        try:
            p_float = float(str(p_unit).replace("$", "").replace(",", "")) if p_unit not in ("", None) else 0.0
        except Exception:
            p_float = 0.0
        importe = round(cant * p_float, 2)
        rows.append({
            "fecha": fecha,
            "sku": sku,
            "descripcion": desc_for(sku),
            "cantidad": cant,
            "precio": f"{p_float:.2f}" if p_unit not in ("", None) else "",
            "importe": f"{importe:.2f}",
        })

    write_daily_csv(base_dir, fecha, rows)

def main():
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if event_path and Path(event_path).exists():
        process_issue_event(Path(event_path))
    else:
        log("sin GITHUB_EVENT_PATH; nada que procesar")

if __name__ == "__main__":
    main()
