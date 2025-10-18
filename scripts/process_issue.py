#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Procesa issues de ventas/producción y genera CSVs diarios para las páginas.

Formato del cuerpo del Issue aceptado (ambos):
  Fecha: YYYY-MM-DD
  Notas: ...
  Items
  SKU | Cantidad | Precio
  PALETA-AGUA-FRESA | 2 | 25.00

o con negritas:
  **Fecha**: YYYY-MM-DD

Si no encuentra la fecha en el cuerpo, la toma del título:
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

# ===================== Catálogo (menu.json) =====================
def load_menu():
    """Cargar docs/menu.json (o menu.json en raíz) → dict por SKU"""
    for p in [DOCS / "menu.json", ROOT / "menu.json"]:
        if p.exists():
            try:
                with p.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                return {row["item"]: row for row in data if "item" in row}
            except Exception as e:
                log(f"no pude leer {p}: {e}")
    log("⚠️  no encontré menu.json; descripciones/precios pueden quedar vacíos.")
    return {}

MENU = load_menu()

def desc_for(sku: str) -> str:
    row = MENU.get(sku)
    if not row:
        return sku
    return (row.get("descripcion") or row.get("item") or sku)

def price_for(sku: str):
    row = MENU.get(sku)
    if not row:
        return ""
    return row.get("precio", "")

# ===================== Parseo de Issue =====================
DATE_RX_BODY = re.compile(
    r"^\s*\*{0,2}\s*fecha\s*\*{0,2}\s*[:：]\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*$",
    re.IGNORECASE | re.MULTILINE,
)
DATE_RX_TITLE = re.compile(r"@?\s*(\d{4}-\d{2}-\d{2})\s*$")
ITEMS_HEADER_RX = re.compile(r"^\s*items\s*$", re.IGNORECASE)

def parse_issue_payload(issue: dict):
    """
    Devuelve: {
      'fecha': 'YYYY-MM-DD',
      'items': [{'item': sku, 'cantidad': int, 'precio': str}],
      'labels': set([...]),
      'title': str,
      'body': str
    }
    """
    title  = issue.get("title") or ""
    body   = issue.get("body")  or ""
    labels = {lbl.get("name","").lower() for lbl in issue.get("labels", [])}

    # 1) fecha en el cuerpo (Fecha: ... o **Fecha**: ...)
    m = DATE_RX_BODY.search(body)
    fecha = m.group(1) if m else None
    # 2) fallback: fecha al final del título “... @ YYYY-MM-DD”
    if not fecha:
        mt = DATE_RX_TITLE.search(title)
        if mt:
            fecha = mt.group(1)

    # 3) localizar bloque Items
    lines = body.splitlines()
    start_idx = None
    for i, ln in enumerate(lines):
        if ITEMS_HEADER_RX.match(ln.strip()):
            start_idx = i + 1
            break
    if start_idx is None:
        start_idx = 0  # buscar cualquier línea con pipes

    items = []
    for ln in lines[start_idx:]:
        if "|" not in ln:
            continue
        row = [c.strip() for c in ln.split("|")]
        if len(row) < 2:
            continue

        # —— filtros para NO capturar encabezados/separadores/etiquetas ——
        joined_lower = " ".join(row).lower()

        # encabezado típico "sku | cantidad | precio"
        if ("sku" in joined_lower and "cantidad" in joined_lower) or ("precio" in joined_lower and "sku" in joined_lower):
            continue

        # separadores "----|----"
        if set("".join(row)) <= {"-", " ", ":"}:
            continue

        # líneas tipo "**Cantidad**:"
        if re.match(r"^\**\s*cantidad\s*\**\s*:?\s*$", row[0], flags=re.I):
            continue

        # —— Parseo esperado: SKU | Cantidad | Precio? ——
        sku = row[0]
        if not sku or sku.lower() == "sku":
            continue

        try:
            cantidad = int(row[1])
        except Exception:
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

# ===================== Escritura de CSV =====================
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def write_daily_csv(base_dir: Path, fecha: str, rows: list):
    """
    rows: [{'fecha','sku','descripcion','cantidad','precio','importe'}]
    Escribe/append en base_dir/YYYY-MM-DD.csv
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

# ===================== Flujo principal =====================
def process_issue_event(event_path: Path):
    with event_path.open("r", encoding="utf-8") as f:
        event = json.load(f)

    issue = event.get("issue") or {}
    if not issue:
        log("no es evento de issue; nada que hacer")
        return

    parsed = parse_issue_payload(issue)
    fecha  = parsed["fecha"]
    items  = parsed["items"]
    labels = parsed["labels"]

    if not fecha:
        log("⚠️  issue sin fecha; título:", issue.get("title"))
        return
    if not items:
        log("⚠️  issue sin items; body:\n", (issue.get("body") or "")[:400])
        return

    # ¿Es mercado? (si alguna etiqueta contiene “mercado”)
    is_mercado = any("mercado" in lbl for lbl in labels)
    base_dir = DOCS / ("mercado/diario" if is_mercado else "diario")

    # Normalizar filas para CSV
    rows = []
    for it in items:
        sku  = it["item"]
        cant = int(it.get("cantidad") or 0)

        # precio unitario: usa el del item; si no viene, el del menú
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
