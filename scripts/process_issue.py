#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lee eventos de Issues (ventas/producción) y escribe CSV por día.
General  → docs/diario/YYYY-MM-DD.csv
Mercado → docs/mercado/diario/YYYY-MM-DD.csv

Formato soportado en el body del Issue:
Fecha: YYYY-MM-DD
Notas:
Items
SKU | Cantidad | Precio
PALETA-AGUA-FRESA | 2 | 25.00

También acepta **Fecha**: ... y toma la fecha del título ("... @ YYYY-MM-DD") si falta.
"""

import os, re, csv, json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

def log(*a): print("[inventory]", *a)

# ---------- catálogo (para completar descripción/precio si faltan) ----------
def load_menu():
    for p in [DOCS/"menu.json", ROOT/"menu.json"]:
        if p.exists():
            try:
                return {r["item"]: r for r in json.loads(p.read_text(encoding="utf-8")) if "item" in r}
            except Exception as e:
                log("no pude leer", p, e)
    log("⚠️  sin menu.json (descripciones/precios pueden ir vacíos)")
    return {}
MENU = load_menu()
def desc_for(sku): r = MENU.get(sku); return (r.get("descripcion") if r else None) or sku
def price_for(sku): r = MENU.get(sku); return (r.get("precio") if r else "")

# ---------- parseo del issue ----------
DATE_RX_BODY  = re.compile(r"^\s*\*{0,2}fecha\*{0,2}\s*:\s*(\d{4}-\d{2}-\d{2})\s*$", re.I|re.M)
DATE_RX_TITLE = re.compile(r"@?\s*(\d{4}-\d{2}-\d{2})\s*$")
ITEMS_RX      = re.compile(r"^\s*items\s*$", re.I)

def parse_issue(issue: dict):
    title  = issue.get("title") or ""
    body   = issue.get("body")  or ""
    labels = { (l.get("name") or "").lower() for l in issue.get("labels", []) }

    m = DATE_RX_BODY.search(body); fecha = m.group(1) if m else None
    if not fecha:
        mt = DATE_RX_TITLE.search(title); fecha = mt.group(1) if mt else None

    lines = body.splitlines()
    start = 0
    for i, ln in enumerate(lines):
        if ITEMS_RX.match(ln.strip()): start = i+1; break

    items=[]
    for ln in lines[start:]:
        if "|" not in ln: continue
        row = [c.strip() for c in ln.split("|")]
        if len(row)<2: continue

        # filtros para no tragar encabezados / separadores
        jl = " ".join(row).lower()
        if ("sku" in jl and "cantidad" in jl) or ("precio" in jl and "sku" in jl): continue
        if set("".join(row)) <= {"-"," ",":"}: continue
        if re.match(r"^\**\s*cantidad\s*\**\s*:?\s*$", row[0], re.I): continue

        sku=row[0]
        if not sku or sku.lower()=="sku": continue
        try:
            cant=int(row[1])
        except Exception:
            continue
        precio = row[2] if len(row)>=3 and row[2] else ""

        items.append({"item":sku, "cantidad":cant, "precio":precio})

    return {"fecha":fecha, "items":items, "labels":labels}

def ensure_dir(p:Path): p.mkdir(parents=True, exist_ok=True)

def write_daily(fecha:str, rows:list, mercado:bool):
    base = DOCS/("mercado/diario" if mercado else "diario")
    ensure_dir(base)
    out = base/f"{fecha}.csv"
    new = not out.exists()
    with out.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new: w.writerow(["fecha","sku","descripcion","cantidad","precio","importe"])
        for r in rows:
            w.writerow([r["fecha"], r["sku"], r["descripcion"], r["cantidad"], r["precio"], r["importe"]])
    log("✓ actualizado", out.relative_to(ROOT))

def main():
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path or not Path(event_path).exists():
        log("sin GITHUB_EVENT_PATH; nada que hacer"); return
    event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    issue = event.get("issue") or {}
    parsed = parse_issue(issue)
    fecha, items, labels = parsed["fecha"], parsed["items"], parsed["labels"]

    if not fecha: log("⚠️ sin fecha"); return
    if not items: log("⚠️ sin items"); return

    mercado = any("mercado" in l for l in labels)
    rows=[]
    for it in items:
        sku  = it["item"]
        cant = int(it["cantidad"])
        p_u  = it.get("precio") or price_for(sku) or ""
        try: p = float(str(p_u).replace("$","").replace(",","")) if p_u!="" else 0.0
        except: p = 0.0
        rows.append({
            "fecha":fecha,
            "sku":sku,
            "descripcion":desc_for(sku),
            "cantidad":cant,
            "precio": (f"{p:.2f}" if p_u!="" else ""),
            "importe": f"{cant*p:.2f}"
        })
    write_daily(fecha, rows, mercado)

if __name__=="__main__": main()
