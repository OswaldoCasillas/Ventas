from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INVENTORY_MERCADO_CSV = ROOT / "data" / "inventory_mercado.csv"
MENU_MERCADO_JSON = ROOT / "docs" / "menu_mercado.json"

REMOVE_TOKENS = [
    "AGUA-FRESCA",
    "SENCILLO",
    "DOBLE",
    "EXTRA",
    "MALTEADA",
    "CHAMOYADA",
    "CHOCOLATE-CALIENTE",
    "FRAPUCHINO",
    "DORILOCOS",
    "PLATANITO",
]

REMOVE_EXACT = {
    "BEBIDA-CHAMOYADA",
    "BEBIDA-CHOCOLATE-CALIENTE",
    "BEBIDA-CHOCOLATE-CALIENTE-VEGANO",
    "BEBIDA-FRAPUCHINO",
    "BOTANA-DORILOCOS",
    "BOTANA-PLATANITOS",
}

def to_int(value: str) -> int:
    try:
        return int(float(str(value).strip() or "0"))
    except Exception:
        return 0

def to_float(value: str) -> float:
    try:
        return float(str(value).strip() or "0")
    except Exception:
        return 0.0

def should_remove(item: str, descripcion: str) -> bool:
    item_up = item.upper().strip()
    text = f"{item} {descripcion}".upper()
    if item_up in REMOVE_EXACT:
        return True
    return any(token in text for token in REMOVE_TOKENS)

def mercado_price(item: str, precio: float) -> float:
    item_up = item.upper().strip()
    if item_up.startswith("PALETA-MINI-"):
        return 10.0
    return precio

def load_inventory(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            item = str(raw.get("item", "")).strip()
            descripcion = str(raw.get("descripcion", "")).strip()
            if not item:
                continue
            if should_remove(item, descripcion):
                continue
            precio = mercado_price(item, to_float(raw.get("precio", "")))
            rows.append({
                "product_id": str(raw.get("product_id", "")).strip(),
                "item": item,
                "descripcion": descripcion,
                "precio": precio,
                "stock": to_int(raw.get("stock", "")),
            })
    rows.sort(key=lambda x: (x["item"], x["descripcion"], x["product_id"]))
    return rows

def write_menu(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
        newline="\n",
    )

def main() -> None:
    if not INVENTORY_MERCADO_CSV.exists():
        raise FileNotFoundError(f"No existe inventory_mercado.csv: {INVENTORY_MERCADO_CSV}")

    rows = load_inventory(INVENTORY_MERCADO_CSV)
    write_menu(MENU_MERCADO_JSON, rows)
    print(f"Menu mercado generado: {MENU_MERCADO_JSON} ({len(rows)} productos)")

if __name__ == "__main__":
    main()
