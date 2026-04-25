from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INVENTORY_MERCADO_CSV = ROOT / "data" / "inventory_mercado.csv"

REMOVE_TOKENS = [
    "AGUA-FRESCA",
    "SENCILLO",
    "DOBLE",
    "EXTRA",
]

def should_remove(item: str, descripcion: str) -> bool:
    text = f"{item} {descripcion}".upper()
    return any(token in text for token in REMOVE_TOKENS)

def main() -> None:
    if not INVENTORY_MERCADO_CSV.exists():
      raise FileNotFoundError(f"No existe: {INVENTORY_MERCADO_CSV}")

    with INVENTORY_MERCADO_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = [str(x).strip() for x in (reader.fieldnames or [])]
        rows = []
        removed = 0
        for raw in reader:
            row = {str(k).strip(): (v if v is not None else "") for k, v in raw.items()}
            item = str(row.get("item", "")).strip()
            descripcion = str(row.get("descripcion", "")).strip()
            if should_remove(item, descripcion):
                removed += 1
                continue
            rows.append(row)

    with INVENTORY_MERCADO_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"inventory_mercado limpio: {len(rows)} filas, removidas {removed}")

if __name__ == "__main__":
    main()
