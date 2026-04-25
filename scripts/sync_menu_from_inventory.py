from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INVENTORY_CSV = ROOT / "data" / "inventory.csv"
MENU_JSON = ROOT / "docs" / "menu.json"


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


def load_inventory(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            item = str(raw.get("item", "")).strip()
            if not item:
                continue

            rows.append(
                {
                    "product_id": str(raw.get("product_id", "")).strip(),
                    "item": item,
                    "descripcion": str(raw.get("descripcion", "")).strip(),
                    "precio": to_float(raw.get("precio", "")),
                    "stock": to_int(raw.get("stock", "")),
                }
            )

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
    if not INVENTORY_CSV.exists():
        raise FileNotFoundError(f"No existe inventory.csv: {INVENTORY_CSV}")

    rows = load_inventory(INVENTORY_CSV)
    write_menu(MENU_JSON, rows)
    print(f"Menu generado: {MENU_JSON} ({len(rows)} productos)")


if __name__ == "__main__":
    main()
