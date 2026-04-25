from __future__ import annotations

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DOCS = ROOT / "docs"


def ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def read_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame(columns=columns)
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df[columns].copy()


def normalize_sales(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in ["cantidad", "precio_unit", "importe"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["cantidad"] = df["cantidad"].astype(int)
    if "status" in df.columns:
        df = df[df["status"].fillna("activa").isin(["", "activa"])]
    df["fecha"] = pd.to_datetime(df["fecha"].astype(str), errors="coerce")
    df = df[df["fecha"].notna()].copy()
    df["fecha"] = df["fecha"].dt.strftime("%Y-%m-%d")
    if "descripcion" in df.columns:
        df["descripcion"] = df["descripcion"].replace("", pd.NA).fillna(df["item"])
    df["metodo_pago"] = df["metodo_pago"].replace("", "efectivo")
    return df.sort_values(["fecha", "txn_id", "item"], ascending=[True, True, True], na_position="last")


def write_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path)
    df.to_csv(path, index=False, lineterminator="\n")


def export_sales_views(df: pd.DataFrame, base_dir: Path) -> None:
    detail_cols = ["txn_id", "fecha", "item", "cantidad", "precio_unit", "importe", "issue", "metodo_pago", "source_id", "descripcion", "status", "correction_ref", "notas"]
    out = df.copy()
    for col in detail_cols:
        if col not in out.columns:
            out[col] = ""
    out = out[detail_cols]
    write_csv(out, base_dir / "ventas_detalle.csv")

    dates = [d for d in sorted(out["fecha"].dropna().unique().tolist()) if str(d) and str(d) != "nan"]
    for d in dates:
        write_csv(out[out["fecha"] == d].copy(), base_dir / "diario" / f"{d}-ventas.csv")

    by_day = out.groupby("fecha", as_index=False).agg(cantidad=("cantidad", "sum"), importe=("importe", "sum")).sort_values("fecha")
    write_csv(by_day, base_dir / "ventas_por_dia.csv")

    by_item = out.groupby("item", as_index=False).agg(cantidad=("cantidad", "sum"), importe=("importe", "sum")).sort_values(["cantidad", "importe", "item"], ascending=[False, False, True])
    write_csv(by_item, base_dir / "ventas_por_item.csv")


def export_production(df: pd.DataFrame) -> None:
    cols = ["fecha", "item", "cantidad", "issue", "source_id", "descripcion"]
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    if not out.empty:
        out["cantidad"] = pd.to_numeric(out["cantidad"], errors="coerce").fillna(0).astype(int)
        out["fecha"] = pd.to_datetime(out["fecha"].astype(str), errors="coerce")
        out = out[out["fecha"].notna()].copy()
        out["fecha"] = out["fecha"].dt.strftime("%Y-%m-%d")
        out = out.sort_values(["fecha", "item"], na_position="last")
    write_csv(out[cols], DOCS / "produccion_detalle.csv")


def main() -> None:
    sales_cols = ["txn_id", "fecha", "item", "cantidad", "precio_unit", "importe", "issue", "metodo_pago", "source_id", "descripcion", "status", "correction_ref", "notas"]
    prod_cols = ["fecha", "item", "cantidad", "issue", "source_id", "descripcion"]

    sales = normalize_sales(read_csv(DATA / "sales.csv", sales_cols))
    sales_mkt = normalize_sales(read_csv(DATA / "sales_mercado.csv", sales_cols))
    production = read_csv(DATA / "production.csv", prod_cols)

    export_sales_views(sales, DOCS)
    export_sales_views(sales_mkt, DOCS / "mercado")
    export_production(production)
    print("Reportes reconstruidos correctamente.")


if __name__ == "__main__":
    main()
