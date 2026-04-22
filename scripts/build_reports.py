# scripts/build_reports.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def _ensure_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _safe_df(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    return df.copy()


def _collect_unique_dates(*series_like) -> list[str]:
    chunks = []
    for s in series_like:
        if s is None:
            continue
        s = pd.Series(s)
        dt = pd.to_datetime(s.astype(str), errors="coerce").dropna()
        if not dt.empty:
            chunks.append(dt)

    if not chunks:
        return []

    all_dt = pd.concat(chunks, ignore_index=True)
    return sorted(all_dt.dt.strftime("%Y-%m-%d").unique().tolist())


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    _ensure_dir(path)
    df.to_csv(path, index=False, lineterminator="\n")


def build_reports(
    inv_gen: pd.DataFrame,
    inv_mkt: pd.DataFrame,
    sales_detail: pd.DataFrame | None = None,
    prod_detail: pd.DataFrame | None = None,
    out_dir: str | Path = "docs",
) -> None:
    out_dir = Path(out_dir)
    _ensure_dir(out_dir / "diario/file.txt")

    sales_detail = _safe_df(sales_detail)
    prod_detail = _safe_df(prod_detail)

    for col in [
        "txn_id",
        "fecha",
        "item",
        "cantidad",
        "precio_unit",
        "importe",
        "issue",
        "metodo_pago",
        "payment",
        "descripcion",
        "product_id",
        "source_id",
    ]:
        if col not in sales_detail.columns:
            sales_detail[col] = None

    sales_detail["cantidad"] = pd.to_numeric(sales_detail["cantidad"], errors="coerce").fillna(0).astype(int)
    sales_detail["precio_unit"] = pd.to_numeric(sales_detail["precio_unit"], errors="coerce")
    sales_detail["importe"] = pd.to_numeric(sales_detail["importe"], errors="coerce")

    if "payment" not in sales_detail.columns or sales_detail["payment"].isna().all():
        sales_detail["payment"] = sales_detail["metodo_pago"]

    sales_detail["metodo_pago"] = sales_detail["metodo_pago"].fillna("efectivo")
    sales_detail["payment"] = sales_detail["payment"].fillna(sales_detail["metodo_pago"])

    mask_imp = sales_detail["importe"].isna()
    if not sales_detail.empty:
        sales_detail.loc[mask_imp, "importe"] = (
            sales_detail.loc[mask_imp, "cantidad"].astype(float)
            * sales_detail.loc[mask_imp, "precio_unit"].fillna(0).astype(float)
        )

    for col in ["txn_id", "fecha", "item", "cantidad", "issue", "source_id", "descripcion", "product_id"]:
        if col not in prod_detail.columns:
            prod_detail[col] = None

    if not prod_detail.empty:
        prod_detail["cantidad"] = pd.to_numeric(prod_detail["cantidad"], errors="coerce").fillna(0).astype(int)

    dates = _collect_unique_dates(sales_detail.get("fecha"), prod_detail.get("fecha"))

    if not sales_detail.empty:
        sales_export = sales_detail.copy()
        sales_export["fecha"] = pd.to_datetime(sales_export["fecha"].astype(str), errors="coerce").dt.strftime("%Y-%m-%d")
        sales_export = sales_export.sort_values(["fecha", "item", "precio_unit"], na_position="last")
    else:
        sales_export = sales_detail.copy()

    _write_csv(sales_export, out_dir / "ventas_detalle.csv")

    for d in dates:
        if sales_export.empty:
            day_rows = sales_export.copy()
        else:
            day_rows = sales_export[sales_export["fecha"] == d]
        _write_csv(day_rows, out_dir / "diario" / f"{d}-ventas.csv")

    if not sales_export.empty:
        vpd = (
            sales_export.groupby("fecha", as_index=False)
            .agg(cantidad=("cantidad", "sum"), importe=("importe", "sum"))
            .sort_values("fecha")
        )
    else:
        vpd = pd.DataFrame(columns=["fecha", "cantidad", "importe"])
    _write_csv(vpd, out_dir / "ventas_por_dia.csv")

    if not sales_export.empty:
        vpi = (
            sales_export.groupby("item", as_index=False)
            .agg(cantidad=("cantidad", "sum"), importe=("importe", "sum"))
            .sort_values(["cantidad", "importe", "item"], ascending=[False, False, True])
        )
    else:
        vpi = pd.DataFrame(columns=["item", "cantidad", "importe"])
    _write_csv(vpi, out_dir / "ventas_por_item.csv")

    if not prod_detail.empty:
        prod_export = prod_detail.copy()
        prod_export["fecha"] = pd.to_datetime(prod_export["fecha"].astype(str), errors="coerce").dt.strftime("%Y-%m-%d")
        prod_export = prod_export.sort_values(["fecha", "item"], na_position="last")
        _write_csv(prod_export, out_dir / "produccion_detalle.csv")


if __name__ == "__main__":
    # Mantén tu lógica actual de carga de CSVs / fuentes y luego llama a build_reports(...)
    print("Usa este archivo reemplazando tu scripts/build_reports.py actual.")
