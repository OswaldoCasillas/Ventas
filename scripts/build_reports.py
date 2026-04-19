# scripts/build_reports.py
# ------------------------------------------------------------
# Genera archivos de reportes a partir de detalles de ventas y producción.
# Corrige el manejo de fechas mezcladas (str/float/NaT) y estandariza a YYYY-MM-DD.
# ------------------------------------------------------------

from __future__ import annotations
from pathlib import Path
import pandas as pd


def _collect_unique_dates(*series_like) -> list[str]:
    """
    Recibe una o más Series/listas de fechas, convierte todo a datetime,
    descarta NaT y regresa lista única ordenada como 'YYYY-MM-DD'.
    """
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


def _ensure_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _safe_df(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    return df.copy()


def build_reports(
    inv_gen: pd.DataFrame,
    inv_mkt: pd.DataFrame,
    sales_detail: pd.DataFrame | None = None,
    prod_detail: pd.DataFrame | None = None,
    out_dir: str | Path = "docs"
) -> None:
    """
    Genera:
      - docs/ventas_por_dia.csv  (fecha,cantidad,importe)
      - docs/ventas_por_item.csv (item,cantidad,importe)
      - docs/ventas_detalle.csv  (detalle consolidado)
      - docs/diario/YYYY-MM-DD-ventas.csv (detalle por día)
      - docs/produccion_detalle.csv (si hay producción)

    Parámetros:
      inv_gen, inv_mkt: inventarios (se mantienen por compatibilidad)
      sales_detail: DataFrame con columnas al menos:
        fecha,item,cantidad,precio_unit,importe,descripcion,product_id
      prod_detail: DataFrame con columnas al menos:
        fecha,item,cantidad,descripcion,product_id
    """
    out_dir = Path(out_dir)
    _ensure_dir(out_dir / "diario/file.txt")

    # -------- Normalización segura --------
    sales_detail = _safe_df(sales_detail)
    prod_detail = _safe_df(prod_detail)

    # -------- Asegura columnas básicas para ventas --------
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

    sales_detail["cantidad"] = pd.to_numeric(
        sales_detail["cantidad"], errors="coerce"
    ).fillna(0).astype(int)

    sales_detail["precio_unit"] = pd.to_numeric(
        sales_detail["precio_unit"], errors="coerce"
    )

    sales_detail["importe"] = pd.to_numeric(
        sales_detail["importe"], errors="coerce"
    )

    # Si no viene "payment", usa "metodo_pago"
    if "payment" not in sales_detail.columns or sales_detail["payment"].isna().all():
        sales_detail["payment"] = sales_detail["metodo_pago"]

    sales_detail["metodo_pago"] = sales_detail["metodo_pago"].fillna("efectivo")
    sales_detail["payment"] = sales_detail["payment"].fillna(sales_detail["metodo_pago"])

    # Si importe viene vacío, lo calculamos
    mask_imp = sales_detail["importe"].isna()
    if not sales_detail.empty:
        sales_detail.loc[mask_imp, "importe"] = (
            sales_detail.loc[mask_imp, "cantidad"].astype(float)
            * sales_detail.loc[mask_imp, "precio_unit"].fillna(0).astype(float)
        )

    # -------- Producción --------
    for col in ["txn_id", "fecha", "item", "cantidad", "issue", "source_id", "descripcion", "product_id"]:
        if col not in prod_detail.columns:
            prod_detail[col] = None

    if not prod_detail.empty:
        prod_detail["cantidad"] = pd.to_numeric(
            prod_detail["cantidad"], errors="coerce"
        ).fillna(0).astype(int)

    # -------- Fechas únicas --------
    dates = _collect_unique_dates(
        sales_detail.get("fecha"),
        prod_detail.get("fecha")
    )

    # -------- ventas_detalle.csv --------
    ventas_detalle_csv = out_dir / "ventas_detalle.csv"
    _ensure_dir(ventas_detalle_csv)

    if not sales_detail.empty:
        sales_export = sales_detail.copy()
        sales_export["fecha"] = pd.to_datetime(
            sales_export["fecha"].astype(str),
            errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        sales_export = sales_export.sort_values(
            ["fecha", "item", "precio_unit"],
            na_position="last"
        )
    else:
        sales_export = sales_detail.copy()

    sales_export.to_csv(ventas_detalle_csv, index=False)

    # -------- diarios por fecha --------
    for d in dates:
        if sales_export.empty:
            day_rows = sales_export.copy()
        else:
            day_rows = sales_export[sales_export["fecha"] == d]

        daily_path = out_dir / "diario" / f"{d}-ventas.csv"
        _ensure_dir(daily_path)
        day_rows.to_csv(daily_path, index=False)

    # -------- ventas_por_dia.csv --------
    vpd_path = out_dir / "ventas_por_dia.csv"
    _ensure_dir(vpd_path)

    if not sales_export.empty:
        vpd = (
            sales_export.groupby("fecha", as_index=False)
            .agg(
                cantidad=("cantidad", "sum"),
                importe=("importe", "sum"),
            )
            .sort_values("fecha")
        )
    else:
        vpd = pd.DataFrame(columns=["fecha", "cantidad", "importe"])

    vpd.to_csv(vpd_path, index=False)

    # -------- ventas_por_item.csv --------
    vpi_path = out_dir / "ventas_por_item.csv"
    _ensure_dir(vpi_path)

    if not sales_export.empty:
        vpi = (
            sales_export.groupby("item", as_index=False)
            .agg(
                cantidad=("cantidad", "sum"),
                importe=("importe", "sum"),
            )
            .sort_values(["cantidad", "importe", "item"], ascending=[False, False, True])
        )
    else:
        vpi = pd.DataFrame(columns=["item", "cantidad", "importe"])

    vpi.to_csv(vpi_path, index=False)

    # -------- produccion_detalle.csv --------
    prod_csv = out_dir / "produccion_detalle.csv"

    if not prod_detail.empty:
        prod_export = prod_detail.copy()
        prod_export["fecha"] = pd.to_datetime(
            prod_export["fecha"].astype(str),
            errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        prod_export = prod_export.sort_values(
            ["fecha", "item"],
            na_position="last"
        )

        _ensure_dir(prod_csv)
        prod_export.to_csv(prod_csv, index=False)
