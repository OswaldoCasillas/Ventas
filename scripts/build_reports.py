# scripts/build_reports.py
# ------------------------------------------------------------
# Genera archivos de reportes a partir de detalles de ventas y producción.
# Corrige el manejo de fechas mezcladas (str/float/NaT) y estandariza a YYYY-MM-DD.
# ------------------------------------------------------------

from __future__ import annotations
import os
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


def build_reports(
    inv_gen: pd.DataFrame,
    inv_mkt: pd.DataFrame,
    sales_detail: pd.DataFrame,
    prod_detail: pd.DataFrame,
    out_dir: str | Path = "docs"
) -> None:
    """
    Genera:
      - docs/ventas_por_dia.csv  (fecha,cantidad,importe)
      - docs/ventas_por_item.csv (item,cantidad,importe)
      - docs/ventas_detalle.csv  (detalle consolidado)
      - docs/diario/YYYY-MM-DD-ventas.csv (detalle por día)
      - docs/produccion_detalle.csv (si hay producción)
      - Mercado (si corresponde) lo gestionas fuera de aquí

    Parámetros:
      inv_gen, inv_mkt: inventarios (no se usan aquí, pero se mantienen por compatibilidad)
      sales_detail: DataFrame con columnas al menos: fecha,item,cantidad,precio_unit,importe,descripcion,product_id
      prod_detail:  DataFrame con columnas al menos: fecha,item,cantidad,descripcion,product_id
    """
    out_dir = Path(out_dir)
    _ensure_dir(out_dir / "diario/file.txt")  # crea docs/diario/

    # -------- Normalización mínima de columnas esperadas --------
    sales_detail = (sales_detail or pd.DataFrame()).copy()
    prod_detail  = (prod_detail  or pd.DataFrame()).copy()

    # Asegura columnas básicas para ventas
    for col in ["fecha", "item", "cantidad", "precio_unit", "importe", "descripcion", "product_id", "payment"]:
        if col not in sales_detail.columns:
            sales_detail[col] = None

    # Tipos numéricos
    sales_detail["cantidad"]    = pd.to_numeric(sales_detail["cantidad"], errors="coerce").fillna(0).astype(int)
    sales_detail["precio_unit"] = pd.to_numeric(sales_detail["precio_unit"], errors="coerce")
    sales_detail["importe"]     = pd.to_numeric(sales_detail["importe"], errors="coerce")

    # Si importe viene vacío, lo calculamos
    mask_imp = sales_detail["importe"].isna()
    sales_detail.loc[mask_imp, "importe"] = (
        sales_detail.loc[mask_imp, "cantidad"].astype(float) * sales_detail.loc[mask_imp, "precio_unit"].astype(float)
    )

    # Producción (opcional)
    if not prod_detail.empty:
        for col in ["fecha", "item", "cantidad", "descripcion", "product_id"]:
            if col not in prod_detail.columns:
                prod_detail[col] = None
        prod_detail["cantidad"] = pd.to_numeric(prod_detail["cantidad"], errors="coerce").fillna(0).astype(int)

    # -------- Fechas únicas (corregido) --------
    dates = _collect_unique_dates(sales_detail.get("fecha"), prod_detail.get("fecha"))
    # Si no hay fechas, no generamos diarios (pero sí podemos generar agregados vacíos)
    # No cortamos la función: dejamos que los agregados escriban aunque estén vacíos.

    # -------- Guardar ventas_detalle.csv --------
    ventas_detalle_csv = out_dir / "ventas_detalle.csv"
    _ensure_dir(ventas_detalle_csv)
    # Ordena por fecha e item para consistencia
    if not sales_detail.empty:
        # Normalizamos 'fecha' a texto YYYY-MM-DD para salida
        f_norm = pd.to_datetime(sales_detail["fecha"].astype(str), errors="coerce").dt.strftime("%Y-%m-%d")
        sales_export = sales_detail.copy()
        sales_export["fecha"] = f_norm
        sales_export = sales_export.sort_values(["fecha", "item", "precio_unit"], na_position="last")
    else:
        sales_export = sales_detail.copy()
    sales_export.to_csv(ventas_detalle_csv, index=False)

    # -------- diarios por fecha --------
    for d in dates:
        day_rows = sales_export[sales_export["fecha"] == d]
        daily_path = out_dir / "diario" / f"{d}-ventas.csv"
        _ensure_dir(daily_path)
        day_rows.to_csv(daily_path, index=False)

    # -------- ventas_por_dia.csv --------
    vpd_path = out_dir / "ventas_por_dia.csv"
    if not sales_export.empty:
        vpd = (sales_export.groupby("fecha", as_index=False)
                          .agg(cantidad=("cantidad", "sum"),
                               importe=("importe", "sum")))
        vpd = vpd.sort_values("fecha")
    else:
        vpd = pd.DataFrame(columns=["fecha", "cantidad", "importe"])
    _ensure_dir(vpd_path)
    vpd.to_csv(vpd_path, index=False)

    # -------- ventas_por_item.csv --------
    vpi_path = out_dir / "ventas_por_item.csv"
    if not sales_export.empty:
        vpi = (sales_export.groupby("item", as_index=False)
                          .agg(cantidad=("cantidad", "sum"),
                               importe=("importe", "sum")))
        vpi = vpi.sort_values(["cantidad", "importe", "item"], ascending=[False, False, True])
    else:
        vpi = pd.DataFrame(columns=["item", "cantidad", "importe"])
    _ensure_dir(vpi_path)
    vpi.to_csv(vpi_path, index=False)

    # -------- produccion_detalle.csv (si aplica) --------
    prod_csv = out_dir / "produccion_detalle.csv"
    if not prod_detail.empty:
        f_norm = pd.to_datetime(prod_detail["fecha"].astype(str), errors="coerce").dt.strftime("%Y-%m-%d")
        prod_export = prod_detail.copy()
        prod_export["fecha"] = f_norm
        prod_export = prod_export.sort_values(["fecha", "item"], na_position="last")
        _ensure_dir(prod_csv)
        prod_export.to_csv(prod_csv, index=False)
    else:
        # Si prefieres mantener vacío sólo cuando exista alguno anterior, descomenta:
        # if prod_csv.exists(): prod_csv.unlink()
        pass
