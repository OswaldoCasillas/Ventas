import os
import re
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # repo root
DATA = ROOT / "data"
DOCS = ROOT / "docs"
MERC = DOCS / "mercado"

DOCS.mkdir(exist_ok=True, parents=True)
MERC.mkdir(exist_ok=True, parents=True)

# Entradas esperadas (ya las usas)
SALES_MAIN = DATA / "sales.csv"             # txn_id,fecha,item,cantidad,precio_unit,importe,issue,(labels? opcional)
SALES_MERC = DATA / "sales_mercado.csv"     # idem

# Salidas
DETALLE_MAIN = DOCS / "ventas_detalle.csv"
DETALLE_MERC = MERC / "ventas_detalle.csv"
POR_DIA      = DOCS / "ventas_por_dia.csv"

CARD_PATTERNS = [
    re.compile(r"\(TARJETA\)", re.I),
    re.compile(r"\*\*Método de pago\*\*:\s*Tarjeta", re.I),
    re.compile(r"\[TARJETA\]", re.I)
]

def is_card(row):
    # Si tu extracción desde issues incluyó labels:
    labels = (row.get("labels") or "").lower()
    if "pago-tarjeta" in labels:
        return True

    title = str(row.get("title", ""))  # si existe
    body  = str(row.get("body", ""))   # si existe
    issue = str(row.get("issue", ""))  # por compat: algunos parsers guardan cuerpo en 'issue'

    text = " ".join([title, body, issue])
    return any(p.search(text) for p in CARD_PATTERNS)

def load_sales_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["txn_id","fecha","item","cantidad","precio_unit","importe","issue"])
    df = pd.read_csv(path, dtype=str).fillna("")
    # Normaliza numéricos
    for col in ["cantidad","precio_unit","importe"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            df[col] = 0
    # Método de pago
    if "metodo_pago" not in df.columns:
        df["metodo_pago"] = df.apply(lambda r: "tarjeta" if is_card(r) else "efectivo", axis=1)
    return df

def write_detalle(df: pd.DataFrame, outpath: Path):
    cols = ["txn_id","fecha","item","cantidad","precio_unit","importe","metodo_pago","issue"]
    for c in cols:
        if c not in df.columns:
            df[c] = "" if c in ("txn_id","fecha","item","issue") else 0
    df = df[cols].copy()
    df.sort_values(["fecha","item"], inplace=True)
    df.to_csv(outpath, index=False)

def make_por_dia(df_all: pd.DataFrame) -> pd.DataFrame:
    if df_all.empty:
        return pd.DataFrame(columns=[
            "fecha","total_unidades","total_importe",
            "unidades_efectivo","importe_efectivo",
            "unidades_tarjeta","importe_tarjeta"
        ])
    # Asegura columnas
    for c in ("cantidad","importe"):
        if c not in df_all.columns:
            df_all[c] = 0
    if "metodo_pago" not in df_all.columns:
        df_all["metodo_pago"] = df_all.apply(lambda r: "tarjeta" if is_card(r) else "efectivo", axis=1)

    def agg(df):
        t_u = df["cantidad"].sum()
        t_i = df["importe"].sum()
        ef  = df[df["metodo_pago"]=="efectivo"]
        tj  = df[df["metodo_pago"]=="tarjeta"]
        return pd.Series({
            "total_unidades": t_u,
            "total_importe":  t_i,
            "unidades_efectivo": ef["cantidad"].sum(),
            "importe_efectivo":  ef["importe"].sum(),
            "unidades_tarjeta":  tj["cantidad"].sum(),
            "importe_tarjeta":   tj["importe"].sum(),
        })

    out = df_all.groupby("fecha", as_index=False).apply(agg).reset_index(drop=True)
    out = out[[
        "fecha","total_unidades","total_importe",
        "unidades_efectivo","importe_efectivo",
        "unidades_tarjeta","importe_tarjeta"
    ]]
    out.sort_values("fecha", inplace=True)
    return out

def main():
    df_main = load_sales_csv(SALES_MAIN)
    df_merc = load_sales_csv(SALES_MERC)

    # Escribe detalle con metodo_pago
    write_detalle(df_main, DETALLE_MAIN)
    write_detalle(df_merc, DETALLE_MERC)

    # Por día (consolida ambos)
    both = pd.concat([df_main, df_merc], ignore_index=True)
    por_dia = make_por_dia(both)
    por_dia.to_csv(POR_DIA, index=False)

if __name__ == "__main__":
    main()
