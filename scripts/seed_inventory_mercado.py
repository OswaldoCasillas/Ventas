#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
INV_GENERAL = DATA / "inventory.csv"
INV_MERCADO = DATA / "inventory_mercado.csv"

# === STOCK MERCADO (según tu lista) ===
# OJO: los SKUs coinciden con los que ya te generé.
STOCK_MKT = {
    "PALETA-AGUA-ALGODON": 0,
    "PALETA-CREMA-ARROZ": 3,
    "PALETA-CREMA-BAILEYS": 3,
    "PALETA-CREMA-BESO-D-ANGEL": 0,
    "PALETA-CREMA-CAJETA": 2,
    "PALETA-CREMA-CAPUCHINO": 2,
    "PALETA-CREMA-CHICLE": 2,
    "PALETA-CREMA-CHOCOLATE": 4,
    "PALETA-CREMA-CHONGOS": 2,
    "PALETA-CREMA-COCO": 5,
    "PALETA-CREMA-DURAZNO-CREMA": 2,
    "PALETA-CREMA-FERRERO": 5,
    "PALETA-AGUA-FRESA": 5,
    "PALETA-CREMA-FRESA": 0,
    "PALETA-CREMA-FRESAS-CREAM": 5,
    "PALETA-AGUA-FRUTOS-ROJOS": 2,
    "PALETA-AGUA-GROSELLA": 8,
    "PALETA-AGUA-GUANABANA": 3,
    "PALETA-AGUA-GUAYABA": 3,
    "PALETA-AGUA-KIWI": 0,
    "PALETA-CREMA-KAHLUA": 0,
    "PALETA-AGUA-LIMON": 8,
    "PALETA-AGUA-LIMON-CHAMOY": 0,
    "PALETA-CREMA-MAMEY": 3,
    "PALETA-AGUA-MANDARINA": 3,
    "PALETA-AGUA-MANGO": 4,
    "PALETA-AGUA-MANGO-CHAMOY": 8,
    "PALETA-AGUA-MARACUYA": 3,
    "PALETA-CREMA-MAZAPAN": 3,
    "PALETA-AGUA-MELON": 2,
    "PALETA-CREMA-MENTA-CON-CH": 3,
    "PALETA-CREMA-MOCTEZUMA": 0,
    "PALETA-CREMA-NUEZ": 4,
    "PALETA-CREMA-NUTELLA": 0,
    "PALETA-CREMA-OREO": 4,
    "PALETA-CREMA-PAY-DE-LIMON": 0,
    "PALETA-AGUA-PERRO": 3,
    "PALETA-AGUA-PICAFRESA": 4,
    "PALETA-AGUA-PICAPANDAS": 4,
    "PALETA-AGUA-PINA": 2,
    "PALETA-AGUA-PINA-CHAMOY": 2,
    "PALETA-CREMA-PINON": 3,
    "PALETA-CREMA-PISTACHE": 3,
    "PALETA-AGUA-PULPARINDO": 2,
    "PALETA-CREMA-QUESO": 1,
    "PALETA-CREMA-QUESO-ZARZA": 3,
    "PALETA-CREMA-RAFFAELLO": 2,
    "PALETA-CREMA-ROMPOPE": 0,
    "PALETA-CREMA-RON-PASAS": 0,
    "PALETA-CREMA-SALTED-CARAMEL": 0,
    "PALETA-AGUA-SANDIA": 2,
    "PALETA-AGUA-SANDIA-CHAMOY": 2,
    "PALETA-CREMA-SELVA-BLANCA": 2,
    "PALETA-CREMA-SELVA-NEGRA": 5,
    "PALETA-CREMA-SNICKERS": 0,
    "PALETA-AGUA-TAMARINDO": 3,
    "PALETA-CREMA-TARO": 2,
    "PALETA-CREMA-TRIX": 0,
    "PALETA-AGUA-UVA": 0,
    "PALETA-CREMA-VAINILLA": 0,
    "PALETA-CREMA-VINO": 0,
    "PALETA-CREMA-ZAPOTE": 3,
    "PALETA-CREMA-PERLA-NEGRA": 0,
    "PALETA-CREMA-PINA-COLADA": 0,
    "PALETA-CREMA-TEQUILA": 0,
    "PALETA-SIN-AZUCAR-CAJETA-SA": 0,
    "PALETA-SIN-AZUCAR-CAPUKETO": 1,
    "PALETA-SIN-AZUCAR-CHOCOFRESA-K": 0,
    "PALETA-SIN-AZUCAR-CHOCOKETO": 1,
    "PALETA-SIN-AZUCAR-COCOKETO": 2,
    "PALETA-SIN-AZUCAR-FERREROKETO": 0,
    "PALETA-SIN-AZUCAR-FRESA-SA": 0,
    "PALETA-SIN-AZUCAR-FRUTOSKETO": 2,
    "PALETA-SIN-AZUCAR-GROSELLA-SA": 0,
    "PALETA-SIN-AZUCAR-GUANABANASA": 2,
    "PALETA-SIN-AZUCAR-LIMONCHM-SA": 0,
    "PALETA-AGUA-LIMON-MENTA": 0,
    "PALETA-SIN-AZUCAR-MAMEY-SA": 0,
    "PALETA-SIN-AZUCAR-MANGO-SA": 0,
    "PALETA-SIN-AZUCAR-MAZAKETO": 0,
    "PALETA-SIN-AZUCAR-MENTAKETO": 0,
    "PALETA-SIN-AZUCAR-NUEZKETO": 2,
    "PALETA-SIN-AZUCAR-PINONKETO": 2,
    "PALETA-SIN-AZUCAR-PISTAKETO": 2,
    "PALETA-SIN-AZUCAR-VAINILLAKETO": 0,
}

def main():
    if not INV_GENERAL.exists():
        raise SystemExit("ERROR: data/inventory.csv no existe. Súbelo primero.")

    inv = pd.read_csv(INV_GENERAL)
    # normaliza tipos
    inv["item"] = inv["item"].astype(str)
    if "descripcion" not in inv.columns: inv["descripcion"] = ""
    if "precio" not in inv.columns: inv["precio"] = ""

    # 1) Paletas del inventario general
    paletas = inv[inv["item"].str.startswith("PALETA-")].copy()
    if paletas.empty:
        print("WARN: No encontré paletas en inventory.csv (items que inician con 'PALETA-').")

    # 2) Arrancamos inventorío Mercado con las paletas del general (stock=0)
    mkt = paletas[["item","descripcion","precio"]].copy()
    mkt["stock"] = 0

    # 3) Sobrescribe stock con lo de tu lista
    missing = []
    for sku, stk in STOCK_MKT.items():
        if (mkt["item"] == sku).any():
            mkt.loc[mkt["item"] == sku, "stock"] = int(stk)
        else:
            # SKU no existe en el general → lo añadimos sin precio (te avisamos)
            missing.append(sku)
            mkt = pd.concat([
                mkt,
                pd.DataFrame([{"item": sku, "descripcion": "", "precio": "", "stock": int(stk)}])
            ], ignore_index=True)

    # 4) Orden bonito por item
    mkt = mkt[["item","descripcion","stock","precio"]].sort_values("item").reset_index(drop=True)

    # 5) Escribe archivo
    DATA.mkdir(parents=True, exist_ok=True)
    mkt.to_csv(INV_MERCADO, index=False, encoding="utf-8")
    print(f"OK: Escribí {INV_MERCADO} con {len(mkt)} filas.")

    if missing:
        print("\nAVISO: Estos SKUs no estaban en inventory.csv y se agregaron SIN precio.")
        for s in missing:
            print(" -", s)
        print("Tip: agrega esos SKUs al inventario general para heredar descripción y precio.")

if __name__ == "__main__":
    main()
