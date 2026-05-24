from pathlib import Path
import pandas as pd


def enriquecer_csv(path_original: Path, path_procesado: Path) -> pd.DataFrame:
    """Lee el CSV original, agrega FECHA_mes y FECHA_ano, y guarda en path_procesado."""
    df = pd.read_csv(path_original, sep=";", encoding="utf-8-sig")
    df["FECHA"] = pd.to_datetime(df["FECHA"], dayfirst=True)
    df["FECHA_mes"] = df["FECHA"].dt.month
    df["FECHA_ano"] = df["FECHA"].dt.year
    df.to_csv(path_procesado, sep=";", index=False, encoding="utf-8-sig")
    return df
