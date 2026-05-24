import pandas as pd


def _limpiar_codigo(texto: str) -> str:
    texto = texto.strip()
    if texto.startswith("```"):
        texto = texto.split("```")[1]
        if texto.startswith("python"):
            texto = texto[6:]
    return texto.strip()


def _info_dfs(dfs: dict) -> str:
    lines = []
    for name, df in dfs.items():
        lines.append(f"DataFrame `{name}`: {df.shape[0]} filas × {df.shape[1]} columnas")
        lines.append(f"  Columnas: {list(df.columns)}")
        lines.append(f"  Tipos: {df.dtypes.to_dict()}")
        lines.append(f"  Muestra:\n{df.head(3).to_string(index=False)}")
    return "\n".join(lines)
