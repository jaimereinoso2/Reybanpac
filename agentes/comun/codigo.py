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
        cat_lines = []
        for col in df.select_dtypes(include="object").columns:
            unique_vals = df[col].dropna().unique()
            if len(unique_vals) <= 25:
                vals_str = ", ".join(f'"{v}"' for v in sorted(unique_vals))
                cat_lines.append(f"    {col}: [{vals_str}]")
        if cat_lines:
            lines.append("  Valores únicos de columnas categóricas (usar EXACTAMENTE así en filtros):")
            lines.extend(cat_lines)
    return "\n".join(lines)
