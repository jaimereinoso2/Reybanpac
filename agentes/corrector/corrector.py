from agentes.comun.llm import _llm
from agentes.comun.codigo import _limpiar_codigo

SYSTEM_PROMPT = """Eres un experto corrector de código pandas.

DataFrames disponibles y sus columnas exactas:
{col_info}

El siguiente código tiene estos errores:
{errores}

Código con errores:
```python
{codigo}
```

Actividad: "{actividad}"
Pregunta original: "{pregunta}"

Corrige el código para resolver los errores.
Reglas:
- El resultado final DEBE quedar en `{output_var}` como pandas DataFrame
- Usa solo las columnas y variables listadas arriba
- Sin markdown, sin backticks — devuelve SOLO el código corregido"""


def run(codigo: str, errores: str, dfs_disponibles: dict, actividad: str, pregunta: str, numero_paso: int) -> str:
    output_var = f"df_paso{numero_paso}"
    col_info = "\n".join(
        f"- `{name}`: {list(df.columns)}"
        for name, df in dfs_disponibles.items()
    )
    prompt = SYSTEM_PROMPT.format(
        col_info=col_info,
        errores=errores,
        codigo=codigo,
        actividad=actividad,
        pregunta=pregunta,
        output_var=output_var,
    )
    return _limpiar_codigo(_llm(prompt))
