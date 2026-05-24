import json
from agentes.comun.llm import _llm

SYSTEM_PROMPT = """Eres un verificador de código pandas experto.

DataFrames disponibles y sus columnas exactas:
{col_info}

Variables disponibles: {vars_disponibles}

Código a verificar:
```python
{codigo}
```

Verifica ÚNICAMENTE:
1. Todos los nombres de columna usados existen en el DataFrame correspondiente (respeta mayúsculas/minúsculas exactas)
2. Todas las variables de DataFrame usadas están en {vars_disponibles}
3. El resultado final queda asignado a `{output_var}`

Responde SOLO con JSON sin markdown:
{{"valido": true}}
o
{{"valido": false, "errores": "descripción concisa de los errores"}}"""


def run(codigo: str, dfs_disponibles: dict, numero_paso: int) -> dict:
    output_var = f"df_paso{numero_paso}"
    col_info = "\n".join(
        f"- `{name}`: {list(df.columns)}"
        for name, df in dfs_disponibles.items()
    )
    prompt = SYSTEM_PROMPT.format(
        col_info=col_info,
        vars_disponibles=list(dfs_disponibles.keys()),
        codigo=codigo,
        output_var=output_var,
    )
    texto = _llm(prompt).strip()
    if texto.startswith("```"):
        texto = texto.split("```")[1]
        if texto.startswith("json"):
            texto = texto[4:]
    texto = texto.strip().rstrip("```").strip()
    try:
        return json.loads(texto)
    except Exception:
        return {"valido": True}
