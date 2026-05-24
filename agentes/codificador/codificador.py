from agentes.comun.llm import _llm
from agentes.comun.codigo import _limpiar_codigo, _info_dfs
from agentes.comun.contexto import cargar_ontologia

SYSTEM_PROMPT = """Eres un experto en análisis de datos con pandas para Python.

Contexto del negocio:
{ontologia}

FECHA DE REFERENCIA (equivalente al "hoy"): {fecha_referencia}
Cualquier expresión temporal relativa ("últimos N meses", "último trimestre", "mes actual",
"año en curso", etc.) debe calcularse hacia atrás desde esta fecha en el código pandas,
NO desde la fecha real del sistema (no uses pd.Timestamp.now() ni datetime.today()).

---

DataFrames disponibles en este momento:
{info_dfs}

Pregunta original del usuario: "{pregunta}"

Genera el código pandas para realizar esta operación:
"{actividad}"

Responde en este formato exacto:

RAZONAMIENTO:
<explica qué operación realizas, en 2-4 oraciones>

CODIGO:
<solo el código Python/pandas>

Reglas:
- Los DataFrames de entrada son de solo lectura
- El resultado final DEBE quedar en `{output_var}` como pandas DataFrame
- Si usas .groupby(), termina con .reset_index()
- Si obtienes una Series, conviértela con .reset_index() o pd.DataFrame()
- Para filtrar por mes usa `FECHA_mes` (int 1-12; octubre = 10, enero = 1, etc.); para filtrar por año usa `FECHA_ano` (int, ej. 2024); NO uses .dt.month ni .dt.year cuando estas columnas ya existen
- Solo pandas, sin imports adicionales
- Sin markdown, sin backticks"""


def run(actividad: str, pregunta: str, numero_paso: int, dfs_disponibles: dict, fecha_referencia: str) -> tuple[str, str]:
    output_var = f"df_paso{numero_paso}"
    prompt = SYSTEM_PROMPT.format(
        ontologia=cargar_ontologia(),
        fecha_referencia=fecha_referencia,
        info_dfs=_info_dfs(dfs_disponibles),
        pregunta=pregunta,
        actividad=actividad,
        output_var=output_var,
    )
    texto = _llm(prompt)
    razonamiento, codigo = "", ""
    if "CODIGO:" in texto:
        partes = texto.split("CODIGO:", 1)
        razonamiento = partes[0].replace("RAZONAMIENTO:", "").strip()
        codigo = _limpiar_codigo(partes[1])
    else:
        codigo = _limpiar_codigo(texto)
    return razonamiento, codigo
