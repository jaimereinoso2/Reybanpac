import json
from agentes.comun.llm import _llm, _cfg
from agentes.comun.contexto import cargar_schema_depurado

SYSTEM_PROMPT = """Eres un experto en análisis de datos con pandas para Python.

Tienes un DataFrame global llamado `DF_GLOBAL`. Su estructura y columnas son:

{schema}

La columna FECHA es datetime64.
Las columnas `FECHA_mes` (int, 1-12) y `FECHA_ano` (int, ej. 2024) están siempre disponibles.
Úsalas para cualquier filtro o agrupación por mes o año — son más simples y seguras que operar sobre FECHA.
Ejemplos: octubre → FECHA_mes == 10; año 2024 → FECHA_ano == 2024; agrupar por mes → groupby("FECHA_mes").

FECHA DE REFERENCIA (equivalente al "hoy"): {fecha_referencia}
Esta es la fecha del último mes disponible en los datos. Cualquier expresión temporal
relativa del usuario ("últimos 6 meses", "último trimestre", "mes actual", "año en curso",
etc.) debe calcularse hacia atrás desde esta fecha, NO desde la fecha real del sistema.
Ejemplo: si la fecha de referencia es 2025-01-01, "los últimos 6 meses" es agosto 2024 – enero 2025.

El usuario pregunta: "{pregunta}"

Genera un plan donde CADA paso:
- Produce exactamente un nuevo DataFrame llamado `df_pasoN` (N = número del paso)
- Es una de estas operaciones:
  a) Filtrar/seleccionar/transformar desde `DF_GLOBAL` o desde un `df_pasoX` anterior
  b) Operar entre dos o más DataFrames anteriores (merge, concat, cálculos cruzados)

El paso 1 SIEMPRE debe leer de `DF_GLOBAL`.

CRÍTICO — valores de texto en filtros: cuando el plan incluya un filtro por un valor de texto
(nombre de zona, hacienda, región, etc.), copia ese valor EXACTAMENTE como aparece en la pregunta
del usuario, sin abreviar, partir ni modificar ninguna palabra.
Ejemplo: si la pregunta dice "Zona Fumisa", el paso debe decir `filtrar donde Zona == 'Zona Fumisa'`,
NUNCA `Zona == 'Fumisa'` ni `Zona == 'zona fumisa'`.

Responde ÚNICAMENTE con un JSON array de strings, sin markdown. Ejemplo:
[
  "df_paso1: Seleccionar de DF_GLOBAL las columnas Nombre_Unidad, Costo_Ha, Total_Cajas, FECHA filtrando año 2024",
  "df_paso2: Calcular en df_paso1 el costo_por_caja = Costo_Ha / Total_Cajas",
  "df_paso3: Ordenar df_paso2 por costo_por_caja descendente y tomar top 5"
]"""


def run(pregunta: str, fecha_referencia: str) -> list[str]:
    prompt = SYSTEM_PROMPT.format(
        schema=cargar_schema_depurado(),
        pregunta=pregunta,
        fecha_referencia=fecha_referencia,
    )
    texto = _llm(prompt, model=_cfg("GEMINI_MODEL_REASONING", "gemini-2.5-pro"))
    if texto.startswith("```"):
        texto = texto.split("```")[1]
        if texto.startswith("json"):
            texto = texto[4:]
    return json.loads(texto.strip().rstrip("```").strip())
