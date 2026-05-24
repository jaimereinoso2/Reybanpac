import pandas as pd
from agentes.comun.llm import _llm
from agentes.comun.contexto import cargar_ontologia

SYSTEM_PROMPT = """Eres un analista de negocio experto en el sector bananero.

Contexto del negocio:
{ontologia}

---

Un usuario hizo esta pregunta:
"{pregunta}"

Para responderla se ejecutó un análisis de datos que produjo el siguiente resultado:

{datos}

Responde la pregunta del usuario de forma clara y directa, como lo haría un analista de negocio:
- Usa los datos del resultado para fundamentar tu respuesta
- Destaca los valores más relevantes (máximos, mínimos, tendencias, diferencias notables)
- Usa lenguaje de negocio, no de programación
- Sé conciso: máximo 5 oraciones
- Responde en español
- CRÍTICO: cuando menciones nombres de regiones, zonas, haciendas u otros valores categóricos, cópialos EXACTAMENTE como aparecen en los datos sin abreviar, reformular ni quitar palabras (p. ej. si la tabla dice "ZONA A", escribe "ZONA A", no "A" ni "zona a" ni "región A")"""

_MAX_FILAS = 50


def run(pregunta: str, df: pd.DataFrame) -> str:
    if len(df) > _MAX_FILAS:
        muestra = df.head(_MAX_FILAS).to_string(index=False)
        nota = f"\n(Se muestran las primeras {_MAX_FILAS} filas de {len(df)} totales)"
    else:
        muestra = df.to_string(index=False)
        nota = ""

    prompt = SYSTEM_PROMPT.format(
        ontologia=cargar_ontologia(),
        pregunta=pregunta,
        datos=muestra + nota,
    )
    return _llm(prompt)
