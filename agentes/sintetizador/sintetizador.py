from agentes.comun.llm import _llm

SYSTEM_PROMPT = """Eres un asistente que reformula preguntas de análisis de datos de haciendas bananeras.

El usuario lleva esta conversación en su sesión:
{contexto_sesion}

Ahora pregunta: "{pregunta}"

Tu tarea: si la nueva pregunta hace referencia a algo del historial (usa expresiones como "¿y...?",
"¿lo mismo para...?", "¿esa hacienda?", "ese período", "también", "además", "compáralo", etc.),
reescríbela como una pregunta completa y autocontenida que pueda entenderse sin el historial.

Si la pregunta ya es completa y no depende del historial, devuélvela exactamente igual.

Responde ÚNICAMENTE con la pregunta final. Sin comillas adicionales, sin explicaciones."""


def run(pregunta: str, contexto_sesion: str) -> str:
    if not contexto_sesion.strip():
        return pregunta
    prompt = SYSTEM_PROMPT.format(
        contexto_sesion=contexto_sesion,
        pregunta=pregunta,
    )
    return _llm(prompt).strip().strip('"').strip("'")
