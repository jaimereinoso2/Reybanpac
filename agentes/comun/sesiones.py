import uuid
from datetime import datetime
from agentes.comun.llm import _llm

_MAX_RECIENTES = 5

_store: dict = {"sesiones": {}, "sesion_activa": None}


def _reset():
    """Limpia el estado en memoria — usado solo en tests."""
    _store["sesiones"].clear()
    _store["sesion_activa"] = None


def listar_sesiones() -> list[dict]:
    return list(_store["sesiones"].values())


def sesion_activa_id() -> str | None:
    return _store.get("sesion_activa")


def obtener_sesion(sid: str) -> dict | None:
    return _store["sesiones"].get(sid)


def crear_sesion(nombre: str = None) -> str:
    sid = str(uuid.uuid4())[:8]
    if nombre is None:
        n = len(_store["sesiones"]) + 1
        nombre = "Sesión inicial" if n == 1 else f"Sesión {n}"
    _store["sesiones"][sid] = {
        "id": sid,
        "nombre": nombre,
        "creada": datetime.now().isoformat(),
        "interacciones": [],
        "resumen": None,
    }
    _store["sesion_activa"] = sid
    return sid


def cambiar_sesion(sid: str):
    _store["sesion_activa"] = sid


def renombrar_sesion(sid: str, nuevo_nombre: str):
    if sid in _store["sesiones"]:
        _store["sesiones"][sid]["nombre"] = nuevo_nombre.strip() or _store["sesiones"][sid]["nombre"]


def agregar_interaccion(sid: str, pregunta: str, interpretacion: str, df=None):
    sesion = _store["sesiones"].get(sid)
    if not sesion:
        return

    datos_str = None
    if df is not None and not df.empty:
        raw = df.to_string(index=False, max_rows=25)
        datos_str = raw[:2500] + "\n...(truncado)" if len(raw) > 2500 else raw

    sesion["interacciones"].append({
        "pregunta": pregunta,
        "interpretacion": interpretacion,
        "datos": datos_str,
        "timestamp": datetime.now().isoformat(),
    })

    if len(sesion["interacciones"]) > _MAX_RECIENTES:
        a_resumir = sesion["interacciones"][:-_MAX_RECIENTES]
        recientes = sesion["interacciones"][-_MAX_RECIENTES:]

        texto = "\n".join(
            f"- Pregunta: {i['pregunta']}\n  Respuesta: {i['interpretacion']}"
            for i in a_resumir
        )
        resumen_anterior = sesion.get("resumen") or ""
        prompt = (
            "Resume en máximo 150 palabras las siguientes interacciones de análisis de datos "
            "de haciendas bananeras. Captura los temas consultados y hallazgos clave para "
            "servir de contexto a preguntas futuras. Solo el resumen, sin encabezados."
            + (f"\n\nResumen previo:\n{resumen_anterior}" if resumen_anterior else "")
            + f"\n\nInteracciones:\n{texto}"
        )
        sesion["resumen"] = _llm(prompt)
        sesion["interacciones"] = recientes


def obtener_contexto(sid: str) -> str:
    sesion = obtener_sesion(sid)
    if not sesion:
        return ""
    partes = []
    if sesion.get("resumen"):
        partes.append(f"Resumen de preguntas anteriores en esta sesión:\n{sesion['resumen']}")
    if sesion["interacciones"]:
        bloques = []
        for i, inter in enumerate(sesion["interacciones"]):
            bloque = (
                f"{i+1}. Pregunta: \"{inter['pregunta']}\"\n"
                f"   Hallazgo: {inter['interpretacion'][:300]}"
            )
            if inter.get("datos"):
                bloque += f"\n   Datos del resultado:\n{inter['datos']}"
            bloques.append(bloque)
        partes.append(f"Últimas preguntas en esta sesión:\n" + "\n\n".join(bloques))
    return "\n\n".join(partes)
