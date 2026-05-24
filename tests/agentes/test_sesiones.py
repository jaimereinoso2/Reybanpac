"""
Specs: agentes/comun/sesiones
Prueba la lógica de sesiones en memoria sin llamar al LLM real.
"""
import pytest
from unittest.mock import patch
from agentes.comun.sesiones import (
    crear_sesion, cambiar_sesion, listar_sesiones, sesion_activa_id,
    agregar_interaccion, obtener_contexto, obtener_sesion, renombrar_sesion,
    _MAX_RECIENTES, _reset,
)


@pytest.fixture(autouse=True)
def estado_limpio():
    """Resetea el estado en memoria antes de cada test."""
    _reset()


class DescribeSesiones:

    def it_crea_la_primera_sesion_con_nombre_inicial(self):
        sid = crear_sesion()
        sesion = obtener_sesion(sid)
        assert sesion["nombre"] == "Sesión inicial"
        assert sesion["interacciones"] == []
        assert sesion["resumen"] is None

    def it_la_segunda_sesion_tiene_nombre_numerico(self):
        crear_sesion()
        sid2 = crear_sesion()
        assert obtener_sesion(sid2)["nombre"] == "Sesión 2"

    def it_acepta_nombre_personalizado(self):
        sid = crear_sesion("Análisis de costos Q1")
        assert obtener_sesion(sid)["nombre"] == "Análisis de costos Q1"

    def it_la_sesion_creada_queda_activa(self):
        sid = crear_sesion()
        assert sesion_activa_id() == sid

    def it_cambiar_sesion_actualiza_la_activa(self):
        sid1 = crear_sesion()
        sid2 = crear_sesion()
        cambiar_sesion(sid1)
        assert sesion_activa_id() == sid1

    def it_renombrar_sesion_actualiza_el_nombre(self):
        sid = crear_sesion()
        renombrar_sesion(sid, "Mi análisis")
        assert obtener_sesion(sid)["nombre"] == "Mi análisis"

    def it_listar_sesiones_devuelve_todas(self):
        crear_sesion()
        crear_sesion()
        crear_sesion()
        assert len(listar_sesiones()) == 3

    def it_agregar_interaccion_guarda_pregunta_e_interpretacion(self):
        sid = crear_sesion()
        agregar_interaccion(sid, "¿Costo promedio?", "El costo promedio fue $120.")
        sesion = obtener_sesion(sid)
        assert len(sesion["interacciones"]) == 1
        assert sesion["interacciones"][0]["pregunta"] == "¿Costo promedio?"

    def it_obtener_contexto_vacio_si_no_hay_interacciones(self):
        sid = crear_sesion()
        assert obtener_contexto(sid) == ""

    def it_obtener_contexto_incluye_preguntas_recientes(self):
        sid = crear_sesion()
        agregar_interaccion(sid, "¿Zona más cara?", "La zona Norte es la más cara.")
        ctx = obtener_contexto(sid)
        assert "¿Zona más cara?" in ctx
        assert "Norte" in ctx

    def it_no_llama_al_llm_mientras_no_supere_el_maximo(self):
        sid = crear_sesion()
        with patch("agentes.comun.sesiones._llm") as mock_llm:
            for i in range(_MAX_RECIENTES):
                agregar_interaccion(sid, f"Pregunta {i}", f"Respuesta {i}")
            mock_llm.assert_not_called()

    def it_llama_al_llm_para_resumir_al_superar_el_maximo(self):
        sid = crear_sesion()
        with patch("agentes.comun.sesiones._llm", return_value="Resumen generado.") as mock_llm:
            for i in range(_MAX_RECIENTES + 1):
                agregar_interaccion(sid, f"Pregunta {i}", f"Respuesta {i}")
            mock_llm.assert_called_once()

    def it_mantiene_solo_las_ultimas_recientes_tras_resumir(self):
        sid = crear_sesion()
        with patch("agentes.comun.sesiones._llm", return_value="Resumen."):
            for i in range(_MAX_RECIENTES + 1):
                agregar_interaccion(sid, f"Pregunta {i}", f"Respuesta {i}")
        sesion = obtener_sesion(sid)
        assert len(sesion["interacciones"]) == _MAX_RECIENTES
        assert sesion["resumen"] == "Resumen."

    def it_el_contexto_incluye_resumen_cuando_existe(self):
        sid = crear_sesion()
        with patch("agentes.comun.sesiones._llm", return_value="Resumen previo de costos."):
            for i in range(_MAX_RECIENTES + 1):
                agregar_interaccion(sid, f"Pregunta {i}", f"Respuesta {i}")
        ctx = obtener_contexto(sid)
        assert "Resumen previo de costos." in ctx
