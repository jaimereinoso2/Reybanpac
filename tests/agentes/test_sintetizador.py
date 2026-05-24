"""
Specs: agentes/sintetizador
Prueba el agente de forma aislada — sin llamar al LLM real.
"""
from unittest.mock import patch
from agentes.sintetizador.sintetizador import run


_CONTEXTO = (
    "Últimas preguntas en esta sesión:\n"
    "1. Pregunta: \"¿Cuáles son las 5 haciendas con mayor costo por hectárea en 2024?\"\n"
    "   Hallazgo: Las haciendas más caras fueron La Esmeralda, San José, El Palmar, "
    "La Victoria y Santa Rosa, con costos entre $180 y $220 por hectárea."
)


class DescribeSintetizador:

    def it_devuelve_la_pregunta_sin_cambios_si_no_hay_contexto(self):
        pregunta = "¿Cuál es el costo promedio por hectárea en 2024?"
        resultado = run(pregunta, "")
        assert resultado == pregunta

    def it_no_llama_al_llm_si_no_hay_contexto(self):
        with patch("agentes.sintetizador.sintetizador._llm") as mock_llm:
            run("¿Costo promedio en 2024?", "")
            mock_llm.assert_not_called()

    def it_llama_al_llm_cuando_hay_contexto(self):
        pregunta_expandida = "¿Cuál es el costo promedio por hectárea de las 5 haciendas más caras en 2024 en la zona Norte?"
        with patch("agentes.sintetizador.sintetizador._llm", return_value=pregunta_expandida) as mock_llm:
            resultado = run("¿y en la zona Norte?", _CONTEXTO)
            mock_llm.assert_called_once()
            assert resultado == pregunta_expandida

    def it_el_prompt_incluye_el_contexto_y_la_pregunta(self):
        llamadas = []
        with patch("agentes.sintetizador.sintetizador._llm", side_effect=lambda p: (llamadas.append(p) or "pregunta expandida")):
            run("¿y en la zona Norte?", _CONTEXTO)
        assert _CONTEXTO in llamadas[0]
        assert "¿y en la zona Norte?" in llamadas[0]

    def it_elimina_comillas_extras_de_la_respuesta(self):
        with patch("agentes.sintetizador.sintetizador._llm", return_value='"¿Pregunta limpia?"'):
            resultado = run("¿pregunta?", _CONTEXTO)
        assert resultado == "¿Pregunta limpia?"

    def it_puede_importarse_sin_cargar_el_csv(self):
        import agentes.sintetizador.sintetizador  # noqa: F401
