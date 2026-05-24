"""
Specs: agentes/interpretador
Responsabilidad: tomar la pregunta original y el DataFrame resultado
y responder en lenguaje natural como un analista de negocio.
"""
import pandas as pd
import pytest
from unittest.mock import patch
from agentes.interpretador.interpretador import run, _MAX_FILAS


@pytest.fixture
def df_resultado():
    return pd.DataFrame({
        "Nombre_Unidad": ["Hacienda A", "Hacienda B", "Hacienda C"],
        "Costo_Ha": [320.5, 280.1, 410.7],
    })


@pytest.fixture
def df_grande():
    return pd.DataFrame({
        "Nombre_Unidad": [f"Hacienda {i}" for i in range(_MAX_FILAS + 10)],
        "Costo_Ha": [100.0 + i for i in range(_MAX_FILAS + 10)],
    })


class DescribeInterpretador:

    def it_retorna_una_respuesta_en_texto(self, df_resultado):
        respuesta_llm = "La hacienda con mayor costo es Hacienda C con 410.7, seguida de Hacienda A."

        with patch("agentes.interpretador.interpretador._llm", return_value=respuesta_llm):
            resultado = run("¿Cuál es el costo por hacienda?", df_resultado)

        assert isinstance(resultado, str)
        assert len(resultado) > 0

    def it_el_prompt_incluye_la_pregunta_original(self, df_resultado):
        pregunta = "¿Cuáles son las haciendas más caras?"
        llamadas = []

        with patch("agentes.interpretador.interpretador._llm", side_effect=lambda p: (llamadas.append(p) or "respuesta")):
            run(pregunta, df_resultado)

        assert pregunta in llamadas[0]

    def it_el_prompt_incluye_los_datos_del_dataframe(self, df_resultado):
        llamadas = []

        with patch("agentes.interpretador.interpretador._llm", side_effect=lambda p: (llamadas.append(p) or "respuesta")):
            run("pregunta", df_resultado)

        assert "Hacienda A" in llamadas[0]
        assert "320.5" in llamadas[0]

    def it_el_prompt_incluye_el_contexto_del_negocio(self, df_resultado):
        llamadas = []

        with patch("agentes.interpretador.interpretador._llm", side_effect=lambda p: (llamadas.append(p) or "respuesta")):
            run("pregunta", df_resultado)

        # la ontología menciona Reybanpac y banano
        assert "banano" in llamadas[0].lower() or "reybanpac" in llamadas[0].lower()

    def it_trunca_el_dataframe_cuando_supera_el_maximo_de_filas(self, df_grande):
        llamadas = []

        with patch("agentes.interpretador.interpretador._llm", side_effect=lambda p: (llamadas.append(p) or "respuesta")):
            run("pregunta", df_grande)

        assert str(_MAX_FILAS) in llamadas[0]
        assert str(len(df_grande)) in llamadas[0]

    def it_no_trunca_cuando_el_dataframe_tiene_pocas_filas(self, df_resultado):
        llamadas = []

        with patch("agentes.interpretador.interpretador._llm", side_effect=lambda p: (llamadas.append(p) or "respuesta")):
            run("pregunta", df_resultado)

        assert "Se muestran las primeras" not in llamadas[0]

    def it_puede_importarse_sin_cargar_el_csv(self):
        import agentes.interpretador.interpretador  # noqa: F401
