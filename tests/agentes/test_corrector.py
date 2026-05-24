"""
Specs: agentes/corrector
Prueba el agente de forma aislada — sin cargar CSV ni llamar a la API real.
"""
import pandas as pd
import pytest
from unittest.mock import patch
from agentes.corrector.corrector import run


@pytest.fixture
def dfs_simple():
    df = pd.DataFrame({"Zona": ["Norte"], "Costo_Ha": [100.0]})
    return {"DF_GLOBAL": df}


class DescribeCorrector:

    def it_retorna_codigo_corregido_sin_backticks(self, dfs_simple):
        codigo_corregido = "df_paso1 = DF_GLOBAL[['Zona', 'Costo_Ha']]"

        with patch("agentes.corrector.corrector._llm", return_value=codigo_corregido):
            resultado = run(
                codigo="df_paso1 = DF_GLOBAL[['zona', 'costo_ha']]",
                errores="columnas en minúsculas no existen",
                dfs_disponibles=dfs_simple,
                actividad="seleccionar columnas",
                pregunta="comparar costos",
                numero_paso=1,
            )

        assert "```" not in resultado
        assert "df_paso1" in resultado

    def it_el_prompt_incluye_el_codigo_con_errores(self, dfs_simple):
        llamadas = []
        codigo_malo = "df_paso1 = DF_GLOBAL[['zona']]"

        with patch("agentes.corrector.corrector._llm", side_effect=lambda p: (llamadas.append(p) or "df_paso1 = DF_GLOBAL[['Zona']]")):
            run(codigo_malo, "columna incorrecta", dfs_simple, "actividad", "pregunta", 1)

        assert codigo_malo in llamadas[0]

    def it_el_prompt_incluye_la_descripcion_de_los_errores(self, dfs_simple):
        llamadas = []
        error = "columna 'zona' debe ser 'Zona'"

        with patch("agentes.corrector.corrector._llm", side_effect=lambda p: (llamadas.append(p) or "df_paso1 = DF_GLOBAL[['Zona']]")):
            run("df_paso1 = DF_GLOBAL[['zona']]", error, dfs_simple, "actividad", "pregunta", 1)

        assert error in llamadas[0]

    def it_el_prompt_incluye_las_columnas_disponibles(self, dfs_simple):
        llamadas = []

        with patch("agentes.corrector.corrector._llm", side_effect=lambda p: (llamadas.append(p) or "df_paso1 = DF_GLOBAL[['Zona']]")):
            run("df_paso1 = DF_GLOBAL.copy()", "error", dfs_simple, "actividad", "pregunta", 1)

        assert "Zona" in llamadas[0]
        assert "Costo_Ha" in llamadas[0]

    def it_puede_importarse_sin_cargar_el_csv(self):
        import agentes.corrector.corrector  # noqa: F401
