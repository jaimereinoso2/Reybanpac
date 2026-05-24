"""
Specs: agentes/codificador
Prueba el agente de forma aislada — sin cargar CSV ni llamar a la API real.
"""
import pandas as pd
import pytest
from unittest.mock import patch
from agentes.codificador.codificador import run


_FECHA_REF = "2025-01-01 (enero 2025)"


@pytest.fixture
def dfs_simple():
    df = pd.DataFrame({
        "Zona": ["Norte", "Sur"],
        "Costo_Ha": [100.0, 200.0],
        "Total_Cajas": [1000, 2000],
    })
    return {"DF_GLOBAL": df}


class DescribeCodificador:

    def it_retorna_razonamiento_y_codigo_separados(self, dfs_simple):
        respuesta = "RAZONAMIENTO:\nFiltra las columnas necesarias.\n\nCODIGO:\ndf_paso1 = DF_GLOBAL[['Zona', 'Costo_Ha']]"

        with patch("agentes.codificador.codificador._llm", return_value=respuesta):
            razonamiento, codigo = run("seleccionar Zona y Costo_Ha", "pregunta", 1, dfs_simple, _FECHA_REF)

        assert "Filtra" in razonamiento
        assert "df_paso1" in codigo
        assert "```" not in codigo

    def it_retorna_codigo_sin_razonamiento_si_el_llm_no_incluye_la_etiqueta(self, dfs_simple):
        respuesta = "df_paso1 = DF_GLOBAL.copy()"

        with patch("agentes.codificador.codificador._llm", return_value=respuesta):
            razonamiento, codigo = run("copiar DF_GLOBAL", "pregunta", 1, dfs_simple, _FECHA_REF)

        assert razonamiento == ""
        assert "df_paso1" in codigo

    def it_el_prompt_incluye_la_actividad_y_la_pregunta(self, dfs_simple):
        actividad = "agrupar por zona"
        pregunta = "¿cuál es el costo por zona?"
        llamadas = []

        respuesta = "RAZONAMIENTO:\nok\n\nCODIGO:\ndf_paso1 = DF_GLOBAL.copy()"
        with patch("agentes.codificador.codificador._llm", side_effect=lambda p: (llamadas.append(p) or respuesta)):
            run(actividad, pregunta, 1, dfs_simple, _FECHA_REF)

        assert actividad in llamadas[0]
        assert pregunta in llamadas[0]

    def it_el_prompt_incluye_el_nombre_de_variable_de_salida_correcto(self, dfs_simple):
        llamadas = []
        respuesta = "RAZONAMIENTO:\nok\n\nCODIGO:\ndf_paso3 = DF_GLOBAL.copy()"

        with patch("agentes.codificador.codificador._llm", side_effect=lambda p: (llamadas.append(p) or respuesta)):
            run("actividad", "pregunta", 3, dfs_simple, _FECHA_REF)

        assert "df_paso3" in llamadas[0]

    def it_el_prompt_instruye_a_usar_fecha_mes_y_fecha_ano(self, dfs_simple):
        llamadas = []
        respuesta = "RAZONAMIENTO:\nok\n\nCODIGO:\ndf_paso1 = DF_GLOBAL.copy()"

        with patch("agentes.codificador.codificador._llm", side_effect=lambda p: (llamadas.append(p) or respuesta)):
            run("filtrar por octubre del 2024", "costos de octubre 2024", 1, dfs_simple, _FECHA_REF)

        assert "FECHA_mes" in llamadas[0]
        assert "FECHA_ano" in llamadas[0]
        # la regla debe desaconsejar .dt.month / .dt.year
        assert ".dt.month" in llamadas[0] or "dt.year" in llamadas[0] or "FECHA_mes" in llamadas[0]

    def it_el_prompt_incluye_la_fecha_de_referencia(self, dfs_simple):
        llamadas = []
        respuesta = "RAZONAMIENTO:\nok\n\nCODIGO:\ndf_paso1 = DF_GLOBAL.copy()"

        with patch("agentes.codificador.codificador._llm", side_effect=lambda p: (llamadas.append(p) or respuesta)):
            run("filtrar últimos 6 meses", "consulta temporal", 1, dfs_simple, _FECHA_REF)

        assert _FECHA_REF in llamadas[0]

    def it_puede_importarse_sin_cargar_el_csv(self):
        import agentes.codificador.codificador  # noqa: F401
