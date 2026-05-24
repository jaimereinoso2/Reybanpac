"""
Specs: agentes/verificador
Prueba el agente de forma aislada — sin cargar CSV ni llamar a la API real.
"""
import json
import pandas as pd
import pytest
from unittest.mock import patch
from agentes.verificador.verificador import run


@pytest.fixture
def dfs_simple():
    df = pd.DataFrame({"Zona": ["Norte"], "Costo_Ha": [100.0]})
    return {"DF_GLOBAL": df}


class DescribeVerificador:

    def it_retorna_valido_true_cuando_el_llm_aprueba(self, dfs_simple):
        with patch("agentes.verificador.verificador._llm", return_value=json.dumps({"valido": True})):
            resultado = run("df_paso1 = DF_GLOBAL[['Zona']]", dfs_simple, 1)

        assert resultado["valido"] is True

    def it_retorna_valido_false_con_errores_cuando_el_llm_rechaza(self, dfs_simple):
        respuesta = json.dumps({"valido": False, "errores": "columna 'zona' no existe"})

        with patch("agentes.verificador.verificador._llm", return_value=respuesta):
            resultado = run("df_paso1 = DF_GLOBAL[['zona']]", dfs_simple, 1)

        assert resultado["valido"] is False
        assert "errores" in resultado

    def it_el_prompt_incluye_los_nombres_de_columnas_disponibles(self, dfs_simple):
        llamadas = []

        with patch("agentes.verificador.verificador._llm", side_effect=lambda p: (llamadas.append(p) or json.dumps({"valido": True}))):
            run("df_paso1 = DF_GLOBAL.copy()", dfs_simple, 1)

        assert "Zona" in llamadas[0]
        assert "Costo_Ha" in llamadas[0]

    def it_el_prompt_incluye_la_variable_de_salida_esperada(self, dfs_simple):
        llamadas = []

        with patch("agentes.verificador.verificador._llm", side_effect=lambda p: (llamadas.append(p) or json.dumps({"valido": True}))):
            run("df_paso2 = df_paso1.copy()", dfs_simple, 2)

        assert "df_paso2" in llamadas[0]

    def it_retorna_valido_true_ante_json_malformado(self, dfs_simple):
        with patch("agentes.verificador.verificador._llm", return_value="respuesta no JSON"):
            resultado = run("df_paso1 = DF_GLOBAL.copy()", dfs_simple, 1)

        assert resultado.get("valido") is True

    def it_puede_importarse_sin_cargar_el_csv(self):
        import agentes.verificador.verificador  # noqa: F401
