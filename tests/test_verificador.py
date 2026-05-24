"""
Specs: verificar_codigo y corregir_codigo
Responsabilidad: verificar estáticamente que el código generado usa nombres de
columnas y variables correctos, y corregirlo cuando no es así.
"""
import json
import pytest
from unittest.mock import patch
from pipeline_csv import verificar_codigo, corregir_codigo


class DescribeVerificadorCodigo:

    def it_reporta_valido_cuando_el_llm_aprueba_el_codigo(self, dfs_con_global):
        respuesta = json.dumps({"valido": True})
        codigo = "df_paso1 = DF_GLOBAL[['Zona', 'Costo_Ha']]"

        with patch("pipeline_csv._llm", return_value=respuesta):
            resultado = verificar_codigo(codigo, dfs_con_global, 1)

        assert resultado["valido"] is True

    def it_reporta_invalido_con_descripcion_del_error(self, dfs_con_global):
        respuesta = json.dumps({
            "valido": False,
            "errores": "columna 'costo_ha' no existe, debe ser 'Costo_Ha'"
        })
        codigo = "df_paso1 = DF_GLOBAL[['Zona', 'costo_ha']]"

        with patch("pipeline_csv._llm", return_value=respuesta):
            resultado = verificar_codigo(codigo, dfs_con_global, 1)

        assert resultado["valido"] is False
        assert "errores" in resultado
        assert len(resultado["errores"]) > 0

    def it_es_tolerante_si_el_llm_retorna_json_con_markdown(self, dfs_con_global):
        respuesta = "```json\n" + json.dumps({"valido": True}) + "\n```"
        codigo = "df_paso1 = DF_GLOBAL.copy()"

        with patch("pipeline_csv._llm", return_value=respuesta):
            resultado = verificar_codigo(codigo, dfs_con_global, 1)

        assert resultado["valido"] is True

    def it_retorna_valido_true_si_el_json_no_puede_parsearse(self, dfs_con_global):
        """Ante respuesta malformada del LLM, se asume código válido para no bloquear."""
        with patch("pipeline_csv._llm", return_value="respuesta no JSON"):
            resultado = verificar_codigo("df_paso1 = DF_GLOBAL.copy()", dfs_con_global, 1)

        assert resultado.get("valido") is True


class DescribeCorrectorCodigo:

    def it_retorna_codigo_corregido_sin_backticks(self, dfs_con_global):
        codigo_bueno = "df_paso1 = DF_GLOBAL[['Zona', 'Costo_Ha']]"
        with patch("pipeline_csv._llm", return_value=codigo_bueno):
            resultado = corregir_codigo(
                codigo="df_paso1 = DF_GLOBAL[['Zona', 'costo_ha']]",
                errores="columna 'costo_ha' no existe",
                dfs_disponibles=dfs_con_global,
                actividad="seleccionar columnas",
                pregunta_original="comparar costos por zona",
                numero_paso=1,
            )

        assert "```" not in resultado
        assert "Costo_Ha" in resultado

    def it_preserva_el_nombre_de_variable_de_salida_correcto(self, dfs_con_global):
        codigo_corregido = "df_paso2 = df_paso1.groupby('Zona')['Costo_Ha'].mean().reset_index()"
        with patch("pipeline_csv._llm", return_value=codigo_corregido):
            resultado = corregir_codigo(
                codigo="df_paso2 = df_paso1.groupby('zona')['Costo_Ha'].mean()",
                errores="columna 'zona' no existe",
                dfs_disponibles=dfs_con_global,
                actividad="agrupar por zona",
                pregunta_original="costo promedio por zona",
                numero_paso=2,
            )

        assert "df_paso2" in resultado
