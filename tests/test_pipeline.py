"""
Specs: verificar_y_ejecutar y ejecutar_pipeline
Responsabilidad: orquestar el ciclo verificar → corregir → ejecutar por paso,
y coordinar todos los pasos para responder una pregunta de negocio.
"""
import json
import pandas as pd
import pytest
from unittest.mock import patch
from pipeline_csv import verificar_y_ejecutar, ejecutar_pipeline


class DescribeVerificarYEjecutar:

    def it_retorna_codigo_y_dataframe_cuando_el_codigo_es_valido_desde_el_inicio(
        self, df_haciendas, dfs_con_global
    ):
        codigo = "df_paso1 = DF_GLOBAL.copy()"

        with patch("pipeline_csv.verificar_codigo", return_value={"valido": True}):
            codigo_final, df = verificar_y_ejecutar(
                codigo, "copiar DF_GLOBAL", "pregunta test", dfs_con_global, 1
            )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(df_haciendas)

    def it_corrige_y_reintenta_cuando_hay_error_estatico(self, dfs_con_global):
        codigo_malo = "df_paso1 = DF_GLOBAL[['costo_ha']]"
        codigo_bueno = "df_paso1 = DF_GLOBAL[['Costo_Ha']]"

        verificaciones = [
            {"valido": False, "errores": "columna 'costo_ha' no existe"},
            {"valido": True},
        ]

        with patch("pipeline_csv.verificar_codigo", side_effect=verificaciones), \
             patch("pipeline_csv.corregir_codigo", return_value=codigo_bueno):
            codigo_final, df = verificar_y_ejecutar(
                codigo_malo, "seleccionar Costo_Ha", "pregunta", dfs_con_global, 1
            )

        assert isinstance(df, pd.DataFrame)
        assert "Costo_Ha" in df.columns

    def it_corrige_y_reintenta_cuando_hay_error_de_ejecucion(self, dfs_con_global):
        codigo_malo = "df_paso1 = DF_GLOBAL.groupby('Zona')['Costo_Ha'].mean()"
        codigo_bueno = "df_paso1 = DF_GLOBAL.groupby('Zona')['Costo_Ha'].mean().reset_index()"

        with patch("pipeline_csv.verificar_codigo", return_value={"valido": True}), \
             patch("pipeline_csv.corregir_codigo", return_value=codigo_bueno):
            codigo_final, df = verificar_y_ejecutar(
                codigo_malo, "agrupar por zona", "pregunta", dfs_con_global, 1
            )

        assert isinstance(df, pd.DataFrame)

    def it_lanza_excepcion_si_agota_los_intentos_maximos(self, dfs_con_global):
        codigo_siempre_malo = "df_paso1 = DF_GLOBAL[['col_que_no_existe']]"

        with patch("pipeline_csv.verificar_codigo", return_value={"valido": True}), \
             patch("pipeline_csv.corregir_codigo", return_value=codigo_siempre_malo):
            with pytest.raises(RuntimeError, match="paso 1"):
                verificar_y_ejecutar(
                    codigo_siempre_malo, "actividad", "pregunta", dfs_con_global, 1
                )

    def it_invoca_callback_con_eventos_de_verificacion(self, dfs_con_global):
        codigo = "df_paso1 = DF_GLOBAL.copy()"
        eventos_recibidos = []

        with patch("pipeline_csv.verificar_codigo", return_value={"valido": True}):
            verificar_y_ejecutar(
                codigo, "copiar", "pregunta", dfs_con_global, 1,
                callback=lambda e: eventos_recibidos.append(e)
            )

        tipos = [e["tipo"] for e in eventos_recibidos]
        assert "verificacion" in tipos

    def it_el_callback_recibe_estado_ok_cuando_el_paso_es_exitoso(self, dfs_con_global):
        codigo = "df_paso1 = DF_GLOBAL.copy()"
        estados = []

        with patch("pipeline_csv.verificar_codigo", return_value={"valido": True}):
            verificar_y_ejecutar(
                codigo, "copiar", "pregunta", dfs_con_global, 1,
                callback=lambda e: estados.append(e.get("estado"))
            )

        assert "ok" in estados


class DescribeEjecutarPipeline:

    def it_retorna_plan_pasos_y_dataframe_final(self, df_haciendas):
        plan_json = json.dumps([
            "df_paso1: Seleccionar de DF_GLOBAL columnas Zona y Costo_Ha",
            "df_paso2: Agrupar df_paso1 por Zona promediando Costo_Ha",
        ])
        codigo_paso1 = (
            "RAZONAMIENTO:\nSelecciona las columnas.\n\n"
            "CODIGO:\ndf_paso1 = DF_GLOBAL[['Zona', 'Costo_Ha']]"
        )
        codigo_paso2 = (
            "RAZONAMIENTO:\nAgrupa y promedia.\n\n"
            "CODIGO:\ndf_paso2 = df_paso1.groupby('Zona')['Costo_Ha'].mean().reset_index()"
        )

        with patch("pipeline_csv.DF_GLOBAL", df_haciendas), \
             patch("pipeline_csv._llm", side_effect=[
                 plan_json,
                 codigo_paso1,
                 json.dumps({"valido": True}),
                 codigo_paso2,
                 json.dumps({"valido": True}),
             ]):
            pasos_texto, pasos_resultado, df_final = ejecutar_pipeline(
                "¿Cuál es el costo promedio por zona?"
            )

        assert isinstance(pasos_texto, list)
        assert len(pasos_texto) == 2
        assert isinstance(pasos_resultado, list)
        assert len(pasos_resultado) == 2
        assert isinstance(df_final, pd.DataFrame)

    def it_cada_paso_resultado_contiene_actividad_codigo_y_dataframe(self, df_haciendas):
        plan_json = json.dumps([
            "df_paso1: Filtrar DF_GLOBAL por zona Norte",
        ])
        llm_codigo = (
            "RAZONAMIENTO:\nFiltra por zona.\n\n"
            "CODIGO:\ndf_paso1 = DF_GLOBAL[DF_GLOBAL['Zona'] == 'Norte']"
        )

        with patch("pipeline_csv.DF_GLOBAL", df_haciendas), \
             patch("pipeline_csv._llm", side_effect=[
                 plan_json,
                 llm_codigo,
                 json.dumps({"valido": True}),
             ]):
            _, pasos_resultado, _ = ejecutar_pipeline("Haciendas en zona Norte")

        paso = pasos_resultado[0]
        assert "actividad" in paso
        assert "codigo" in paso
        assert "df_resultado" in paso
        assert isinstance(paso["df_resultado"], pd.DataFrame)

    def it_invoca_callback_para_cada_evento_del_pipeline(self, df_haciendas):
        plan_json = json.dumps(["df_paso1: Copiar DF_GLOBAL"])
        llm_codigo = "RAZONAMIENTO:\nCopia.\n\nCODIGO:\ndf_paso1 = DF_GLOBAL.copy()"

        eventos = []

        with patch("pipeline_csv.DF_GLOBAL", df_haciendas), \
             patch("pipeline_csv._llm", side_effect=[
                 plan_json,
                 llm_codigo,
                 json.dumps({"valido": True}),
             ]):
            ejecutar_pipeline("dame todo", callback=lambda e: eventos.append(e["tipo"]))

        assert "plan_inicio" in eventos
        assert "plan_listo" in eventos
        assert "paso_inicio" in eventos
        assert "paso_listo" in eventos
        assert "pipeline_listo" in eventos
