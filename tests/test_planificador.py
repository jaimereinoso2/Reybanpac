"""
Specs: hacer_plan
Responsabilidad: traducir una pregunta en lenguaje natural a una lista ordenada
de pasos, donde cada paso produce un DataFrame nombrado df_pasoN.
"""
import json
import pytest
from unittest.mock import patch
from pipeline_csv import hacer_plan


class DescribePlanificador:

    def it_retorna_una_lista_de_strings_para_una_pregunta_valida(self):
        respuesta = json.dumps([
            "df_paso1: Seleccionar de DF_GLOBAL Nombre_Unidad, Costo_Ha del año 2024",
            "df_paso2: Ordenar df_paso1 por Costo_Ha descendente y tomar top 5",
        ])
        with patch("pipeline_csv._llm", return_value=respuesta):
            pasos = hacer_plan("¿Cuáles son las 5 haciendas más caras en 2024?")

        assert isinstance(pasos, list)
        assert len(pasos) == 2
        assert all(isinstance(p, str) for p in pasos)

    def it_el_primer_paso_siempre_referencia_df_global(self):
        respuesta = json.dumps([
            "df_paso1: Filtrar DF_GLOBAL por zona Norte",
            "df_paso2: Agrupar df_paso1 por Nombre_Unidad sumando Total_Cajas",
        ])
        with patch("pipeline_csv._llm", return_value=respuesta):
            pasos = hacer_plan("Producción en zona Norte")

        assert "DF_GLOBAL" in pasos[0]

    def it_maneja_respuesta_llm_con_bloque_markdown_json(self):
        respuesta = "```json\n" + json.dumps([
            "df_paso1: Filtrar DF_GLOBAL por año 2023",
        ]) + "\n```"
        with patch("pipeline_csv._llm", return_value=respuesta):
            pasos = hacer_plan("Costos de 2023")

        assert isinstance(pasos, list)
        assert len(pasos) == 1

    def it_puede_generar_plan_con_un_solo_paso(self):
        respuesta = json.dumps([
            "df_paso1: Seleccionar de DF_GLOBAL todas las columnas del año 2024"
        ])
        with patch("pipeline_csv._llm", return_value=respuesta):
            pasos = hacer_plan("Dame todos los datos de 2024")

        assert len(pasos) == 1

    def it_puede_generar_plan_con_multiples_pasos(self):
        respuesta = json.dumps([
            "df_paso1: Filtrar DF_GLOBAL por año 2024",
            "df_paso2: Calcular en df_paso1 costo_por_caja = Costo_Ha / Total_Cajas",
            "df_paso3: Agrupar df_paso2 por Zona promediando costo_por_caja",
            "df_paso4: Ordenar df_paso3 por costo_por_caja descendente",
        ])
        with patch("pipeline_csv._llm", return_value=respuesta):
            pasos = hacer_plan("Ranking de zonas por eficiencia de costos en 2024")

        assert len(pasos) == 4
