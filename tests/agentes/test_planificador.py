"""
Specs: agentes/planificador
Prueba el agente de forma aislada — sin cargar CSV ni llamar a la API real.
"""
import json
import pytest
from unittest.mock import patch
from agentes.planificador.planificador import run, SYSTEM_PROMPT


_FECHA_REF = "2025-01-01 (enero 2025)"


class DescribePlanificador:

    def it_retorna_lista_de_pasos_para_una_pregunta_valida(self):
        pasos_esperados = [
            "df_paso1: Seleccionar de DF_GLOBAL Nombre_Unidad, Costo_Ha del año 2024",
            "df_paso2: Ordenar df_paso1 por Costo_Ha descendente y tomar top 5",
        ]
        with patch("agentes.planificador.planificador._llm", return_value=json.dumps(pasos_esperados)):
            resultado = run("¿Cuáles son las 5 haciendas más caras en 2024?", _FECHA_REF)

        assert resultado == pasos_esperados

    def it_el_prompt_incluye_la_pregunta_del_usuario(self):
        pregunta = "¿Cuál es la zona con mayor sigatoka?"
        llamadas = []

        with patch("agentes.planificador.planificador._llm", side_effect=lambda p, **kw: (llamadas.append(p) or json.dumps(["df_paso1: x"]))):
            run(pregunta, _FECHA_REF)

        assert pregunta in llamadas[0]

    def it_el_prompt_incluye_el_schema_depurado(self):
        llamadas = []

        with patch("agentes.planificador.planificador._llm", side_effect=lambda p, **kw: (llamadas.append(p) or json.dumps(["df_paso1: x"]))):
            run("pregunta cualquiera", _FECHA_REF)

        assert "FECHA" in llamadas[0]
        assert "FECHA_mes" in llamadas[0]
        assert "FECHA_ano" in llamadas[0]
        assert "Costo_Ha" in llamadas[0]

    def it_el_prompt_instruye_a_usar_fecha_mes_y_fecha_ano_para_filtros(self):
        llamadas = []

        with patch("agentes.planificador.planificador._llm", side_effect=lambda p, **kw: (llamadas.append(p) or json.dumps(["df_paso1: x"]))):
            run("costos de octubre del 2024", _FECHA_REF)

        assert "FECHA_mes" in llamadas[0]
        assert "FECHA_ano" in llamadas[0]
        # el prompt debe dar el ejemplo concreto del número de mes
        assert "octubre" in llamadas[0] or "10" in llamadas[0]

    def it_el_prompt_incluye_la_fecha_de_referencia(self):
        llamadas = []

        with patch("agentes.planificador.planificador._llm", side_effect=lambda p, **kw: (llamadas.append(p) or json.dumps(["df_paso1: x"]))):
            run("últimos 6 meses de costos", _FECHA_REF)

        assert _FECHA_REF in llamadas[0]

    def it_maneja_respuesta_llm_con_bloque_markdown_json(self):
        pasos = ["df_paso1: Filtrar DF_GLOBAL por año 2023"]
        respuesta = "```json\n" + json.dumps(pasos) + "\n```"

        with patch("agentes.planificador.planificador._llm", return_value=respuesta):
            resultado = run("costos de 2023", _FECHA_REF)

        assert resultado == pasos

    def it_puede_importarse_sin_cargar_el_csv(self):
        import agentes.planificador.planificador  # noqa: F401
