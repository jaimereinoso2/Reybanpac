"""
Specs: _limpiar_codigo
Responsabilidad: normalizar la respuesta de texto del LLM extrayendo solo el
código Python, sin bloques de markdown ni backticks.
"""
import pytest
from pipeline_csv import _limpiar_codigo


class DescribeLimpiadorCodigo:

    def it_retorna_codigo_sin_cambios_cuando_ya_esta_limpio(self):
        codigo = "df_paso1 = DF_GLOBAL.copy()"
        assert _limpiar_codigo(codigo) == codigo

    def it_elimina_bloque_markdown_triple_backtick(self):
        entrada = "```\ndf_paso1 = DF_GLOBAL.copy()\n```"
        resultado = _limpiar_codigo(entrada)
        assert "```" not in resultado
        assert "df_paso1 = DF_GLOBAL.copy()" in resultado

    def it_elimina_el_indicador_de_lenguaje_python(self):
        entrada = "```python\ndf_paso1 = DF_GLOBAL[['Zona', 'Costo_Ha']]\n```"
        resultado = _limpiar_codigo(entrada)
        assert "python" not in resultado
        assert "df_paso1 = DF_GLOBAL" in resultado

    def it_elimina_espacios_en_blanco_circundantes(self):
        entrada = "  \n  df_paso1 = DF_GLOBAL.copy()  \n  "
        resultado = _limpiar_codigo(entrada)
        assert resultado == resultado.strip()

    def it_maneja_codigo_con_multiples_lineas(self):
        entrada = "```python\ndf = DF_GLOBAL.copy()\ndf['nueva'] = 1\ndf_paso1 = df\n```"
        resultado = _limpiar_codigo(entrada)
        assert "df['nueva'] = 1" in resultado
        assert "df_paso1 = df" in resultado
        assert "```" not in resultado
