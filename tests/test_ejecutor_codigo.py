"""
Specs: _ejecutar_codigo
Responsabilidad: ejecutar código pandas generado por el LLM en un entorno
aislado y retornar el DataFrame resultante o un mensaje de error.
"""
import pandas as pd
import pytest
from pipeline_csv import _ejecutar_codigo


class DescribeEjecutorCodigo:

    def it_ejecuta_codigo_valido_y_retorna_un_dataframe(self, df_haciendas):
        codigo = "df_paso1 = DF_GLOBAL.copy()"
        df, error = _ejecutar_codigo(codigo, {"DF_GLOBAL": df_haciendas}, 1)

        assert error is None
        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(df_haciendas)

    def it_retorna_error_cuando_el_codigo_lanza_una_excepcion(self, df_haciendas):
        codigo = "df_paso1 = DF_GLOBAL['columna_inexistente']"
        df, error = _ejecutar_codigo(codigo, {"DF_GLOBAL": df_haciendas}, 1)

        assert df is None
        assert error is not None
        assert len(error) > 0

    def it_retorna_error_si_la_variable_de_salida_no_fue_asignada(self, df_haciendas):
        codigo = "resultado = DF_GLOBAL.copy()"  # debería asignar a df_paso1
        df, error = _ejecutar_codigo(codigo, {"DF_GLOBAL": df_haciendas}, 1)

        assert df is None
        assert "df_paso1" in error

    def it_retorna_error_si_el_resultado_no_es_un_dataframe(self, df_haciendas):
        codigo = "df_paso1 = 42"
        df, error = _ejecutar_codigo(codigo, {"DF_GLOBAL": df_haciendas}, 1)

        assert df is None
        assert error is not None

    def it_no_modifica_los_dataframes_de_entrada(self, df_haciendas):
        columnas_originales = list(df_haciendas.columns)
        len_original = len(df_haciendas)

        codigo = "df_paso1 = DF_GLOBAL.copy(); df_paso1['col_nueva'] = 99"
        _ejecutar_codigo(codigo, {"DF_GLOBAL": df_haciendas}, 1)

        assert list(df_haciendas.columns) == columnas_originales
        assert len(df_haciendas) == len_original

    def it_puede_acceder_a_multiples_dataframes_disponibles(self, dfs_multiples):
        codigo = "df_paso2 = pd.concat([DF_GLOBAL, df_paso1], ignore_index=True)"
        df, error = _ejecutar_codigo(codigo, dfs_multiples, 2)

        assert error is None
        assert isinstance(df, pd.DataFrame)
        assert len(df) > len(dfs_multiples["df_paso1"])

    def it_tiene_pandas_disponible_como_pd_en_el_entorno(self, df_haciendas):
        codigo = "df_paso1 = pd.DataFrame({'a': [1, 2, 3]})"
        df, error = _ejecutar_codigo(codigo, {"DF_GLOBAL": df_haciendas}, 1)

        assert error is None
        assert list(df.columns) == ["a"]

    def it_ejecuta_paso_n_cualquiera_usando_el_numero_correcto(self, df_haciendas):
        codigo = "df_paso3 = DF_GLOBAL[DF_GLOBAL['Zona'] == 'Norte']"
        df, error = _ejecutar_codigo(codigo, {"DF_GLOBAL": df_haciendas}, 3)

        assert error is None
        assert all(df["Zona"] == "Norte")
