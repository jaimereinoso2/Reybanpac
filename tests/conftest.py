import pandas as pd
import pytest


@pytest.fixture
def df_haciendas():
    return pd.DataFrame({
        "FECHA": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01", "2023-12-01"]),
        "Semana": [1, 5, 9, 48],
        "Zona": ["Norte", "Sur", "Norte", "Centro"],
        "Unidad": ["H001", "H002", "H001", "H003"],
        "Nombre_Unidad": ["Hacienda A", "Hacienda B", "Hacienda A", "Hacienda C"],
        "Real": [0.95, 0.88, 0.91, 0.97],
        "Costo_Ha": [100.0, 200.0, 150.0, 120.0],
        "Total_Cajas": [1000, 2000, 1500, 900],
        "Total_Hectareas": [10.0, 15.0, 10.0, 8.0],
        "Incidencia_Sigatoka": [2.1, 3.5, 2.8, 1.2],
    })


@pytest.fixture
def dfs_con_global(df_haciendas):
    return {"DF_GLOBAL": df_haciendas}


@pytest.fixture
def dfs_multiples(df_haciendas):
    df_paso1 = df_haciendas[df_haciendas["FECHA"].dt.year == 2024].copy()
    return {"DF_GLOBAL": df_haciendas, "df_paso1": df_paso1}
