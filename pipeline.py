import os
import json
import psycopg2
import pandas as pd
import anthropic
from dotenv import load_dotenv

load_dotenv()


def _cfg(key: str, default: str = None) -> str:
    """Lee de st.secrets si está disponible, sino de variables de entorno."""
    try:
        import streamlit as st
        return st.secrets[key]
    except Exception:
        return os.getenv(key, default)


client = anthropic.Anthropic(api_key=_cfg("ANTHROPIC_API_KEY"))
CLAUDE_MODEL = _cfg("CLAUDE_MODEL", "claude-sonnet-4-6")

SCHEMA_CONTEXT = """
Tabla: haciendas
Granularidad: un registro = una hacienda + un mes.
Cobertura temporal: enero 2020 a junio 2025.

Columnas:
- FECHA (date): fecha del registro (tipo DATE en PostgreSQL, formato YYYY-MM-DD)
- Semana (int): número de semana del año
- Zona (text): región geográfica
- Unidad (text): código identificador de la hacienda
- Nombre_Unidad (text): nombre de la hacienda
- Real (numeric): indicador de rendimiento/ratio de producción real
- Costo_Ha (numeric): costo total acumulado por hectárea
- Atencion_Plantacion (numeric): costos de mantenimiento del cultivo
- C_Riego (numeric): costo total de riego
- C_Mano_Obra_Riego (numeric): costo de personal para riego
- C_Mantenimiento_Riego (numeric): costo de reparaciones de infraestructura de riego
- C_Combustible (numeric): costo de combustible
- C_Control_Sigatoca (numeric): costo del programa contra la Sigatoka
- C_Aplicacion_Aerea (numeric): costo de fumigación aérea
- C_Deshoje (numeric): costo de deshoje
- C_Costos_Productos (numeric): costo en insumos químicos y fertilizantes
- C_Fertilizacion (numeric): costo total de fertilización
- C_Sacos_Fert (numeric): costo de sacos de fertilizante
- C_ManodeObra_Fert (numeric): costo de aplicación de fertilizante
- C_Transporte_Fert (numeric): costo de transporte de fertilizante
- C_Administracion_Hacienda (numeric): costos administrativos
- Sueldos (numeric): nómina de empleados fijos
- Servicios_Basicos (numeric): pagos de luz, agua y otros servicios
- C_Empaque_Fijo (numeric): costos fijos de empaque
- Mantenimiento_Empacadora (numeric): costos de mantenimiento de empacadora
- Mantenimiento_Equipo (numeric): costo de mantenimiento de equipo
- C_Logistica (numeric): costo total de logística
- Transporte (numeric): gasto en fletes y acarreo
- Materiales (numeric): inversión en insumos de empaque
- Reclasificaciones_Transporte (numeric): ajustes contables sobre transporte
- Reclasificaciones_Materiales (numeric): ajustes contables sobre materiales
- C_Empaque_Variable (numeric): costos variables de empaque
- C_Cosecha (numeric): costo de cosecha
- C_Transporte (numeric): otros costos de transporte
- C_Depreciaciones (numeric): depreciación de activos fijos
- Total_Cajas (numeric): volumen total de cajas producidas
- Total_Hectareas (numeric): superficie productiva en hectáreas
- Racimo_Rechazado (numeric): cantidad de fruta rechazada
- Total_Peso_Caja (numeric): peso total de las cajas
- Promedio_Peso_Caja (numeric): peso promedio por caja
- Tipo_Suelo (text): clasificación del terreno
- Incidencia_Sigatoka (numeric): nivel de presencia de la plaga
- Temperatura_C (numeric): temperatura media en °C
- Precipitacion_mm (numeric): lluvia acumulada en mm
- Evotranspiracion (numeric): tasa de evapotranspiración
- Humedad (numeric): porcentaje de humedad relativa
- Ausentismo_Agricola (numeric): total de inasistencias del personal
- Ausentismo_Justificado_Agricola (numeric): inasistencias justificadas
- Ausentismo_Injustificado_Agricola (numeric): inasistencias injustificadas
- RotPerson_Salida_Todos_Motivos_Agricola (numeric): índice de rotación de personal
- Pago_Labor_Persona (numeric): indicador de pago por jornada
- Pago_Por_Cuenta (numeric): indicador de pagos por cuenta
- Vacante_Labor (numeric): puestos de trabajo vacantes
"""

COLUMNAS_VALIDAS = [
    line.split("(")[0].replace("-", "").strip()
    for line in SCHEMA_CONTEXT.splitlines()
    if line.strip().startswith("-")
]
COLUMNAS_VALIDAS_STR = ", ".join(f'"{c}"' for c in COLUMNAS_VALIDAS)


def _llm(prompt: str) -> str:
    message = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text.strip()


def _limpiar_sql(texto: str) -> str:
    texto = texto.strip()
    if texto.startswith("```"):
        texto = texto.split("```")[1]
        if texto.startswith("sql"):
            texto = texto[3:]
    return texto.strip()


def _conectar():
    return psycopg2.connect(
        user=_cfg("user"), password=_cfg("password"),
        host=_cfg("host"), port=_cfg("port"), dbname=_cfg("dbname")
    )


def hacer_plan(pregunta: str) -> list[str]:
    prompt = f"""Eres un experto en análisis de datos con SQL y pandas para Python.

Tienes la tabla "haciendas" en PostgreSQL con las columnas:
{COLUMNAS_VALIDAS_STR}

El usuario pregunta: "{pregunta}"

Genera un plan de resolución con estas reglas:
- El PRIMER paso debe describir qué datos extraer de la base de datos con SQL (una sola consulta amplia).
- Los pasos SIGUIENTES deben describir operaciones de transformación, filtrado o cálculo sobre el DataFrame de pandas resultante del paso anterior. NO deben consultar la base de datos.

Responde ÚNICAMENTE con un JSON array de strings, sin markdown. Ejemplo:
[
  "Extraer de la BD: Nombre_Unidad, Costo_Ha, Total_Cajas, FECHA para el año 2024",
  "Calcular el costo por caja dividiendo Costo_Ha entre Total_Cajas",
  "Ordenar de mayor a menor por costo por caja y quedarse con las 5 primeras"
]"""

    texto = _llm(prompt)
    if texto.startswith("```"):
        texto = texto.split("```")[1]
        if texto.startswith("json"):
            texto = texto[4:]
    return json.loads(texto.strip().rstrip("```").strip())


def generar_sql(actividad: str, pregunta_original: str) -> tuple[str, str]:
    prompt = f"""Eres un experto en SQL para PostgreSQL y en el negocio bananero.

Tabla disponible:
{SCHEMA_CONTEXT}

Pregunta original del usuario: "{pregunta_original}"

Genera el SQL para extraer los datos necesarios para esta actividad:
"{actividad}"

Responde en este formato exacto:

RAZONAMIENTO:
<explica qué columnas extraes y por qué, en 2-4 oraciones>

SQL:
<solo el SQL, con saltos de línea e indentación>

Reglas del SQL:
- Tabla: "haciendas"
- Extrae columnas suficientes para que los pasos siguientes puedan operar sobre el DataFrame sin volver a la BD
- FECHA es DATE; para filtrar/agrupar por año: EXTRACT(YEAR FROM "FECHA")
- Comillas dobles en TODOS los nombres de columna
- Solo estas columnas (PROHIBIDO inventar otras): {COLUMNAS_VALIDAS_STR}
- Sin markdown, sin backticks"""

    texto = _llm(prompt)
    razonamiento, sql = "", ""
    if "SQL:" in texto:
        partes = texto.split("SQL:", 1)
        razonamiento = partes[0].replace("RAZONAMIENTO:", "").strip()
        sql = _limpiar_sql(partes[1])
    else:
        sql = _limpiar_sql(texto)
    return razonamiento, sql


def corregir_sql(sql: str) -> str:
    prompt = f"""Eres un experto en SQL para PostgreSQL.

Tienes la tabla "haciendas" con EXACTAMENTE estas columnas (respeta mayúsculas/minúsculas):
{COLUMNAS_VALIDAS_STR}

Revisa el siguiente SQL y corrige únicamente los nombres de columna que no correspondan a la lista anterior.
Si un nombre de columna no existe, reemplázalo por el nombre correcto más cercano de la lista.
No cambies nada más (lógica, filtros, alias de resultado, etc.).
Si el SQL ya es correcto, devuélvelo tal cual.

SQL a revisar:
{sql}

Devuelve SOLO el SQL corregido, sin explicaciones, sin markdown, sin backticks."""

    return _limpiar_sql(_llm(prompt))


def generar_pandas(actividad: str, pregunta_original: str, df: pd.DataFrame) -> tuple[str, str]:
    info_df = (
        f"Columnas: {list(df.columns)}\n"
        f"Tipos:    {df.dtypes.to_dict()}\n"
        f"Filas:    {len(df)}\n"
        f"Muestra:\n{df.head(5).to_string(index=False)}"
    )

    prompt = f"""Eres un experto en análisis de datos con pandas para Python.

Tienes un DataFrame llamado `df` con esta estructura:
{info_df}

Pregunta original del usuario: "{pregunta_original}"

Genera el código pandas para realizar esta operación sobre `df`:
"{actividad}"

Responde en este formato exacto:

RAZONAMIENTO:
<explica qué operación realizas y por qué, en 2-4 oraciones>

CODIGO:
<solo el código Python/pandas>

Reglas del código:
- El DataFrame de entrada se llama `df`
- El resultado final SIEMPRE debe quedar guardado en `df` como un pandas DataFrame (NO Series, NO lista, NO dict)
- Si usas .groupby(), termina con .reset_index() para que el resultado sea un DataFrame
- Si usas .value_counts() o cualquier método que devuelva una Series, conviértela a DataFrame con .reset_index()
- Solo usa pandas, no importes librerías adicionales
- Sin markdown, sin backticks"""

    texto = _llm(prompt)
    razonamiento, codigo = "", ""
    if "CODIGO:" in texto:
        partes = texto.split("CODIGO:", 1)
        razonamiento = partes[0].replace("RAZONAMIENTO:", "").strip()
        codigo = partes[1].strip()
        if codigo.startswith("```"):
            codigo = codigo.split("```")[1]
            if codigo.startswith("python"):
                codigo = codigo[6:]
        codigo = codigo.strip()
    else:
        codigo = texto.strip()
    return razonamiento, codigo


def ejecutar_pipeline(pregunta: str, callback=None) -> tuple[list[str], list[dict], pd.DataFrame]:
    """
    Ejecuta el pipeline completo.

    Args:
        pregunta: pregunta del usuario
        callback: función opcional callback(evento: dict) para reportar progreso

    Returns:
        (pasos_texto, pasos_resultado, df_final)
        Cada elemento de pasos_resultado es un dict con:
          tipo, actividad, razonamiento, codigo, df_resultado
    """
    def _emit(evento: dict):
        if callback:
            callback(evento)

    _emit({"tipo": "plan_inicio"})
    pasos_texto = hacer_plan(pregunta)
    _emit({"tipo": "plan_listo", "pasos": pasos_texto})

    pasos_resultado = []
    df = None

    # Paso 1: SQL
    actividad = pasos_texto[0]
    _emit({"tipo": "paso_inicio", "numero": 1, "subtipo": "sql", "actividad": actividad})
    razonamiento, sql = generar_sql(actividad, pregunta)
    sql = corregir_sql(sql)
    conn = _conectar()
    df = pd.read_sql(sql, conn)
    conn.close()
    paso = {
        "tipo": "sql",
        "actividad": actividad,
        "razonamiento": razonamiento,
        "codigo": sql,
        "df_resultado": df.copy(),
    }
    pasos_resultado.append(paso)
    _emit({"tipo": "paso_listo", "numero": 1, "paso": paso})

    # Pasos 2+: pandas
    for i, actividad in enumerate(pasos_texto[1:], start=2):
        _emit({"tipo": "paso_inicio", "numero": i, "subtipo": "pandas", "actividad": actividad})
        razonamiento, codigo = generar_pandas(actividad, pregunta, df)
        ns = {"df": df.copy(), "pd": pd}
        exec(codigo, ns)  # noqa: S102
        df = ns["df"]
        paso = {
            "tipo": "pandas",
            "actividad": actividad,
            "razonamiento": razonamiento,
            "codigo": codigo,
            "df_resultado": df.copy(),
        }
        pasos_resultado.append(paso)
        _emit({"tipo": "paso_listo", "numero": i, "paso": paso})

    _emit({"tipo": "pipeline_listo"})
    return pasos_texto, pasos_resultado, df
