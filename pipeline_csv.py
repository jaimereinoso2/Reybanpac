import os
import json
import pandas as pd
import anthropic
from dotenv import load_dotenv

load_dotenv()


def _cfg(key: str, default: str = None) -> str:
    try:
        import streamlit as st
        return st.secrets[key]
    except Exception:
        return os.getenv(key, default)


client = anthropic.Anthropic(api_key=_cfg("ANTHROPIC_API_KEY"))
CLAUDE_MODEL = _cfg("CLAUDE_MODEL", "claude-sonnet-4-6")

CSV_PATH = os.path.join(os.path.dirname(__file__), "datos", "Base Haciendas Depurada.csv")
DF_GLOBAL = pd.read_csv(CSV_PATH, sep=";", encoding="utf-8-sig")
DF_GLOBAL["FECHA"] = pd.to_datetime(DF_GLOBAL["FECHA"])

SCHEMA_CONTEXT = """
DataFrame: DF_GLOBAL
Granularidad: un registro = una hacienda + un mes.
Cobertura temporal: enero 2020 a junio 2025.

Columnas:
- FECHA (date): fecha del registro (tipo datetime64, formato YYYY-MM-DD)
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


# ─── helpers ────────────────────────────────────────────────────────────────

def _llm(prompt: str) -> str:
    message = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text.strip()


def _limpiar_codigo(texto: str) -> str:
    texto = texto.strip()
    if texto.startswith("```"):
        texto = texto.split("```")[1]
        if texto.startswith("python"):
            texto = texto[6:]
    return texto.strip()


def _info_dfs(dfs: dict) -> str:
    lines = []
    for name, df in dfs.items():
        lines.append(f"DataFrame `{name}`: {df.shape[0]} filas × {df.shape[1]} columnas")
        lines.append(f"  Columnas: {list(df.columns)}")
        lines.append(f"  Tipos: {df.dtypes.to_dict()}")
        lines.append(f"  Muestra:\n{df.head(3).to_string(index=False)}")
    return "\n".join(lines)


# ─── planificador ────────────────────────────────────────────────────────────

def hacer_plan(pregunta: str) -> list[str]:
    """
    Genera un plan donde cada paso produce un DataFrame nombrado df_pasoN.
    El paso 1 siempre lee de DF_GLOBAL.
    Los pasos siguientes pueden operar sobre cualquier df_pasoX anterior.
    """
    prompt = f"""Eres un experto en análisis de datos con pandas para Python.

Tienes un DataFrame global llamado `DF_GLOBAL` con las columnas:
{COLUMNAS_VALIDAS_STR}

La columna FECHA es datetime64.

El usuario pregunta: "{pregunta}"

Genera un plan donde CADA paso:
- Produce exactamente un nuevo DataFrame llamado `df_pasoN` (N = número del paso)
- Es una de estas operaciones:
  a) Filtrar/seleccionar/transformar desde `DF_GLOBAL` o desde un `df_pasoX` anterior
  b) Operar entre dos o más DataFrames anteriores (merge, concat, cálculos cruzados)

El paso 1 SIEMPRE debe leer de `DF_GLOBAL`.

Responde ÚNICAMENTE con un JSON array de strings, sin markdown. Ejemplo:
[
  "df_paso1: Seleccionar de DF_GLOBAL las columnas Nombre_Unidad, Costo_Ha, Total_Cajas, FECHA filtrando año 2024",
  "df_paso2: Calcular en df_paso1 el costo_por_caja = Costo_Ha / Total_Cajas",
  "df_paso3: Ordenar df_paso2 por costo_por_caja descendente y tomar top 5"
]"""

    texto = _llm(prompt)
    if texto.startswith("```"):
        texto = texto.split("```")[1]
        if texto.startswith("json"):
            texto = texto[4:]
    return json.loads(texto.strip().rstrip("```").strip())


# ─── generador de código ─────────────────────────────────────────────────────

def generar_codigo_paso(actividad: str, pregunta_original: str, numero_paso: int, dfs_disponibles: dict) -> tuple[str, str]:
    output_var = f"df_paso{numero_paso}"
    info = _info_dfs(dfs_disponibles)

    prompt = f"""Eres un experto en análisis de datos con pandas para Python y en el negocio bananero.

DataFrames disponibles:
{info}

Pregunta original del usuario: "{pregunta_original}"

Genera el código pandas para realizar esta operación:
"{actividad}"

Responde en este formato exacto:

RAZONAMIENTO:
<explica qué operación realizas, en 2-4 oraciones>

CODIGO:
<solo el código Python/pandas>

Reglas:
- Los DataFrames de entrada son de solo lectura
- El resultado final DEBE quedar en `{output_var}` como pandas DataFrame
- Si usas .groupby(), termina con .reset_index()
- Si obtienes una Series, conviértela con .reset_index() o pd.DataFrame()
- Solo pandas, sin imports adicionales
- Sin markdown, sin backticks"""

    texto = _llm(prompt)
    razonamiento, codigo = "", ""
    if "CODIGO:" in texto:
        partes = texto.split("CODIGO:", 1)
        razonamiento = partes[0].replace("RAZONAMIENTO:", "").strip()
        codigo = _limpiar_codigo(partes[1])
    else:
        codigo = _limpiar_codigo(texto)
    return razonamiento, codigo


# ─── verificador ─────────────────────────────────────────────────────────────

def verificar_codigo(codigo: str, dfs_disponibles: dict, numero_paso: int) -> dict:
    """
    Verificación estática: nombres de columnas, variables y variable de salida.
    Retorna {"valido": True} o {"valido": False, "errores": "..."}.
    """
    output_var = f"df_paso{numero_paso}"
    vars_disponibles = list(dfs_disponibles.keys())
    col_info = "\n".join(
        f"- `{name}`: {list(df.columns)}"
        for name, df in dfs_disponibles.items()
    )

    prompt = f"""Eres un verificador de código pandas experto.

DataFrames disponibles y sus columnas exactas:
{col_info}

Variables disponibles: {vars_disponibles}

Código a verificar:
```python
{codigo}
```

Verifica ÚNICAMENTE:
1. Todos los nombres de columna usados existen en el DataFrame correspondiente (respeta mayúsculas/minúsculas exactas)
2. Todas las variables de DataFrame usadas están en {vars_disponibles}
3. El resultado final queda asignado a `{output_var}`

Responde SOLO con JSON sin markdown:
{{"valido": true}}
o
{{"valido": false, "errores": "descripción concisa de los errores"}}"""

    texto = _llm(prompt).strip()
    if texto.startswith("```"):
        texto = texto.split("```")[1]
        if texto.startswith("json"):
            texto = texto[4:]
    texto = texto.strip().rstrip("```").strip()
    try:
        return json.loads(texto)
    except Exception:
        return {"valido": True}


# ─── corrector ───────────────────────────────────────────────────────────────

def corregir_codigo(codigo: str, errores: str, dfs_disponibles: dict, actividad: str, pregunta_original: str, numero_paso: int) -> str:
    output_var = f"df_paso{numero_paso}"
    col_info = "\n".join(
        f"- `{name}`: {list(df.columns)}"
        for name, df in dfs_disponibles.items()
    )

    prompt = f"""Eres un experto corrector de código pandas.

DataFrames disponibles y sus columnas exactas:
{col_info}

El siguiente código tiene estos errores:
{errores}

Código con errores:
```python
{codigo}
```

Actividad: "{actividad}"
Pregunta original: "{pregunta_original}"

Corrige el código para resolver los errores.
Reglas:
- El resultado final DEBE quedar en `{output_var}` como pandas DataFrame
- Usa solo las columnas y variables listadas arriba
- Sin markdown, sin backticks — devuelve SOLO el código corregido"""

    return _limpiar_codigo(_llm(prompt))


# ─── ejecución ───────────────────────────────────────────────────────────────

def _ejecutar_codigo(codigo: str, dfs_disponibles: dict, numero_paso: int) -> tuple:
    output_var = f"df_paso{numero_paso}"
    ns = {name: df.copy() for name, df in dfs_disponibles.items()}
    ns["pd"] = pd
    try:
        exec(codigo, ns)  # noqa: S102
        df = ns.get(output_var)
        if df is None:
            return None, f"El código no asignó nada a '{output_var}'"
        if not isinstance(df, pd.DataFrame):
            return None, f"'{output_var}' es {type(df).__name__}, debe ser pandas DataFrame"
        return df, None
    except Exception as exc:
        return None, str(exc)


# ─── ciclo verificar → corregir ──────────────────────────────────────────────

MAX_ITERACIONES = 4


def verificar_y_ejecutar(codigo: str, actividad: str, pregunta: str, dfs_disponibles: dict, numero_paso: int, callback=None) -> tuple[str, pd.DataFrame]:
    """
    Ciclo: verificar → (si hay error) corregir → volver a verificar → ejecutar.
    Máximo MAX_ITERACIONES iteraciones.
    """
    def _emit(evento):
        if callback:
            callback(evento)

    for intento in range(1, MAX_ITERACIONES + 1):
        # 1. Verificación estática LLM
        resultado = verificar_codigo(codigo, dfs_disponibles, numero_paso)

        if not resultado.get("valido", True):
            errores = resultado.get("errores", "Error desconocido")
            _emit({"tipo": "verificacion", "numero": numero_paso, "intento": intento,
                   "estado": "error_estatico", "errores": errores})
            if intento < MAX_ITERACIONES:
                codigo = corregir_codigo(codigo, errores, dfs_disponibles, actividad, pregunta, numero_paso)
                _emit({"tipo": "correccion", "numero": numero_paso, "intento": intento, "codigo": codigo})
            continue

        # 2. Ejecución real
        df, error = _ejecutar_codigo(codigo, dfs_disponibles, numero_paso)
        if error:
            _emit({"tipo": "verificacion", "numero": numero_paso, "intento": intento,
                   "estado": "error_ejecucion", "errores": error})
            if intento < MAX_ITERACIONES:
                codigo = corregir_codigo(codigo, error, dfs_disponibles, actividad, pregunta, numero_paso)
                _emit({"tipo": "correccion", "numero": numero_paso, "intento": intento, "codigo": codigo})
            continue

        # OK
        _emit({"tipo": "verificacion", "numero": numero_paso, "intento": intento, "estado": "ok"})
        return codigo, df

    raise RuntimeError(f"No se pudo ejecutar correctamente el paso {numero_paso} tras {MAX_ITERACIONES} intentos.")


# ─── pipeline principal ──────────────────────────────────────────────────────

def ejecutar_pipeline(pregunta: str, callback=None) -> tuple[list[str], list[dict], pd.DataFrame]:
    def _emit(evento):
        if callback:
            callback(evento)

    _emit({"tipo": "plan_inicio"})
    pasos_texto = hacer_plan(pregunta)
    _emit({"tipo": "plan_listo", "pasos": pasos_texto})

    pasos_resultado = []
    dfs_acumulados = {"DF_GLOBAL": DF_GLOBAL}

    for i, actividad in enumerate(pasos_texto, start=1):
        _emit({"tipo": "paso_inicio", "numero": i, "subtipo": "pandas", "actividad": actividad})

        razonamiento, codigo = generar_codigo_paso(actividad, pregunta, i, dfs_acumulados)

        codigo, df = verificar_y_ejecutar(
            codigo, actividad, pregunta, dfs_acumulados, i, callback=callback
        )

        dfs_acumulados[f"df_paso{i}"] = df

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
    return pasos_texto, pasos_resultado, pasos_resultado[-1]["df_resultado"]
