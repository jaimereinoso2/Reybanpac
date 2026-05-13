# 🍌 Análisis de Haciendas — Reybanpac

Aplicación web que permite hacer preguntas en **lenguaje natural** sobre los datos de producción y costos de haciendas bananeras. Usa Claude (Anthropic) para generar y ejecutar automáticamente el análisis.

---

## Cómo funciona

### 1. Carga de datos al arrancar

Al iniciar el servidor, la app lee el archivo CSV una sola vez y lo mantiene en memoria como un DataFrame global:

```
datos/Base Haciendas Depurada.csv  →  DF_GLOBAL (RAM)
```

**No hay base de datos.** Todos los análisis se hacen directamente sobre este DataFrame en memoria.

### 2. El usuario hace una pregunta

El usuario escribe una pregunta en español, por ejemplo:

> *"¿Cuáles son las 5 haciendas con mayor costo por hectárea en 2024?"*

### 3. Claude genera un plan de análisis

Claude divide la pregunta en pasos secuenciales. Cada paso produce un DataFrame intermedio:

```
Paso 1: df_paso1 ← filtrar/seleccionar desde DF_GLOBAL
Paso 2: df_paso2 ← calcular o transformar df_paso1
Paso 3: df_paso3 ← ordenar / agregar / cruzar pasos anteriores
...
```

### 4. Por cada paso, Claude genera y ejecuta código pandas

Para cada paso el sistema:

1. **Genera** código pandas con Claude
2. **Verifica estáticamente** el código (nombres de columnas exactos, variables disponibles, variable de salida correcta)
3. **Ejecuta** el código contra el DataFrame real
4. Si hay un error, lo **corrige automáticamente** (hasta 4 intentos)

Todo esto ocurre en Python, en memoria, sin tocar ninguna base de datos.

### 5. Resultado

Se muestra el DataFrame final como tabla interactiva con opción de **descarga en CSV**.

---

## Datos

El archivo `datos/Base Haciendas Depurada.csv` contiene registros mensuales por hacienda desde enero 2020 hasta junio 2025, con ~53 columnas que incluyen:

- Indicadores de producción: `Total_Cajas`, `Total_Hectareas`, `Racimo_Rechazado`
- Costos: `Costo_Ha`, `C_Riego`, `C_Fertilizacion`, `C_Cosecha`, `Sueldos`, etc.
- Variables ambientales: `Temperatura_C`, `Precipitacion_mm`, `Humedad`, `Incidencia_Sigatoka`
- RR.HH.: `Ausentismo_Agricola`, `RotPerson_Salida_Todos_Motivos_Agricola`

Ver [SCHEMA.md](SCHEMA.md) para la descripción completa de columnas.

---

## Estructura del proyecto

```
├── app_csv.py          # Interfaz Streamlit (frontend)
├── pipeline_csv.py     # Lógica de IA: planificador, generador, verificador, corrector
├── datos/
│   └── Base Haciendas Depurada.csv
├── SCHEMA.md           # Descripción de columnas del dataset
├── requirements.txt
└── .python-version
```

---

## Instalación local

**Requisitos:** Python 3.11+

```bash
# Clonar el repositorio
git clone <url-del-repo>
cd Reybanpac

# Crear entorno virtual e instalar dependencias
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Configurar variables de entorno
cp .env.example .env             # editar con tu API key

# Ejecutar la app
streamlit run app_csv.py
```

La app abre en `http://localhost:8501`.

---

## Despliegue en Render

1. **Subir el código a GitHub** (el CSV debe estar incluido en el repo)

   ```bash
   git add .
   git commit -m "deploy"
   git push origin main
   ```

2. En [render.com](https://render.com) crear un **Web Service** apuntando al repo

3. Configurar:
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `streamlit run app_csv.py --server.port $PORT --server.address 0.0.0.0`

4. En **Environment Variables** agregar:
   - `ANTHROPIC_API_KEY` = tu key de Anthropic

---

## Despliegue en Streamlit Community Cloud

1. Ir a [share.streamlit.io](https://share.streamlit.io) e iniciar sesión con GitHub
2. Crear nueva app — seleccionar repo, rama `main`, archivo `app_csv.py`
3. En **Settings → Secrets** agregar:

   ```toml
   ANTHROPIC_API_KEY = "sk-ant-..."
   USUARIO1 = "jaime"
   USUARIO1_PASS = "clave123"
   ```

---

## Variables de entorno / Secretos

| Variable | Descripción | Requerida |
|---|---|---|
| `ANTHROPIC_API_KEY` | API key de Anthropic (Claude) | Sí |
| `CLAUDE_MODEL` | Modelo a usar (default: `claude-sonnet-4-6`) | No |
| `USUARIO1` | Nombre de usuario para login | Sí |
| `USUARIO1_PASS` | Contraseña para login | Sí |

---

## Dependencias principales

| Paquete | Uso |
|---|---|
| `streamlit` | Interfaz web |
| `anthropic` | API de Claude (generación, verificación y corrección de código) |
| `pandas` | Análisis de datos en memoria |
| `python-dotenv` | Carga de variables de entorno locales |
