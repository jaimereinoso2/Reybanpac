# 🍌 Análisis de Haciendas — Reybanpac

Aplicación web que permite hacer preguntas en **lenguaje natural** sobre los datos de producción y costos de haciendas bananeras. Usa Claude (Anthropic) para generar y ejecutar automáticamente el código de análisis.

---

## ¿Qué hace?

1. El usuario escribe una pregunta en español, por ejemplo:
   *"¿Cuáles son las 5 haciendas con mayor costo por hectárea en 2024?"*

2. Claude genera un **plan de análisis** dividido en pasos, donde cada paso produce un DataFrame.

3. Por cada paso, Claude:
   - Genera código pandas
   - Lo **verifica estáticamente** (nombres de columnas, variables, variable de salida)
   - Lo **ejecuta** contra el CSV real
   - Si hay un error, lo **corrige automáticamente** (hasta 4 intentos)

4. Se muestra el resultado final como tabla interactiva con opción de **descarga en CSV**.

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

# Configurar la API key de Anthropic
echo "ANTHROPIC_API_KEY=sk-ant-..." > .env

# Ejecutar la app
streamlit run app_csv.py
```

La app abre en `http://localhost:8501`.

---

## Publicar en internet con Streamlit Community Cloud

[Streamlit Community Cloud](https://streamlit.io/cloud) permite publicar la app **gratis** directamente desde GitHub.

### Pasos

1. **Subir el código a GitHub**

   ```bash
   git add .
   git commit -m "deploy: lista para publicar"
   git push origin main
   ```

   > El archivo `datos/Base Haciendas Depurada.csv` debe estar incluido en el repositorio (verificar que no esté en `.gitignore`).

2. **Crear cuenta en Streamlit Community Cloud**
   Ir a [share.streamlit.io](https://share.streamlit.io) e iniciar sesión con GitHub.

3. **Crear nueva app**
   - Clic en **"New app"**
   - Seleccionar el repositorio y la rama `main`
   - En **"Main file path"** escribir: `app_csv.py`
   - Clic en **"Deploy!"**

4. **Configurar el secreto de la API key**
   Una vez desplegada, ir a **Settings → Secrets** y agregar:

   ```toml
   ANTHROPIC_API_KEY = "sk-ant-..."
   ```

   La app lee este secreto automáticamente a través de `st.secrets`.

5. La app quedará disponible en una URL pública del tipo:
   `https://<tu-usuario>-reybanpac-app-csv.streamlit.app`

### Notas de seguridad

- **Nunca subas el archivo `.env` a GitHub.** Está incluido en `.gitignore`.
- La API key solo debe configurarse como secreto en Streamlit Cloud, no en el código.

---

## Variables de entorno / Secretos

| Variable | Descripción | Requerida |
|---|---|---|
| `ANTHROPIC_API_KEY` | API key de Anthropic (Claude) | Sí |
| `CLAUDE_MODEL` | Modelo a usar (default: `claude-sonnet-4-6`) | No |

---

## Dependencias principales

| Paquete | Uso |
|---|---|
| `streamlit` | Interfaz web |
| `anthropic` | API de Claude (generación, verificación y corrección de código) |
| `pandas` | Análisis de datos |
| `python-dotenv` | Carga de variables de entorno locales |
