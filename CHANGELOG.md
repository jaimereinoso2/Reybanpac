# Registro de cambios y decisiones

Formato: fecha · usuario git · qué se hizo · decisiones tomadas

---

## 2026-05-23 · jaimereinoso2 *(sesión actual, pendiente de commit)*

### Añadido
- `CLAUDE.md` con regla de entorno virtual obligatorio (`venv_reybanpac`) y pasos para crearlo si no existe.
- Specs SDD con pytest: `tests/` con clases `Describe*/it_*` para `_limpiar_codigo`, `_ejecutar_codigo`, `verificar_codigo`, `corregir_codigo`, `hacer_plan`, `verificar_y_ejecutar` y `ejecutar_pipeline`.
- Carpeta `agentes/` con un módulo por agente: `planificador`, `codificador`, `verificador`, `corrector`.
- Carpeta `agentes/comun/` con utilidades compartidas: `llm.py`, `codigo.py`, `contexto.py`.
- Carpeta `informacion/` con `ONTOLOGIA.md` (contexto del negocio) y `SCHEMA.md` (movido desde la raíz).
- `agentes/comun/contexto.py` con `cargar_schema()` y `cargar_ontologia()`.
- Specs independientes por agente en `tests/agentes/`.
- Autenticación basada en archivo `ACCESOS.md` con tabla markdown de usuarios y passwords.
- Menú lateral en `app_csv.py` con 4 secciones: Generar consultas, Subir datos, Descargar datos, Estructura de la tabla.

### Decisiones
- **Agentes como carpetas independientes**: cada función con su propio prompt se aisló en su carpeta para que sea testeable sin depender del resto del sistema.
- **`agentes/comun/` en vez de duplicar código**: `_limpiar_codigo` vive en `comun/` porque la usan tanto `codificador` como `corrector`. Ponerla solo en `corrector` crearía dependencia entre agentes.
- **Autenticación por archivo**: se reemplazaron las variables de entorno (`USUARIO1`, `USUARIO1_PASS`, ...) por `ACCESOS.md` para simplificar la gestión de usuarios sin tocar el código.
- **`ACCESOS.md` incluido en el repositorio**: inicialmente se agregó a `.gitignore`, pero se revirtió porque el deploy en Render.com requiere que el archivo esté en el repo.
- **`importlib.reload` al subir archivo**: al guardar un nuevo CSV se recarga `pipeline_csv` en memoria para que `DF_GLOBAL` refleje los datos nuevos sin reiniciar el proceso.

---

## 2026-05-13 · jaimereinoso2 · `d61735a`

- Ajuste del README.

---

## 2026-03-23 · jaimereinoso2 · `823ac3e` `ba03e33`

- Se agregó logging al pipeline.
- Se reactivó el API key de Anthropic (había sido desactivado temporalmente).

---

## 2026-03-17 · jaimereinoso2 · `e5b3e45` `b4e34ee` `8f37c4e` `0f40a0e` `4085b54`

### Cambios
- Se completó `app_csv.py`: pipeline completo funcionando sobre CSV sin base de datos.
- Se removió `.env` del tracking de git y se actualizó `.gitignore`.

### Decisiones y correcciones
- **Migración de base de datos a CSV**: se abandonó la conexión a base de datos (ver historial marzo 12-14) y se pasó a leer directamente desde un archivo CSV local. Motivo: simplicidad de deploy y menor latencia.
- **Cliente Anthropic lazy**: el cliente se creaba al importar el módulo, antes de que Streamlit cargara sus secrets. Se corrigió dos veces:
  - `b4e34ee`: se pasó a instanciación lazy (solo al llamar).
  - `8f37c4e`: se revirtió a recrear el cliente en cada llamada para evitar estado de autenticación desactualizado.

---

## 2026-03-14 · jaimereinoso2 · `d9e9db9` `41a4bb5`

- Integración con Streamlit: interfaz web con área de texto para preguntas.
- Soporte dual de configuración: `st.secrets` en la nube, `dotenv` en local.

---

## 2026-03-12–13 · jaimereinoso2 · `3092e19` `84fdb16` `27adbe0`

- Pipeline conectado a base de datos PostgreSQL: lee schema, genera pandas paso a paso y resuelve preguntas.

### Decisión posterior (revertida el 2026-03-17)
- Este enfoque fue abandonado en favor de CSV por complejidad de deploy y latencia de red a la base de datos.

---

## 2026-03-10 · jaimereinoso2 · `da13fda`

- Primera versión funcional: el LLM genera código Python que se ejecuta sobre un DataFrame para responder preguntas en lenguaje natural.

---

## 2026-03-06 · jaimereinoso2 · `d8bbbf0` `21f7c91`

- Proof of concept inicial: notebook que consulta un DataFrame con pandas y Claude.
