# Reybanpac — Guía para Claude Code

## Entorno virtual obligatorio

**Todo comando de Python, pytest o pip debe ejecutarse dentro del entorno virtual `venv_reybanpac`.**

El archivo `.python-version` en la raíz del proyecto ya instruye a pyenv para activarlo automáticamente. Verifica que está activo antes de correr cualquier comando:

```bash
python -c "import sys; print(sys.prefix)"
# debe contener: .pyenv/versions/venv_reybanpac
```

### Si el entorno no existe

Créalo con pyenv antes de continuar:

```bash
pyenv virtualenv 3.11.9 venv_reybanpac
pyenv local venv_reybanpac          # escribe .python-version
pip install -r requirements.txt
```

### Comandos de referencia

```bash
# correr tests
python -m pytest tests/ -v

# lanzar la app
streamlit run app_csv.py

# instalar dependencias
pip install -r requirements.txt
```

## Estructura del proyecto

| Archivo | Rol |
|---|---|
| `pipeline_csv.py` | Pipeline principal: planificador, codificador, verificador y corrector |
| `app_csv.py` | Interfaz Streamlit con login |
| `datos/Base Haciendas Depurada.csv` | Dataset mensual de haciendas bananeras |
| `tests/` | Specs SDD con pytest (`Describe*/it_*`) |
| `SCHEMA.md` | Descripción de columnas del dataset |
