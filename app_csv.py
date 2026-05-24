import importlib
import os
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

import pipeline_csv as _pipeline
from agentes.comun.contexto import actualizar_cobertura_temporal
from agentes.comun.datos import enriquecer_csv
from agentes.comun.sesiones import (
    crear_sesion, cambiar_sesion, listar_sesiones, sesion_activa_id,
    agregar_interaccion, obtener_contexto, obtener_sesion, renombrar_sesion,
)
from agentes.interpretador.interpretador import run as interpretar
from agentes.sintetizador.sintetizador import run as sintetizar

load_dotenv()

st.set_page_config(
    page_title="Análisis de Haciendas",
    page_icon="🍌",
    layout="wide",
)

_ACCESOS_PATH        = Path(__file__).parent / "ACCESOS.md"
_DATOS_PATH          = Path(__file__).parent / "datos" / "Base Haciendas Depurada.csv"
_DATOS_PROCESADO_PATH= Path(__file__).parent / "datos" / "Base Haciendas Depurada_procesado.csv"
_SCHEMA_PATH         = Path(__file__).parent / "informacion" / "SCHEMA.md"
_ONTOLOGIA_PATH      = Path(__file__).parent / "informacion" / "ONTOLOGIA.md"


# ─── autenticación ────────────────────────────────────────────────────────────

def _cargar_accesos() -> list[tuple[str, str]]:
    if not _ACCESOS_PATH.exists():
        return []
    accesos = []
    for line in _ACCESOS_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        celdas = [c.strip() for c in line.split("|") if c.strip()]
        if len(celdas) < 2:
            continue
        usuario, password = celdas[0], celdas[1]
        if usuario.lower() == "usuario" or set(usuario) <= {"-"}:
            continue
        accesos.append((usuario, password))
    return accesos


def _verificar_credenciales(usuario: str, password: str) -> bool:
    for u, p in _cargar_accesos():
        if usuario.strip() == u and password == p:
            return True
    return False


if not st.session_state.get("autenticado"):
    st.markdown("""
    <style>
    #MainMenu, footer { visibility: hidden; }
    [data-testid="stToolbar"]    { display: none !important; }
    [data-testid="stDecoration"] { display: none !important; }
    header[data-testid="stHeader"] { background: transparent !important; border: none !important; }

    [data-testid="stApp"] {
        background-image:
            linear-gradient(rgba(0,0,0,0.52), rgba(0,0,0,0.65)),
            url('https://images.unsplash.com/photo-1643892343740-77e5efda67de?w=1920&q=80');
        background-size: cover;
        background-position: center;
        background-attachment: fixed;
    }

    /* Labels de inputs */
    [data-testid="stTextInput"] label,
    [data-testid="stTextInput"] label p {
        color: #ffffff !important;
        text-shadow: 0 1px 6px rgba(0,0,0,0.9) !important;
        font-weight: 500 !important;
        font-size: 0.9rem !important;
    }

    /* Campos de texto */
    [data-testid="stTextInput"] input {
        background: rgba(255,255,255,0.90) !important;
        border: none !important;
        border-radius: 8px !important;
        color: #1e293b !important;
        font-size: 0.95rem !important;
    }

    /* Botón Ingresar */
    [data-testid="stButton"] > button {
        background: #4f46e5 !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        font-size: 0.95rem !important;
        padding: 0.55rem 2.5rem !important;
        margin-top: 0.4rem !important;
        box-shadow: 0 4px 14px rgba(79,70,229,0.45) !important;
    }
    [data-testid="stButton"] > button:hover {
        background: #4338ca !important;
    }

    /* Mensaje de error */
    [data-testid="stAlert"] {
        background: rgba(239,68,68,0.18) !important;
        border: 1px solid rgba(239,68,68,0.4) !important;
        border-radius: 8px !important;
        color: #fecaca !important;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="margin-bottom:1.8rem">
        <div style="
            font-size: 2.2rem;
            font-weight: 800;
            color: #ffffff;
            text-shadow: 0 2px 20px rgba(0,0,0,1), 0 1px 6px rgba(0,0,0,0.9);
            letter-spacing: -0.02em;
            line-height: 1.2;
        ">🍌 Análisis de Haciendas</div>
        <div style="
            color: #cbd5e1;
            font-size: 0.92rem;
            margin-top: 0.5rem;
            text-shadow: 0 1px 6px rgba(0,0,0,0.9);
        ">Ingresa tus credenciales para continuar</div>
    </div>
    """, unsafe_allow_html=True)
    usuario = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")
    if st.button("Ingresar", type="primary"):
        if _verificar_credenciales(usuario, password):
            st.session_state["autenticado"] = True
            st.session_state["usuario"] = usuario
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos.")
    st.stop()


# ─── sesión activa ────────────────────────────────────────────────────────────

_sid = sesion_activa_id()
if _sid is None or obtener_sesion(_sid) is None:
    _sid = crear_sesion()

# ─── menú lateral ─────────────────────────────────────────────────────────────

st.markdown("""
<style>
/* ══ chrome de streamlit ══════════════════════════════════════════════════ */
#MainMenu, footer { visibility: hidden; }
[data-testid="stToolbar"]   { display: none !important; }
[data-testid="stDecoration"] { display: none !important; }

/* ══ sidebar: fondo oscuro ════════════════════════════════════════════════ */
section[data-testid="stSidebar"],
section[data-testid="stSidebar"] > div:first-child {
    background-color: #0f172a !important;
}

/* título */
section[data-testid="stSidebar"] h3 {
    color: #f1f5f9 !important;
    font-weight: 700 !important;
    font-size: 1.05rem !important;
    letter-spacing: -0.01em;
}

/* caption usuario */
section[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p,
section[data-testid="stSidebar"] small {
    color: #64748b !important;
}

/* dividers */
section[data-testid="stSidebar"] hr {
    border-color: #1e293b !important;
    margin: 10px 0 !important;
}

/* ══ section headers ══════════════════════════════════════════════════════ */
.nav-section {
    font-size: 0.62rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: #86efac;
    margin: 18px 0 2px 10px;
}

/* ══ gap fix ══════════════════════════════════════════════════════════════ */
section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
    gap: 0 !important;
}
section[data-testid="stSidebar"] [data-testid="element-container"] {
    margin: 0 !important;
    padding: 0 !important;
}
section[data-testid="stSidebar"] .stButton {
    margin: 0 !important;
}

/* ══ nav buttons ══════════════════════════════════════════════════════════ */
section[data-testid="stSidebar"] .stButton > button {
    background: transparent !important;
    border: none !important;
    text-align: left !important;
    justify-content: flex-start !important;
    font-size: 0.83rem !important;
    color: #94a3b8 !important;
    padding: 0.3rem 0.8rem !important;
    box-shadow: none !important;
    border-radius: 6px !important;
    transition: background 0.12s, color 0.12s !important;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(99,102,241,0.15) !important;
    color: #c7d2fe !important;
}
/* ítem activo (marcado con ▸) */
section[data-testid="stSidebar"] .stButton > button[kind="secondary"]:focus,
section[data-testid="stSidebar"] .stButton > button:active {
    background: rgba(99,102,241,0.22) !important;
    color: #a5b4fc !important;
}

/* ══ main: tipografía y espaciado ════════════════════════════════════════ */
.main .block-container {
    padding-top: 1.8rem !important;
    max-width: 1050px !important;
}

/* ══ info box superior (fechas) ══════════════════════════════════════════ */
[data-testid="stInfo"] {
    background: #eff6ff !important;
    border: 1px solid #bfdbfe !important;
    border-left: 4px solid #3b82f6 !important;
    border-radius: 8px !important;
    color: #1e40af !important;
    font-size: 0.9rem !important;
}

/* ══ success box (interpretación) ════════════════════════════════════════ */
[data-testid="stSuccess"] {
    background: #f0fdf4 !important;
    border: 1px solid #bbf7d0 !important;
    border-left: 4px solid #22c55e !important;
    border-radius: 8px !important;
    font-size: 0.95rem !important;
    line-height: 1.6 !important;
}

/* ══ expander ════════════════════════════════════════════════════════════ */
[data-testid="stExpander"] {
    border: 1px solid #e2e8f0 !important;
    border-radius: 8px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06) !important;
}

/* ══ botón primario ══════════════════════════════════════════════════════ */
.stButton > button[kind="primary"] {
    background: #4f46e5 !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    letter-spacing: 0.01em !important;
    transition: background 0.15s !important;
}
.stButton > button[kind="primary"]:hover {
    background: #4338ca !important;
    color: #ffffff !important;
}

/* ══ botón Salir (anidado en container propio) ═══════════════════════════ */
section[data-testid="stSidebar"] [data-testid="stVerticalBlock"]
    [data-testid="stVerticalBlock"] .stButton > button {
    color: #fca5a5 !important;
}
section[data-testid="stSidebar"] [data-testid="stVerticalBlock"]
    [data-testid="stVerticalBlock"] .stButton > button:hover {
    background: rgba(239,68,68,0.18) !important;
    color: #f87171 !important;
}
</style>
""", unsafe_allow_html=True)

if "nav" not in st.session_state:
    st.session_state["nav"] = "consultas"

def _nav(label: str, key: str):
    active = st.session_state["nav"] == key
    prefix = "▸ " if active else "   "
    if st.button(f"{prefix}{label}", key=f"navbtn_{key}", width='stretch'):
        st.session_state["nav"] = key
        st.rerun()

with st.sidebar:
    st.markdown("### 🍌 Haciendas")
    st.caption(f"👤 {st.session_state.get('usuario', '')}")
    st.divider()

    st.markdown('<p class="nav-section">Sesiones</p>', unsafe_allow_html=True)
    for _s in listar_sesiones():
        _activo = _s["id"] == _sid
        _label = f"{'▸ ' if _activo else '   '}{_s['nombre']}"
        if st.button(_label, key=f"ses_{_s['id']}", width='stretch'):
            cambiar_sesion(_s["id"])
            st.rerun()
    if st.button("＋ Nueva sesión", key="nueva_sesion", width='stretch'):
        crear_sesion()
        st.rerun()
    st.divider()

    st.markdown('<p class="nav-section">Consultas</p>', unsafe_allow_html=True)
    _nav("Preguntar a los datos", "consultas")

    st.markdown('<p class="nav-section">Datos</p>', unsafe_allow_html=True)
    _nav("Subir datos", "datos_subir")
    _nav("Bajar datos", "datos_bajar")
    _nav("Ver datos", "datos_ver")

    st.markdown('<p class="nav-section">Ontología</p>', unsafe_allow_html=True)
    _nav("Subir ontología", "ontologia_subir")
    _nav("Bajar ontología", "ontologia_bajar")
    _nav("Ver ontología", "ontologia_ver")

    st.markdown('<p class="nav-section">Esquema</p>', unsafe_allow_html=True)
    _nav("Subir esquema", "esquema_subir")
    _nav("Bajar esquema", "esquema_bajar")
    _nav("Ver esquema", "esquema_ver")

    st.divider()
    with st.container():
        if st.button("Salir", width='stretch'):
            st.session_state.clear()
            st.rerun()

nav = st.session_state["nav"]

_MESES_ES = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
}

_fecha_min = _pipeline.DF_GLOBAL["FECHA"].min()
_fecha_max = _pipeline.DF_GLOBAL["FECHA"].max()
st.info(
    f"**Base Haciendas Depuradas.csv** — "
    f"Datos disponibles: **{_MESES_ES[_fecha_min.month]} {_fecha_min.year}** "
    f"— **{_MESES_ES[_fecha_max.month]} {_fecha_max.year}**"
)

with st.expander("Bienvenido al agente de consultas", expanded=not st.session_state.get("bienvenida_leida", False)):
    st.session_state["bienvenida_leida"] = True
    st.markdown(f"""
**Bienvenido al agente de consultas de Base Haciendas Depurada.csv.**

Este agente asume que el día de hoy es el último día que aparece en la data
(**{_MESES_ES[_fecha_max.month]} {_fecha_max.year}**), por lo que cualquier referencia temporal relativa
("últimos 6 meses", "año en curso") se calcula hacia atrás desde esa fecha.

El agente trabaja con tres archivos de contexto que pueden actualizarse desde el menú lateral:
- **Ontología** — describe el negocio, sus procesos y métricas clave.
- **Esquema** — documenta cada columna de la tabla.
- **Datos** — el archivo CSV con los registros mensuales de cada hacienda.

> Entre mejor definida sea la ontología, mejores serán las respuestas del agente.

---

**Pirámide analítica — qué puede responder este agente:**

| Nivel | Tipo | Ejemplo |
|---|---|---|
| 1 · Descriptivo ✅ | ¿Qué pasó? | ¿Cuál fue el costo promedio por hectárea en 2024? |
| 2 · Diagnóstico | ¿Por qué pasó? | ¿Por qué subieron los costos en la zona Norte? |
| 3 · Predictivo | ¿Qué pasará? | ¿Cuál será el costo por hectárea en 2028? |
| 4 · Prescriptivo | ¿Qué debo hacer? | ¿Qué acciones debo tomar para bajar el costo en 2028? |

Por ahora, y por razones de demo, el agente responde con comodidad preguntas **descriptivas** —
aquellas que se refieren al pasado o al período disponible en los datos.
Los niveles 2, 3 y 4 también pueden abordarse, pero requieren una **ontología más elaborada**
que incluya relaciones causales, modelos de negocio y proyecciones.

[**Predizer.ai**](https://predizer.ai) puede ayudarte a desarrollar esas ontologías y llevar el agente al siguiente nivel.
""")



# ─── datos ────────────────────────────────────────────────────────────────────

if nav == "datos_subir":
    st.title("Subir datos")
    st.markdown("Sube el archivo **Base Haciendas Depurada.csv** para reemplazar los datos actuales.")

    archivo = st.file_uploader("Selecciona el archivo CSV", type=["csv"])
    if archivo is not None:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.info(f"Archivo seleccionado: **{archivo.name}** ({archivo.size:,} bytes)")
        with col2:
            if st.button("Guardar en el sistema", type="primary", width='stretch'):
                _DATOS_PATH.parent.mkdir(parents=True, exist_ok=True)
                _DATOS_PATH.write_bytes(archivo.getvalue())
                enriquecer_csv(_DATOS_PATH, _DATOS_PROCESADO_PATH)
                importlib.reload(_pipeline)
                actualizar_cobertura_temporal(_pipeline.DF_GLOBAL)
                inicio = _pipeline.DF_GLOBAL["FECHA"].min().strftime("%B %Y")
                fin    = _pipeline.DF_GLOBAL["FECHA"].max().strftime("%B %Y")
                st.success(
                    f"Archivo guardado y enriquecido. "
                    f"Columnas **FECHA_mes** y **FECHA_ano** agregadas. "
                    f"Cobertura: **{inicio}** → **{fin}**."
                )
                st.rerun()

elif nav == "datos_bajar":
    st.title("Bajar datos")
    st.markdown("Descarga el archivo original sin procesar.")

    if _DATOS_PATH.exists():
        stat = _DATOS_PATH.stat()
        st.info(
            f"Archivo: **{_DATOS_PATH.name}**  \n"
            f"Tamaño: {stat.st_size:,} bytes  \n"
            f"Filas: {len(_pipeline.DF_GLOBAL):,}"
        )
        st.download_button(
            label="Descargar Base Haciendas Depurada.csv",
            data=_DATOS_PATH.read_bytes(),
            file_name="Base Haciendas Depurada.csv",
            mime="text/csv",
            type="primary",
            width='stretch',
        )
    else:
        st.warning("No hay ningún archivo de datos en el sistema.")

elif nav == "datos_ver":
    st.title("Ver datos")
    df = _pipeline.DF_GLOBAL
    st.caption(f"{df.shape[0]:,} filas × {df.shape[1]} columnas")
    st.dataframe(df, width='stretch', height=600)


# ─── ontología ────────────────────────────────────────────────────────────────

elif nav == "ontologia_subir":
    st.title("Subir ontología")
    st.markdown("Sube un nuevo archivo **ONTOLOGIA.md** para reemplazar el actual.")

    archivo = st.file_uploader("Selecciona el archivo Markdown", type=["md"])
    if archivo is not None:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.info(f"Archivo seleccionado: **{archivo.name}** ({archivo.size:,} bytes)")
        with col2:
            if st.button("Guardar", type="primary", width='stretch'):
                _ONTOLOGIA_PATH.write_bytes(archivo.getvalue())
                st.success("ONTOLOGIA.md actualizada correctamente.")
                st.rerun()

elif nav == "ontologia_bajar":
    st.title("Bajar ontología")
    if _ONTOLOGIA_PATH.exists():
        st.download_button(
            label="Descargar ONTOLOGIA.md",
            data=_ONTOLOGIA_PATH.read_bytes(),
            file_name="ONTOLOGIA.md",
            mime="text/markdown",
            type="primary",
            width='stretch',
        )
    else:
        st.warning("Archivo ONTOLOGIA.md no encontrado.")

elif nav == "ontologia_ver":
    st.title("Ontología del negocio")
    if _ONTOLOGIA_PATH.exists():
        st.markdown(_ONTOLOGIA_PATH.read_text(encoding="utf-8"))
    else:
        st.warning("Archivo ONTOLOGIA.md no encontrado.")


# ─── esquema ──────────────────────────────────────────────────────────────────

elif nav == "esquema_subir":
    st.title("Subir esquema")
    st.markdown("Sube un nuevo archivo **SCHEMA.md** para reemplazar el actual.")

    archivo = st.file_uploader("Selecciona el archivo Markdown", type=["md"])
    if archivo is not None:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.info(f"Archivo seleccionado: **{archivo.name}** ({archivo.size:,} bytes)")
        with col2:
            if st.button("Guardar", type="primary", width='stretch'):
                _SCHEMA_PATH.write_bytes(archivo.getvalue())
                st.success("SCHEMA.md actualizado correctamente.")
                st.rerun()

elif nav == "esquema_bajar":
    st.title("Bajar esquema")
    if _SCHEMA_PATH.exists():
        st.download_button(
            label="Descargar SCHEMA.md",
            data=_SCHEMA_PATH.read_bytes(),
            file_name="SCHEMA.md",
            mime="text/markdown",
            type="primary",
            width='stretch',
        )
    else:
        st.warning("Archivo SCHEMA.md no encontrado.")

elif nav == "esquema_ver":
    st.title("Esquema de la tabla")
    if _SCHEMA_PATH.exists():
        st.markdown(_SCHEMA_PATH.read_text(encoding="utf-8"))
    else:
        st.warning("Archivo SCHEMA.md no encontrado.")


# ─── consultas ────────────────────────────────────────────────────────────────

elif nav == "consultas":
    _sesion_actual = obtener_sesion(_sid)
    _nombre_sesion = _sesion_actual["nombre"] if _sesion_actual else "Sesión"
    st.title(_nombre_sesion)

    # Historial persistido en base de datos (preguntas de reruns anteriores)
    if _sesion_actual:
        _interacciones = _sesion_actual.get("interacciones", [])
        _resumen = _sesion_actual.get("resumen")
        if _resumen or _interacciones:
            with st.expander(
                f"Historial ({len(_interacciones)} pregunta{'s' if len(_interacciones) != 1 else ''} anterior{'es' if len(_interacciones) != 1 else ''})",
                expanded=False,
            ):
                if _resumen:
                    st.info(f"**Resumen:** {_resumen}")
                for _inter in _interacciones:
                    st.markdown(f"**Pregunta:** {_inter['pregunta']}")
                    st.markdown(f"**Respuesta:** {_inter['interpretacion']}")
                    st.divider()

    # Conversación en memoria para esta sesión (se acumula durante la visita)
    if "conversacion" not in st.session_state or st.session_state.get("_sid_conv") != _sid:
        st.session_state["conversacion"] = []
        st.session_state["_sid_conv"] = _sid

    conversacion = st.session_state["conversacion"]

    # ─── renderizar turnos completados ────────────────────────────────────────
    for i, turno in enumerate(conversacion):

        # Pregunta
        st.markdown(
            f"<div style='background:#f8fafc;border-left:3px solid #6366f1;"
            f"padding:8px 12px;border-radius:6px;margin-bottom:8px;font-size:0.97rem'>"
            f"<strong>Pregunta {i+1}:</strong> {turno['pregunta_original']}</div>",
            unsafe_allow_html=True,
        )
        if turno.get("pregunta_final") and turno["pregunta_final"].strip() != turno["pregunta_original"].strip():
            st.info(f"**Pregunta entendida:** {turno['pregunta_final']}")

        # Pasos en acordeón
        n_pasos = len(turno["pasos_acumulados"])
        with st.expander(f"Ver análisis — {n_pasos} paso{'s' if n_pasos != 1 else ''}", expanded=False):
            for j, item in enumerate(turno["pasos_acumulados"], 1):
                paso = item["paso"]
                vers = item["vers"]
                cors = item["cors"]
                n_correcciones = sum(1 for v in vers if v.get("estado") != "ok")
                titulo_paso = f"Paso {j} — {paso['actividad'][:80]}"
                if n_correcciones:
                    titulo_paso += f" ({n_correcciones} corrección{'es' if n_correcciones > 1 else ''})"
                with st.expander(titulo_paso, expanded=False):
                    if paso["razonamiento"]:
                        st.markdown(f"**Razonamiento:** {paso['razonamiento']}")
                    st.code(paso["codigo"], language="python")
                    df_paso = paso["df_resultado"]
                    st.caption(f"{df_paso.shape[0]} filas × {df_paso.shape[1]} columnas")
                    st.dataframe(df_paso, width='stretch')
                    if n_correcciones:
                        with st.expander(f"Historial de verificación ({len(vers)} iteraciones)"):
                            for ver in vers:
                                if ver.get("estado") == "ok":
                                    st.success(f"Iteración {ver['intento']}: código correcto")
                                else:
                                    tipo_err = "Error estático" if ver.get("estado") == "error_estatico" else "Error de ejecución"
                                    st.error(f"Iteración {ver['intento']} — {tipo_err}:\n{ver['errores']}")
                                    cor = next((c for c in cors if c["intento"] == ver["intento"]), None)
                                    if cor:
                                        st.caption("Código corregido:")
                                        st.code(cor["codigo"], language="python")

        # Respuesta
        st.success(turno["interpretacion"])

        # Datos y descarga
        st.dataframe(turno["df_final"], width='stretch')
        csv_bytes = turno["df_final"].to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Descargar CSV",
            data=csv_bytes,
            file_name=f"resultado_{i+1}.csv",
            mime="text/csv",
            key=f"dl_{i}",
        )

        st.divider()

    # ─── campo de nueva pregunta (siempre al final) ───────────────────────────
    if not conversacion:
        st.markdown("Haz una pregunta en lenguaje natural sobre los datos de producción y costos.")
        ejemplos = [
            "¿Cuáles son las 5 haciendas con mayor costo por hectárea en 2024?",
            "¿Qué haciendas tienen el menor costo por caja en los últimos 6 meses?",
            "¿Cuál es la tendencia mensual de producción total (Total_Cajas) en 2024?",
            "¿Qué zonas tienen mayor incidencia de Sigatoka?",
        ]
        with st.expander("Ver preguntas de ejemplo"):
            for e in ejemplos:
                if st.button(e, key=e):
                    st.session_state["pregunta_input"] = e
                    st.rerun()

    if st.session_state.pop("_clear_pregunta", False):
        st.session_state["pregunta_input"] = ""
    elif "pregunta_input" not in st.session_state:
        st.session_state["pregunta_input"] = ""

    pregunta = st.text_area(
        f"Pregunta {len(conversacion) + 1}" if conversacion else "Pregunta",
        key="pregunta_input",
        placeholder="¿Cuáles son las 5 haciendas con mayor costo por hectárea en 2024?",
        height=80,
    )

    ejecutar = st.button("Analizar", type="primary", disabled=not pregunta.strip())

    if ejecutar and pregunta.strip():

        pasos_acumulados = []
        estado = {"total_pasos": 0}

        try:
            _contexto = obtener_contexto(_sid)
            if _contexto:
                with st.spinner("Entendiendo el contexto de la sesión..."):
                    pregunta_final = sintetizar(pregunta, _contexto)
            else:
                pregunta_final = pregunta

            with st.status("Analizando...", expanded=True) as status:

                def on_evento(evento: dict):
                    tipo = evento["tipo"]
                    if tipo == "plan_listo":
                        estado["total_pasos"] = len(evento["pasos"])
                        st.write(f"Plan listo: {estado['total_pasos']} pasos")
                    elif tipo == "paso_inicio":
                        n, actividad = evento["numero"], evento["actividad"]
                        st.write(f"Paso {n} de {estado['total_pasos']}: {actividad[:80]}...")
                        pasos_acumulados.append({"paso": None, "vers": [], "cors": []})
                    elif tipo == "verificacion":
                        pasos_acumulados[-1]["vers"].append(evento)
                    elif tipo == "correccion":
                        pasos_acumulados[-1]["cors"].append(evento)
                    elif tipo == "paso_listo":
                        pasos_acumulados[-1]["paso"] = evento["paso"]

                _, _, df_final = _pipeline.ejecutar_pipeline(pregunta_final, callback=on_evento)
                status.update(label="Análisis completado", state="complete", expanded=False)

            with st.spinner("Interpretando resultados..."):
                interpretacion = interpretar(pregunta_final, df_final)

            agregar_interaccion(_sid, pregunta_final, interpretacion, df=df_final)

            conversacion.append({
                "pregunta_original": pregunta,
                "pregunta_final": pregunta_final,
                "interpretacion": interpretacion,
                "df_final": df_final,
                "pasos_acumulados": pasos_acumulados,
            })

            st.session_state["_clear_pregunta"] = True
            st.rerun()

        except Exception as e:
            st.error(f"Error durante el análisis: {e}")
            raise

