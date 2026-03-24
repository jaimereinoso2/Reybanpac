import os
import streamlit as st
from dotenv import load_dotenv
from pipeline_csv import ejecutar_pipeline

load_dotenv()

st.set_page_config(
    page_title="Análisis de Haciendas",
    page_icon="🍌",
    layout="wide",
)


def _secret(key: str):
    try:
        return st.secrets[key]
    except Exception:
        return os.getenv(key)


def _verificar_credenciales(usuario: str, password: str) -> bool:
    i = 1
    while True:
        u = _secret(f"USUARIO{i}")
        p = _secret(f"USUARIO{i}_PASS")
        if u is None:
            break
        if usuario.strip() == u and password == p:
            return True
        i += 1
    return False


if not st.session_state.get("autenticado"):
    st.title("🔐 Acceso")
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


st.title("🍌 Análisis de Haciendas")
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

if "pregunta_input" not in st.session_state:
    st.session_state["pregunta_input"] = ""

pregunta = st.text_area(
    "Pregunta",
    key="pregunta_input",
    placeholder="¿Cuáles son las 5 haciendas con mayor costo por hectárea en 2024?",
    height=80,
)

ejecutar = st.button("Analizar", type="primary", disabled=not pregunta.strip())

if ejecutar and pregunta.strip():
    plan_placeholder = st.empty()
    pasos_placeholders = {}
    verificaciones_por_paso = {}  # {n: [evento, ...]}
    correcciones_por_paso = {}    # {n: [evento, ...]}

    def on_evento(evento: dict):
        tipo = evento["tipo"]

        if tipo == "plan_inicio":
            plan_placeholder.info("Generando plan de análisis...")

        elif tipo == "plan_listo":
            pasos = evento["pasos"]
            with plan_placeholder.container():
                st.subheader("Plan de análisis")
                for i, p in enumerate(pasos, 1):
                    st.write(f"**{i}.** {p}")

        elif tipo == "paso_inicio":
            n = evento["numero"]
            actividad = evento["actividad"]
            pasos_placeholders[n] = st.empty()
            verificaciones_por_paso[n] = []
            correcciones_por_paso[n] = []
            pasos_placeholders[n].info(f"⏳ Paso {n}: {actividad[:90]}...")

        elif tipo == "verificacion":
            n = evento["numero"]
            verificaciones_por_paso[n].append(evento)
            if evento["estado"] in ("error_estatico", "error_ejecucion"):
                pasos_placeholders[n].warning(
                    f"🔍 Paso {n} — iteración {evento['intento']}: error detectado, corrigiendo..."
                )

        elif tipo == "correccion":
            n = evento["numero"]
            correcciones_por_paso[n].append(evento)

        elif tipo == "paso_listo":
            n = evento["numero"]
            paso = evento["paso"]
            vers = verificaciones_por_paso.get(n, [])
            cors = correcciones_por_paso.get(n, [])
            n_correcciones = sum(1 for v in vers if v["estado"] != "ok")

            titulo = f"✅ Paso {n} — {paso['actividad'][:80]}"
            if n_correcciones > 0:
                titulo += f" ({n_correcciones} corrección{'es' if n_correcciones > 1 else ''})"

            with pasos_placeholders[n].container():
                with st.expander(titulo, expanded=(n == 1)):
                    if paso["razonamiento"]:
                        st.markdown(f"**Razonamiento:** {paso['razonamiento']}")
                    st.code(paso["codigo"], language="python")
                    df_paso = paso["df_resultado"]
                    st.caption(f"{df_paso.shape[0]} filas × {df_paso.shape[1]} columnas")
                    st.dataframe(df_paso, use_container_width=True)

                    # Historial de verificaciones y correcciones
                    if n_correcciones > 0:
                        with st.expander(f"🔍 Historial de verificación ({len(vers)} iteraciones)"):
                            for ver in vers:
                                if ver["estado"] == "ok":
                                    st.success(f"Iteración {ver['intento']}: código correcto")
                                else:
                                    tipo_err = "Error estático" if ver["estado"] == "error_estatico" else "Error de ejecución"
                                    st.error(f"Iteración {ver['intento']} — {tipo_err}:\n{ver['errores']}")
                                    cor = next(
                                        (c for c in cors if c["intento"] == ver["intento"]), None
                                    )
                                    if cor:
                                        st.caption("Código corregido:")
                                        st.code(cor["codigo"], language="python")

        elif tipo == "pipeline_listo":
            pass

    try:
        pasos_texto, pasos_resultado, df_final = ejecutar_pipeline(pregunta, callback=on_evento)

        st.divider()
        st.subheader("Resultado final")
        st.dataframe(df_final, use_container_width=True)

        csv = df_final.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Descargar CSV",
            data=csv,
            file_name="resultado.csv",
            mime="text/csv",
        )

    except Exception as e:
        st.error(f"Error durante el análisis: {e}")
        raise
