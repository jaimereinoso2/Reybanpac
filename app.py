import streamlit as st
from pipeline import ejecutar_pipeline

st.set_page_config(
    page_title="Análisis de Haciendas",
    page_icon="🍌",
    layout="wide",
)

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

    eventos_recibidos = []

    def on_evento(evento: dict):
        eventos_recibidos.append(evento)
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
            subtipo = evento["subtipo"].upper()
            actividad = evento["actividad"]
            pasos_placeholders[n] = st.empty()
            pasos_placeholders[n].info(f"⏳ Paso {n} ({subtipo}): {actividad[:90]}...")

        elif tipo == "paso_listo":
            n = evento["numero"]
            paso = evento["paso"]
            subtipo = paso["tipo"].upper()
            lang = "sql" if paso["tipo"] == "sql" else "python"
            with pasos_placeholders[n].container():
                with st.expander(
                    f"✅ Paso {n} ({subtipo}) — {paso['actividad'][:80]}",
                    expanded=(n == 1),
                ):
                    if paso["razonamiento"]:
                        st.markdown(f"**Razonamiento:** {paso['razonamiento']}")
                    st.code(paso["codigo"], language=lang)
                    df_paso = paso["df_resultado"]
                    st.caption(f"{df_paso.shape[0]} filas × {df_paso.shape[1]} columnas")
                    st.dataframe(df_paso, use_container_width=True)

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
