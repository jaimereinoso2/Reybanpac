#!/usr/bin/env python3
"""
Agente inteligente de análisis de datos con Gemini
Analiza datos de haciendas bananas mediante preguntas en lenguaje natural

Uso:
    python 01_demo.py
"""

import io
import os
import sys
import subprocess
from dotenv import load_dotenv

import pandas as pd
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_google_genai import ChatGoogleGenerativeAI


# ============================================================================
# 1. CONFIGURACIÓN INICIAL
# ============================================================================

def setup_environment():
    """Cargar variables de entorno y verificar dependencias."""
    # Cargar variables del archivo .env
    load_dotenv()
    
    # Verificar que las dependencias estén instaladas
    try:
        import langchain
        import langchain_core
    except ImportError:
        print("📦 Instalando dependencias...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "langchain", "langchain-core", "langchain-google-genai"])
    
    print("✅ Entorno configurado correctamente\n")


def load_data(filepath: str) -> pd.DataFrame:
    """
    Cargar datos del CSV.
    
    Args:
        filepath: Ruta del archivo CSV
        
    Returns:
        DataFrame con los datos cargados
    """
    df = pd.read_csv(filepath, sep=';', decimal=',')
    print(f"📊 Datos cargados: {df.shape[0]} filas, {df.shape[1]} columnas")
    return df


# ============================================================================
# 2. CONFIGURACIÓN DEL AGENTE GEMINI
# ============================================================================

def setup_agent(df: pd.DataFrame):
    """
    Configurar el agente con Google Gemini.
    
    Args:
        df: DataFrame con los datos
        
    Returns:
        Tupla con (agent_chain, columns_info)
    """
    # Obtener clave API
    gemini_key = os.getenv("GEMINI_API_KEY")
    if not gemini_key:
        raise ValueError("⚠️  GEMINI_API_KEY no configurada en .env")
    
    # Obtener modelo (por defecto gemini-2.0-flash)
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
    
    # Crear el prompt del agente
    prompt_template = PromptTemplate(
        input_variables=["question", "columns_info"],
        template="""Eres un experto en análisis de datos con pandas. 
Tienes acceso a un DataFrame llamado 'df' con datos sobre haciendas.

ESTRUCTURA DEL DATAFRAME:
{columns_info}

Basándote en la siguiente pregunta, genera SOLO código Python que resuelva la pregunta.
El código debe:
1. Usar el DataFrame 'df' que ya está disponible
2. Buscar las columnas relevantes en el DataFrame (pueden tener nombres similares a los mencionados)
3. Retornar un resultado (print, o guardar en una variable)
4. Ser ejecutable directamente con exec()
5. Si no encuentra columnas exactas, usa las que mejor se ajusten al contexto

IMPORTANTE: Primero explora df.columns para encontrar los nombres exactos de las columnas

Pregunta: {question}

Retorna SOLO el código Python, sin explicaciones adicionales, sin markdown, sin triple backticks."""
    )
    
    # Configurar Gemini
    llm = ChatGoogleGenerativeAI(
        model=gemini_model,
        temperature=0,
        google_api_key=gemini_key
    )
    print(f"✅ Usando Google {gemini_model}\n")
    
    # Obtener información de columnas
    columns_info = (
        f"Columnas disponibles: {list(df.columns)}\n"
        f"Dtypes: {df.dtypes.to_dict()}\n"
        f"Forma: {df.shape}"
    )
    
    # Crear cadena LLM
    output_parser = StrOutputParser()
    agent_chain = prompt_template | llm | output_parser
    
    return agent_chain, columns_info


# ============================================================================
# 3. FUNCIONES DE EJECUCIÓN
# ============================================================================

def agent_generate_code(agent_chain, question: str, columns_info: str) -> str:
    """
    Generar código Python usando el agente.
    
    Args:
        agent_chain: Cadena del agente LLM
        question: Pregunta en lenguaje natural
        columns_info: Información de las columnas del DataFrame
        
    Returns:
        Código Python generado
    """
    code = agent_chain.invoke({"question": question, "columns_info": columns_info})
    return code.strip()


def execute_code(code_string: str, df_context: pd.DataFrame) -> str:
    """
    Ejecutar código Python generado de forma segura.
    
    Args:
        code_string: Código a ejecutar
        df_context: DataFrame para el contexto
        
    Returns:
        Resultado de la ejecución
    """
    # Capturar la salida
    captured_output = io.StringIO()
    original_stdout = sys.stdout
    
    try:
        sys.stdout = captured_output
        
        local_vars = {
            'df': df_context,
            'pd': pd,
            'result': None,
            '__builtins__': __builtins__
        }
        
        # Intentar eval primero
        try:
            result_value = eval(code_string.strip(), {"pd": pd, "df": df_context})
            sys.stdout = original_stdout
            
            if result_value is not None:
                return str(result_value)
        except:
            # Si eval falla, intentar exec
            sys.stdout = captured_output
            exec(code_string, {"__builtins__": __builtins__, "pd": pd, "df": df_context}, local_vars)
            sys.stdout = original_stdout
        
        # Obtener salida capturada
        output = captured_output.getvalue()
        
        if output:
            return output.strip()
        elif local_vars.get('result') is not None:
            return str(local_vars['result'])
        else:
            return "✅ Código ejecutado correctamente"
            
    except Exception as e:
        sys.stdout = original_stdout
        return f"❌ Error al ejecutar: {str(e)}"
    finally:
        sys.stdout = original_stdout


def query_and_execute(agent_chain: object, columns_info: str, question: str, df: pd.DataFrame) -> tuple:
    """
    Ejecutar ciclo completo: generar código y ejecutarlo.
    
    Args:
        agent_chain: Cadena del agente
        columns_info: Información de columnas
        question: Pregunta a responder
        df: DataFrame con datos
        
    Returns:
        Tupla (código_generado, resultado)
    """
    print(f"💡 Pregunta: {question}\n")
    
    # Generar código
    print("🤖 Generando código con LangChain...")
    generated_code = agent_generate_code(agent_chain, question, columns_info)
    
    print(f"📝 Código generado:\n{generated_code}\n")
    
    # Ejecutar código
    print("▶️  Ejecutando código...")
    try:
        result = execute_code(generated_code, df)
        print(f"✅ Resultado:\n{result}")
        return generated_code, result
    except Exception as e:
        print(f"❌ Error al ejecutar: {str(e)}")
        return generated_code, f"Error: {str(e)}"


# ============================================================================
# 4. PROGRAMA PRINCIPAL
# ============================================================================

def main():
    """Función principal - ejecutar análisis."""
    
    # Configurar entorno
    setup_environment()
    
    # Cargar datos
    print("📂 Cargando datos...\n")
    df = load_data('./datos/Base Haciendas Depurada.csv')
    print(df.head())
    print()
    
    # Configurar agente
    print("🔧 Configurando agente Gemini...\n")
    agent_chain, columns_info = setup_agent(df)
    
    # Definir preguntas
    preguntas = [
        "Si consideramos el cumplimiento del 100% de los programas en todas las haciendas, ¿cuál sería el costo/caja y costo/hectárea real?",
        "Considerando las haciendas con las mismas variables (tamaño de la hacienda, certificaciones, numero de procesos, condición fitosanitaria, tipo de plagas, condiciones de suelo), ¿cuál es el ranking de haciendas en las diferentes categorías?",
        "¿Cuáles son las variables que más influyen en cada hacienda en sus niveles de costos?",
        "¿A qué se puede atribuir la tendencia actual en los indicadores de producción (merma, peso, productividad)?",
        "¿Qué debería de ajustarse a nivel de producción y costos, para reducir el costo las próximas semanas?",
        "¿Qué practicas tienen las haciendas con menor costo, que pueden ser replicadas en las demás haciendas?",
        "¿Qué variables administrativas, pueden estar afectando los costos?",
        "¿Hay algún cambio en los programas de atención a la plantación que puedan estar afectando negativamente al costo?",
        "¿Qué ha influido en la reducción de costos de las haciendas que han mejorado en los últimos 3 meses?",
        "¿Qué programa se debería de ajustar para mejorar la productividad?"
    ]
    
    # Ejecutar análisis
    print("\n" + "="*80)
    print("🚀 ANÁLISIS: 10 Preguntas de Inteligencia Artificial")
    print("="*80 + "\n")
    
    resultados = []
    for i, pregunta in enumerate(preguntas, 1):
        print(f"\n📌 PREGUNTA {i}/10:")
        code, resultado = query_and_execute(agent_chain, columns_info, pregunta, df)
        resultados.append({
            'pregunta': pregunta,
            'codigo': code,
            'resultado': resultado
        })
        print("\n" + "-"*80)
    
    # Resumen
    print("\n" + "="*80)
    print("📊 RESUMEN DE ANÁLISIS")
    print("="*80 + "\n")
    
    exitosos = sum(1 for r in resultados if "Error" not in r['resultado'] and "error" not in r['resultado'])
    con_error = len(resultados) - exitosos
    
    print(f"✅ Análisis exitosos: {exitosos}/{len(resultados)}")
    print(f"❌ Análisis con error: {con_error}/{len(resultados)}")
    
    # Mostrar resultados exitosos
    print("\n" + "="*80)
    print("✨ STATUS DE PREGUNTAS")
    print("="*80 + "\n")
    
    preguntas_corto = [
        "1. Cumplimiento 100% - costo/caja y costo/ha",
        "2. Ranking de haciendas por categoría",
        "3. Variables que influyen en costos",
        "4. Tendencia en indicadores de producción",
        "5. Ajustes para reducir costos",
        "6. Prácticas de haciendas con menor costo",
        "7. Variables administrativas en costos",
        "8. Cambios en programas de atención",
        "9. Influencia en reducción de costos (3 meses)",
        "10. Programa a ajustar para productividad"
    ]
    
    for i, res in enumerate(resultados, 1):
        has_error = "Error" in res['resultado'] or "error" in res['resultado']
        status = "❌" if has_error else "✅"
        print(f"{status} {preguntas_corto[i-1]}")
    
    print("\n" + "="*80)
    print("✅ ANÁLISIS COMPLETADO")
    print("="*80)
    
    return resultados


if __name__ == "__main__":
    main()
