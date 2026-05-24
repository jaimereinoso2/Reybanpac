import os
from google import genai


def _cfg(key: str, default: str = None) -> str:
    try:
        import streamlit as st
        return st.secrets[key]
    except Exception:
        return os.getenv(key, default)


def _llm(prompt: str, model: str = None) -> str:
    model = model or _cfg("GEMINI_MODEL_CODE", "gemini-2.5-flash")
    client = genai.Client(api_key=_cfg("GEMINI_API_KEY"))
    response = client.models.generate_content(model=model, contents=prompt)
    return response.text.strip()
