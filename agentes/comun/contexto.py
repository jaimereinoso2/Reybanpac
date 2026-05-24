import re
from pathlib import Path

import pandas as pd

_INFORMACION = Path(__file__).parent.parent.parent / "informacion"
_ONTOLOGIA_PATH = _INFORMACION / "ONTOLOGIA.md"

_MESES_ES = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
}


def cargar_ontologia() -> str:
    return _ONTOLOGIA_PATH.read_text(encoding="utf-8")


def cargar_schema() -> str:
    return (_INFORMACION / "SCHEMA.md").read_text(encoding="utf-8")


def cargar_schema_depurado() -> str:
    return (_INFORMACION / "SCHEMA_depurado.md").read_text(encoding="utf-8")


def actualizar_cobertura_temporal(df: pd.DataFrame, columna_fecha: str = "FECHA") -> None:
    """Calcula el rango de fechas del DataFrame y actualiza ONTOLOGIA.md."""
    fechas = pd.to_datetime(df[columna_fecha])
    inicio = fechas.min()
    fin = fechas.max()

    linea = (
        f"Los datos disponibles cubren desde "
        f"**{_MESES_ES[inicio.month]} {inicio.year}** "
        f"hasta **{_MESES_ES[fin.month]} {fin.year}**."
    )

    contenido = _ONTOLOGIA_PATH.read_text(encoding="utf-8")
    contenido = re.sub(
        r"<!-- COBERTURA_INICIO -->.*?<!-- COBERTURA_FIN -->",
        f"<!-- COBERTURA_INICIO -->\n{linea}\n<!-- COBERTURA_FIN -->",
        contenido,
        flags=re.DOTALL,
    )
    _ONTOLOGIA_PATH.write_text(contenido, encoding="utf-8")
