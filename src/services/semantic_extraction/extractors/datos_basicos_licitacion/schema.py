import json
import logging
import re

logger = logging.getLogger(__name__)


class DatosBasicosLicitacionSchemaError(Exception):
    """Error de validación del schema DATOS_BASICOS_LICITACION"""
    pass


def _strip_markdown_code_block(text: str) -> str:
    """
    Elimina bloques de codigo markdown (```json ... ``` o ``` ... ```)
    que el LLM a veces incluye en su respuesta.
    """
    text = text.strip()
    pattern = r"^```(?:json)?\s*\n?(.*?)\n?```$"
    match = re.match(pattern, text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return text


def validate_datos_basicos_licitacion_schema(raw_output: str) -> dict:
    """
    Valida que el JSON cumpla con el contrato DATOS_BASICOS_LICITACION.
    Lanza DatosBasicosLicitacionSchemaError si hay errores.
    """
    logger.info("[DATOS_BASICOS][SCHEMA] Validando salida LLM")

    if not raw_output:
        logger.error("[DATOS_BASICOS][SCHEMA] Salida vacia del LLM")
        raise DatosBasicosLicitacionSchemaError("Salida vacia del LLM")

    cleaned_output = _strip_markdown_code_block(raw_output)

    try:
        data = json.loads(cleaned_output)
    except json.JSONDecodeError as e:
        logger.error(
            "[DATOS_BASICOS][SCHEMA] JSON inválido | error=%s | raw=%s",
            str(e),
            raw_output
        )
        raise DatosBasicosLicitacionSchemaError(
            f"Salida no es JSON válido: {str(e)}"
        )

    if not isinstance(data, dict):
        logger.error(
            "[DATOS_BASICOS][SCHEMA] Estructura inválida | tipo=%s",
            type(data)
        )
        raise DatosBasicosLicitacionSchemaError(
            "La salida debe ser un objeto JSON"
        )

    # Campos esperados según la tabla licitaciones
    expected_keys = [
        "codigo_licitacion",
        "nombre",
        "descripcion",
        "estado",
        "organismo_solicitante"
    ]

    for key in expected_keys:
        data.setdefault(key, None)

    # Validar tipos de datos
    for key in expected_keys:
        if data[key] is not None and not isinstance(data[key], str):
            raise DatosBasicosLicitacionSchemaError(
                f"Campo '{key}' debe ser string o null"
            )

    logger.info(
        "[DATOS_BASICOS][SCHEMA] Validación OK | campos=%s",
        list(data.keys())
    )

    return data
