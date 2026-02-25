import json
import logging
import re

logger = logging.getLogger(__name__)


class FinanzasLicitacionSchemaError(Exception):
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


def validate_finanzas_licitacion_schema(raw_output: str) -> dict:
    logger.info("[FINANZAS][SCHEMA] Validando salida LLM")

    if not raw_output:
        logger.error("[FINANZAS][SCHEMA] Salida vacia del LLM")
        raise FinanzasLicitacionSchemaError("Salida vacia del LLM")

    cleaned_output = _strip_markdown_code_block(raw_output)

    try:
        data = json.loads(cleaned_output)
    except json.JSONDecodeError as e:
        logger.error(
            "[FINANZAS][SCHEMA] JSON inválido | error=%s | raw=%s",
            str(e),
            raw_output
        )
        raise FinanzasLicitacionSchemaError(
            f"Salida no es JSON válido: {str(e)}"
        )

    if not isinstance(data, dict):
        logger.error(
            "[FINANZAS][SCHEMA] Estructura inválida | tipo=%s",
            type(data)
        )
        raise FinanzasLicitacionSchemaError(
            "La salida debe ser un objeto JSON"
        )

    expected_keys = [
        "presupuesto_referencial",
        "moneda",
        "forma_pago",
        "plazo_pago",
        "fuente_financiamiento",
        "garantias",
        "multas"
    ]

    for key in expected_keys:
        data.setdefault(key, None)

    # Validar que los campos sean objetos con 'valor'
    for key in expected_keys:
        if data[key] is not None:
            if not isinstance(data[key], dict):
                raise FinanzasLicitacionSchemaError(
                    f"Campo '{key}' debe ser un objeto con 'valor', 'razonamiento', 'fuentes'"
                )
            if "valor" not in data[key]:
                raise FinanzasLicitacionSchemaError(
                    f"Campo '{key}' carece del campo obligatorio 'valor'"
                )

    logger.info(
        "[FINANZAS][SCHEMA] Validación OK | campos=%s",
        list(data.keys())
    )

    return data
