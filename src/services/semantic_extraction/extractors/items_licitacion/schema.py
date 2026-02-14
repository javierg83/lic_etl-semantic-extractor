
from typing import Any, Dict
import re


class ItemsLicitacionSchemaError(Exception):
    """Error de validación del schema ITEMS_LICITACION"""
    pass


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float))


def _detect_embedded_items(texto: str) -> bool:
    """
    Detecta numeraciones tipo '1.-', '2.-', etc. dentro de una descripción.
    """
    if not texto or not isinstance(texto, str):
        return False
    return len(re.findall(r"\b\d{1,2}\.\-?", texto)) >= 2


def validate_items_licitacion_schema(data: Dict[str, Any]) -> None:
    """
    Valida que el JSON cumpla con el contrato ITEMS_LICITACION.

    Lanza ItemsLicitacionSchemaError si hay errores.
    """

    # ==================================================
    # Nivel raíz
    # ==================================================

    if not isinstance(data, dict):
        raise ItemsLicitacionSchemaError(
            f"El payload no es un objeto JSON (tipo recibido: {type(data)})"
        )

    if data.get("concepto") != "ITEMS_LICITACION":
        raise ItemsLicitacionSchemaError(
            "Campo 'concepto' inválido o ausente"
        )

    # licitacion_id:
    # - puede ser string
    # - puede ser null (si el LLM no lo conoce)
    if "licitacion_id" not in data:
        raise ItemsLicitacionSchemaError(
            "Campo 'licitacion_id' ausente"
        )

    if data["licitacion_id"] is not None and not isinstance(data["licitacion_id"], str):
        raise ItemsLicitacionSchemaError(
            "Campo 'licitacion_id' debe ser string o null"
        )

    if "codigo_licitacion" in data and data["codigo_licitacion"] is not None:
        if not isinstance(data["codigo_licitacion"], str):
            raise ItemsLicitacionSchemaError(
                "Campo 'codigo_licitacion' debe ser string o null"
            )

    if "items" not in data or not isinstance(data["items"], list):
        raise ItemsLicitacionSchemaError(
            "Campo 'items' ausente o no es lista"
        )

    # ==================================================
    # Validación semántica global
    # ==================================================

    # Caso típico: un solo ítem con numeraciones embebidas
    if len(data["items"]) == 1:
        desc = data["items"][0].get("descripcion", "")
        if _detect_embedded_items(desc):
            raise ItemsLicitacionSchemaError(
                "Se detectaron numeraciones múltiples (1.-, 2.-) en la descripción, "
                "pero solo hay un ítem. Posible error de segmentación."
            )

    # ==================================================
    # Validación por ítem
    # ==================================================

    for idx, item in enumerate(data["items"], start=1):
        if not isinstance(item, dict):
            raise ItemsLicitacionSchemaError(
                f"Item #{idx} no es un objeto JSON"
            )

        for field in ["item_key", "nombre_item"]:
            if field not in item or not isinstance(item[field], str) or not item[field].strip():
                raise ItemsLicitacionSchemaError(
                    f"Item #{idx}: campo '{field}' ausente o inválido"
                )

        if "cantidad" in item and item["cantidad"] is not None:
            if not _is_number(item["cantidad"]):
                raise ItemsLicitacionSchemaError(
                    f"Item #{idx}: 'cantidad' debe ser numérico o null"
                )

        if "unidad" in item and item["unidad"] is not None:
            if not isinstance(item["unidad"], str):
                raise ItemsLicitacionSchemaError(
                    f"Item #{idx}: 'unidad' debe ser string o null"
                )

        for field in ["descripcion", "notas"]:
            if field in item and item[field] is not None:
                if not isinstance(item[field], str):
                    raise ItemsLicitacionSchemaError(
                        f"Item #{idx}: '{field}' debe ser string o null"
                    )

        for list_field in [
            "especificaciones",
            "criterios_cumplimiento",
            "exclusiones_o_prohibiciones",
        ]:
            if list_field in item:
                if not isinstance(item[list_field], list):
                    raise ItemsLicitacionSchemaError(
                        f"Item #{idx}: '{list_field}' debe ser lista"
                    )
                for v in item[list_field]:
                    if not isinstance(v, str):
                        raise ItemsLicitacionSchemaError(
                            f"Item #{idx}: '{list_field}' contiene valor no string"
                        )

        if "fuentes" not in item or not isinstance(item["fuentes"], list) or not item["fuentes"]:
            raise ItemsLicitacionSchemaError(
                f"Item #{idx}: debe contener al menos una fuente"
            )

        for f_idx, fuente in enumerate(item["fuentes"], start=1):
            if not isinstance(fuente, dict):
                raise ItemsLicitacionSchemaError(
                    f"Item #{idx}, fuente #{f_idx}: no es objeto JSON"
                )

            for key in ["documento", "documento_id", "pagina", "redis_key"]:
                if key not in fuente:
                    raise ItemsLicitacionSchemaError(
                        f"Item #{idx}, fuente #{f_idx}: falta campo '{key}'"
                    )

            if fuente["pagina"] is not None and not isinstance(fuente["pagina"], int):
                # Intentar conversión si es string numérico
                if isinstance(fuente["pagina"], str) and fuente["pagina"].isdigit():
                    fuente["pagina"] = int(fuente["pagina"])
                # Si falla o es otro tipo, lo dejamos en None para no romper el flujo
                # (Opcional: logging.warning here)
                else:
                    try:
                         # Intento final de int() por si es string con espacios
                         fuente["pagina"] = int(str(fuente["pagina"]).strip())
                    except (ValueError, TypeError):
                        fuente["pagina"] = None

        if "confianza_item" in item:
            if not _is_number(item["confianza_item"]):
                raise ItemsLicitacionSchemaError(
                    f"Item #{idx}: 'confianza_item' debe ser numérico"
                )
            if not (0.0 <= float(item["confianza_item"]) <= 1.0):
                raise ItemsLicitacionSchemaError(
                    f"Item #{idx}: 'confianza_item' fuera de rango 0–1"
                )
