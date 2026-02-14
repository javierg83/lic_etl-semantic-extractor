import logging

logger = logging.getLogger(__name__)


def normalize_finanzas_licitacion(data: dict) -> dict:
    logger.info("[FINANZAS][NORMALIZER] Normalizando datos financieros")

    def _clean(value):
        if isinstance(value, str):
            return value.strip()
        return value

    normalized = {
        "presupuesto_referencial": _clean(data.get("presupuesto_referencial")),
        "moneda": _clean(data.get("moneda")),
        "forma_pago": _clean(data.get("forma_pago")),
        "plazo_pago": _clean(data.get("plazo_pago")),
        "fuente_financiamiento": _clean(data.get("fuente_financiamiento")),
        "garantias": data.get("garantias"),
        "multas": data.get("multas"),
    }

    logger.debug(
        "[FINANZAS][NORMALIZER] Resultado final normalizado:\n%s",
        normalized
    )

    return normalized
