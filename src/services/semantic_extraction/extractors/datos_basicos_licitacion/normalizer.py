import logging

logger = logging.getLogger(__name__)


def normalize_datos_basicos_licitacion(data: dict) -> dict:
    """
    Normaliza los datos básicos de licitación para persistencia.
    Limpia espacios en blanco y asegura formato consistente.
    """
    logger.info("[DATOS_BASICOS][NORMALIZER] Normalizando datos básicos")

    def _clean(value):
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned if cleaned else None
        return value

    normalized = {
        "codigo_licitacion": _clean(data.get("codigo_licitacion")),
        "nombre": _clean(data.get("nombre")),
        "descripcion": _clean(data.get("descripcion")),
        "estado": _clean(data.get("estado")),
        "organismo_solicitante": _clean(data.get("organismo_solicitante")),
    }

    logger.debug(
        "[DATOS_BASICOS][NORMALIZER] Resultado final normalizado:\n%s",
        normalized
    )

    return normalized
