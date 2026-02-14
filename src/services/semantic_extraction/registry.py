from typing import Dict, Type

# Interfaz base
from src.services.semantic_extraction.extractors.base_extractor import BaseSemanticExtractor


# Registro global de extractores
_extractors: Dict[str, Type[BaseSemanticExtractor]] = {}


def register_extractor(concepto: str, extractor_cls: Type[BaseSemanticExtractor]):
    concepto = concepto.upper()

    if concepto in _extractors:
        # 🔹 Mismo extractor → no es error
        if _extractors[concepto] is extractor_cls:
            return

        # 🔹 Otro extractor con mismo concepto → sí es error
        raise ValueError(
            f"Extractor distinto ya registrado para el concepto: {concepto}"
        )

    _extractors[concepto] = extractor_cls

def get_extractor(concepto: str) -> Type[BaseSemanticExtractor]:
    concepto = concepto.upper()
    if concepto not in _extractors:
        raise ValueError(f"No hay extractor registrado para: {concepto}")
    return _extractors[concepto]


# =========================
# REGISTRO DE EXTRACTORES
# =========================

from src.services.semantic_extraction.extractors.items_licitacion.items_licitacion_extractor import (
    ItemsLicitacionExtractor
)

from src.services.semantic_extraction.extractors.finanzas_licitacion.finanzas_licitacion_extractor import (
    FinanzasLicitacionExtractor
)

from src.services.semantic_extraction.extractors.datos_basicos_licitacion.datos_basicos_extractor import (
    DatosBasicosLicitacionExtractor
)

register_extractor("ITEMS_LICITACION", ItemsLicitacionExtractor)
register_extractor("FINANZAS_LICITACION", FinanzasLicitacionExtractor)
register_extractor("DATOS_BASICOS_LICITACION", DatosBasicosLicitacionExtractor)
