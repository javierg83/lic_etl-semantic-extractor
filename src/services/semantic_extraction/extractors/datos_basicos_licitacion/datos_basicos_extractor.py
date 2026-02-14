import logging
from typing import Any, Dict, List

from src.services.semantic_extraction.extractors.base_extractor import BaseSemanticExtractor
from src.services.semantic_extraction.extractors.datos_basicos_licitacion.schema import (
    validate_datos_basicos_licitacion_schema
)
from src.services.semantic_extraction.extractors.datos_basicos_licitacion.normalizer import (
    normalize_datos_basicos_licitacion
)

logger = logging.getLogger(__name__)

# ==========================================================
# CONFIGURACIÓN DE PROMPT
# ==========================================================
PROMPT_VERSION = "v1"


class DatosBasicosLicitacionExtractor(BaseSemanticExtractor):
    """
    Extractor semántico del concepto DATOS_BASICOS_LICITACION
    """

    concepto = "DATOS_BASICOS_LICITACION"

    def build_queries(self, licitacion_id: str) -> List[str]:
        """
        Queries semánticas para obtener los datos básicos de la licitación.
        """
        queries = [
            "código de licitación",
            "número de licitación",
            "identificación del proceso",
            "nombre de la licitación",
            "título del proceso",
            "objeto de la licitación",
            "descripción de la licitación",
            "estado de la licitación",
            "entidad licitante",
            "organismo solicitante",
            "institución que solicita la licitación",
            "empresa u organismo público que convoca"
        ]

        logger.info(
            "[DATOS_BASICOS] Queries semánticas generadas | licitacion_id=%s | queries=%s",
            licitacion_id,
            queries
        )

        return queries

    def build_prompt(self, context: str, licitacion_id: str) -> str:
        """
        Construye el prompt para el LLM.
        """
        logger.info(
            "[DATOS_BASICOS] Construyendo prompt | licitacion_id=%s | context_len=%s",
            licitacion_id,
            len(context or "")
        )

        prompt_path = (
            f"datos_basicos_licitacion/"
            f"prompt_datos_basicos_licitacion_{PROMPT_VERSION}.txt"
        )

        prompt_template = self.load_prompt(prompt_path)

        prompt = prompt_template.replace("{contexto}", context)

        logger.debug("[DATOS_BASICOS] Prompt generado:\n%s", prompt)

        return prompt

    def parse_output(self, raw_output: str) -> Dict[str, Any]:
        """
        Valida, normaliza y empaqueta la salida del LLM.
        """
        logger.info(
            "[DATOS_BASICOS] Parseando salida LLM | raw_len=%s",
            len(raw_output or "")
        )

        logger.debug("[DATOS_BASICOS] Raw output LLM:\n%s", raw_output)

        # Validar esquema
        data = validate_datos_basicos_licitacion_schema(raw_output)

        logger.info(
            "[DATOS_BASICOS] Schema validado correctamente | keys=%s",
            list(data.keys())
        )

        # Normalizar datos
        normalized = normalize_datos_basicos_licitacion(data)

        logger.debug(
            "[DATOS_BASICOS] Resultado normalizado:\n%s",
            normalized
        )

        # Wrapper estándar esperado por el runner
        return {
            "concepto": self.concepto,
            "datos_basicos": normalized
        }
