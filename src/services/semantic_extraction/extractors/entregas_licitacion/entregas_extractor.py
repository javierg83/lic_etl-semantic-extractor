import logging
import json
import re
from typing import Any, Dict, List

from src.services.semantic_extraction.extractors.base_extractor import BaseSemanticExtractor
from src.services.semantic_extraction.extractors.entregas_licitacion.schema import (
    validate_entregas_licitacion_schema,
    EntregasLicitacionSchemaError,
)

logger = logging.getLogger(__name__)

PROMPT_VERSION = "v1"

def clean_json_output(text: str) -> str:
    """ Limpia un bloque de texto que puede estar envuelto en ```json ... ``` """
    if not text or not isinstance(text, str):
        return ""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()

class EntregasLicitacionExtractor(BaseSemanticExtractor):
    """
    Extractor semántico del concepto ENTREGAS_LICITACION
    """
    concepto = "ENTREGAS_LICITACION"

    def build_queries(self, licitacion_id: str) -> List[str]:
        queries = [
            "dirección de entrega",
            "comuna de despacho",
            "plazo de entrega",
            "fecha de despacho",
            "horario de recepción",
            "condiciones de entrega",
            "lugar de entrega",
            "contacto para despacho",
            "instrucciones de envío"
        ]
        logger.info(
            "[ENTREGAS] Queries generadas | licitacion_id=%s | total=%s",
            licitacion_id, len(queries)
        )
        return queries

    def build_prompt(self, context: str, licitacion_id: str) -> str:
        logger.info(
            "[ENTREGAS] Construyendo prompt | licitacion_id=%s | context_len=%s",
            licitacion_id, len(context or "")
        )
        prompt_path = f"{self.concepto.lower()}/prompt_{self.concepto.lower()}_{PROMPT_VERSION}.txt"
        prompt_template = self.load_prompt(prompt_path)
        prompt = prompt_template.replace("{contexto}", context)
        return prompt

    def parse_output(self, raw_output: str) -> Dict[str, Any]:
        logger.info("[ENTREGAS] Parseando salida LLM | raw_len=%s", len(raw_output or ""))
        
        cleaned_output = clean_json_output(raw_output)
        
        if not cleaned_output:
            logger.error("[ENTREGAS] Salida LLM vacía tras limpieza")
            raise EntregasLicitacionSchemaError("Salida vacía tras limpieza")

        try:
            data = json.loads(cleaned_output)
        except json.JSONDecodeError as e:
            logger.error("[ENTREGAS] JSON inválido tras limpieza | error=%s", e)
            return {
                "concepto": self.concepto,
                "notas": f"JSON inválido devuelto por el LLM: {e}",
                "warnings": [f"JSON Parse Error: {e}"],
            }

        validate_entregas_licitacion_schema(data)
        logger.info("[ENTREGAS] Resultado validado correctamente por schema")

        return data
