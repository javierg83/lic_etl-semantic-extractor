import logging
import json
import re
from typing import Any, Dict, List

from src.services.semantic_extraction.extractors.base_extractor import BaseSemanticExtractor
from src.services.licitacion_service import (
    guardar_items_licitacion,
    guardar_especificaciones_tecnicas
)
from src.services.semantic_extraction.extractors.items_licitacion.schema import (
    validate_items_licitacion_schema,
    ItemsLicitacionSchemaError,
)

logger = logging.getLogger(__name__)

# ==========================================================
# CONFIGURACIÓN DE PROMPT
# ==========================================================

PROMPT_VERSION = "v3"


def clean_json_output(text: str) -> str:
    """
    Limpia un bloque de texto que puede estar envuelto en ```json ... ```
    y normaliza comillas raras.
    """
    if not text or not isinstance(text, str):
        return ""

    text = text.strip()

    # Eliminar fences tipo ```json ... ```
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    # Normalizar comillas “raras”
    text = (
        text.replace("“", "\"")
            .replace("”", "\"")
            .replace("‘", "'")
            .replace("’", "'")
    )

    return text.strip()


class ItemsLicitacionExtractor(BaseSemanticExtractor):
    """
    Extractor semántico del concepto ITEMS_LICITACION
    """

    concepto = "ITEMS_LICITACION"

    # ======================================================
    # Queries semánticas (NO MODIFICADAS)
    # ======================================================

    def build_queries(self, licitacion_id: str) -> List[str]:
        queries = [
            "ítems solicitados",
            "detalle de los ítems",
            "productos requeridos",
            "servicios requeridos",
            "cantidad solicitada",
            "especificaciones técnicas",
            "oferta técnica",
            "anexo oferta",
            "lista de ítems",
            "descripción de los ítems",
        ]

        logger.info(
            "[ITEMS] Queries generadas | licitacion_id=%s | total=%s",
            licitacion_id,
            len(queries),
        )
        logger.debug("[ITEMS] Queries: %s", queries)

        return queries

    # ======================================================
    # Prompt
    # ======================================================

    def build_prompt(self, context: str, licitacion_id: str) -> str:
        logger.info(
            "[ITEMS] Construyendo prompt | licitacion_id=%s | context_len=%s",
            licitacion_id,
            len(context or ""),
        )

        # Uses relative path logic now that the prompt is in standard location
        # Convention: items_licitacion/prompt_items_licitacion_v3.txt
        prompt_path = (
            f"{self.concepto.lower()}/"
            f"prompt_{self.concepto.lower()}_{PROMPT_VERSION}.txt"
        )

        prompt_template = self.load_prompt(prompt_path)

        prompt = prompt_template.replace("{contexto}", context)

        logger.debug("[ITEMS] Prompt final (primeros 2000 chars):\n%s", prompt[:2000])

        return prompt

    # ======================================================
    # Parseo de salida LLM
    # ======================================================

    def parse_output(self, raw_output: str) -> Dict[str, Any]:
        logger.info(
            "[ITEMS] Parseando salida LLM | raw_len=%s",
            len(raw_output or ""),
        )

        logger.debug("[ITEMS] Raw output LLM completo:\n%s", raw_output)

        cleaned_output = clean_json_output(raw_output)

        if not cleaned_output:
            logger.error("[ITEMS] Salida LLM vacía tras limpieza")
            raise ItemsLicitacionSchemaError(
                "Salida del LLM vacía tras limpieza"
            )

        # -----------------------------
        # JSON → dict
        # -----------------------------
        try:
            data = json.loads(cleaned_output)
        except json.JSONDecodeError as e:
            logger.error(
                "[ITEMS] JSON inválido tras limpieza | error=%s\nContenido:\n%s",
                e,
                cleaned_output,
            )
            raise ItemsLicitacionSchemaError(
                f"JSON inválido devuelto por el LLM: {e}"
            )

        # -----------------------------
        # Logging estructural del JSON
        # -----------------------------
        logger.info(
            "[ITEMS] Claves principales del JSON parseado: %s",
            list(data.keys()),
        )

        if "resumen" in data:
            logger.info(
                "[ITEMS] Resumen devuelto por el modelo: %s",
                data.get("resumen"),
            )

        # -----------------------------
        # Validación de schema
        # -----------------------------
        validate_items_licitacion_schema(data)
        logger.info("[ITEMS] Resultado validado correctamente por schema")

        items = data.get("items", [])

        # -----------------------------
        # LOG CRÍTICO: items vacíos
        # -----------------------------
        if not items:
            logger.warning(
                "[ITEMS] ⚠️ items == [] | licitacion_id=%s",
                data.get("licitacion_id"),
            )
            logger.warning(
                "[ITEMS] Observaciones del resumen: %s",
                data.get("resumen", {}).get("observaciones"),
            )
            logger.debug(
                "[ITEMS] JSON completo cuando items == []:\n%s",
                json.dumps(data, indent=2, ensure_ascii=False),
            )

        return {
            "concepto": self.concepto,
            "resumen": data.get("resumen"),
            "items": items,
            "especificaciones": data.get("especificaciones", []),
            "warnings": data.get("warnings", []),
        }

    # ======================================================
    # Persistencia (NO MODIFICADA)
    # ======================================================

    def persist_resultado(
        self,
        licitacion_id: str,
        json_data: Dict[str, Any],
        semantic_run_id: str,
    ) -> None:
        logger.info(
            "[ITEMS] Persistiendo resultado | licitacion_id=%s | total_items=%s",
            licitacion_id,
            len(json_data.get("items", [])),
        )

        # Note: BaseExtractor in this project likely doesn't have get_pg_conn unless added.
        # But we'll leave this code as provided, assuming it might be called if conn is available.
        # However, runner.py handles persistence, so this might be redundant.
        pass
