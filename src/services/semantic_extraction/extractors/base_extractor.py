import inspect
import logging
import os
from abc import ABC, abstractmethod
from typing import List, Optional
from datetime import datetime

# TODO: Implement this import
from src.services.llm_service import run_llm_raw

logger = logging.getLogger(__name__)


class BaseSemanticExtractor(ABC):
    """
    Clase base para extractores semánticos.

    RESPONSABILIDAD:
    - Recibir CONTEXTO ya construido por el runner
    - Construir prompt
    - Ejecutar LLM
    - Parsear salida

    NO DEBE:
    - Importar registry
    - Ejecutar embeddings
    - Resolver extractores
    """

    concepto: Optional[str] = None

    def __init__(self, licitacion_id: str):
        self.licitacion_id = licitacion_id

        # Atributos opcionales que el runner puede asignar después de instanciar
        self.prompt_version: Optional[str] = None
        self.extractor_version: Optional[str] = None

        # Control de ejecución (evita loops)
        self._has_run = False
        self._started_at: Optional[datetime] = None
        self._finished_at: Optional[datetime] = None

        logger.debug(
            "[BASE_EXTRACTOR] Instanciado | concepto=%s | licitacion_id=%s",
            self.concepto,
            self.licitacion_id
        )

    # ======================================================
    # Métodos auxiliares para carga de prompts
    # ======================================================

    def load_prompt(self, relative_path: str) -> str:
        """
        Carga un archivo de prompt desde services/semantic_extraction/prompts/
        """
        # Adjusted path logic for new structure under src/services/semantic_extraction/extractors/
        # Assumes this file is in src/services/semantic_extraction/extractors/base_extractor.py
        # Prompts should be in src/services/semantic_extraction/prompts/
        
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        prompts_dir = os.path.join(base_dir, "prompts")
        prompt_path = os.path.join(prompts_dir, relative_path)

        logger.debug("[BASE_EXTRACTOR] Cargando prompt desde: %s", prompt_path)

        if not os.path.exists(prompt_path):
             # Try assuming path provided is absolute or relative to project root if not found
             # Or just fallback to logging error if file not found
             # But for now let's trust the relative structure
             pass

        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read()

    def _load_prompt_template(self) -> str:
        """
        Carga el prompt por defecto basado en el concepto del extractor.
        Convención: prompts/{concepto_lower}/prompt_{concepto_lower}_v1.txt
        """
        concepto_lower = (self.concepto or "").lower()
        relative_path = f"{concepto_lower}/prompt_{concepto_lower}_v1.txt"
        return self.load_prompt(relative_path)

    # ======================================================
    # Métodos a implementar por extractores concretos
    # ======================================================

    @abstractmethod
    def build_queries(self, licitacion_id: str) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def build_prompt(self, context: str, licitacion_id: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def parse_output(self, raw_output: str):
        raise NotImplementedError

    def normalize(self, parsed_output):
        """
        Método de normalización por defecto. Los extractores pueden sobrescribirlo.
        Por defecto retorna el output tal cual.
        """
        return parsed_output

    # ======================================================
    # Ejecución principal (invocada por runner)
    # ======================================================

    def _call_build_prompt(self, context: str) -> str:
        """
        Llama a build_prompt de forma flexible según la firma del método concreto.
        Soporta tanto build_prompt(context) como build_prompt(context, licitacion_id).
        """
        sig = inspect.signature(self.build_prompt)
        params = list(sig.parameters.keys())

        # Si el método acepta 2 parámetros (context, licitacion_id)
        if len(params) >= 2:
            return self.build_prompt(context, self.licitacion_id)
        # Si solo acepta 1 parámetro (context)
        return self.build_prompt(context)

    def _call_build_queries(self) -> List[str]:
        """
        Llama a build_queries de forma flexible según la firma del método concreto.
        Soporta tanto build_queries() como build_queries(licitacion_id).
        """
        sig = inspect.signature(self.build_queries)
        params = list(sig.parameters.keys())

        # Si el método acepta 1 parámetro (licitacion_id)
        if len(params) >= 1:
            return self.build_queries(self.licitacion_id)
        # Si no acepta parámetros
        return self.build_queries()

    def run(self, context: str):
        if self._has_run:
            logger.warning(
                "[SEMANTIC][%s] Ejecución duplicada bloqueada | licitacion_id=%s",
                self.concepto,
                self.licitacion_id
            )
            return None

        self._has_run = True
        self._started_at = datetime.utcnow()

        logger.info(
            "[SEMANTIC][%s] Inicio extraccion | licitacion_id=%s | context_len=%s",
            self.concepto,
            self.licitacion_id,
            len(context or "")
        )

        # Prompt (llamada flexible)
        prompt = self._call_build_prompt(context)

        logger.debug(
            "[SEMANTIC][%s] Prompt construido | len=%s",
            self.concepto,
            len(prompt or "")
        )

        # LLM
        logger.info("[SEMANTIC][%s] Ejecutando LLM", self.concepto)
        raw_output = run_llm_raw(prompt)

        if not raw_output:
            logger.error("[SEMANTIC][%s] Salida vacia del LLM", self.concepto)
            raise RuntimeError("Salida vacia del LLM")

        logger.debug(
            "[SEMANTIC][%s] Raw output:\n%s",
            self.concepto,
            raw_output
        )

        # Parseo
        parsed = self.parse_output(raw_output)

        # Normalización
        result = self.normalize(parsed)

        self._finished_at = datetime.utcnow()

        logger.info(
            "[SEMANTIC][%s] Extraccion finalizada | duracion_ms=%s",
            self.concepto,
            int((self._finished_at - self._started_at).total_seconds() * 1000)
        )

        return result
