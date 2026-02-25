"""
Modulo principal de homologacion de productos.

Este modulo contiene la logica para homologar items de licitacion
contra un catalogo de productos usando LLM.

IMPORTANTE: Este modulo NO crea conexiones a BD internamente.
La conexion debe ser proporcionada como parametro.
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from uuid import uuid4
from typing import List

from src.services.llm_service import run_llm_raw_with_tokens
from src.services.homologacion.homologacion_db import (
    insertar_homologacion_producto,
    insertar_candidato_homologacion,
)

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).parent / "prompts" / "prompt_homologacion_v3.txt"


def build_prompt_homologacion(items_detectados: List[dict], productos_catalogo: List[dict]) -> str:
    """
    Construye el prompt para el LLM de homologacion.

    Args:
        items_detectados: Lista de items extraidos de la licitacion
        productos_catalogo: Lista de productos del catalogo

    Returns:
        Prompt formateado para el LLM
    """
    prompt_base = PROMPT_PATH.read_text(encoding="utf-8")

    prompt = prompt_base.replace(
        "{{ productos | tojson(indent=2) }}",
        json.dumps(productos_catalogo, ensure_ascii=False, indent=2)
    )
    prompt = prompt.replace(
        "{{ items_detectados | tojson(indent=2) }}",
        json.dumps(items_detectados, ensure_ascii=False, indent=2)
    )

    return prompt


def homologar_productos_para_licitacion(
    licitacion_id: str,
    items_licitacion: List[dict],
    productos_catalogo: List[dict],
    modelo: str,
    conn
) -> dict:
    """
    Ejecuta el proceso de homologacion de productos para una licitacion.

    IMPORTANTE: La conexion a BD (conn) debe ser proporcionada desde el flujo
    principal. Este metodo NO crea conexiones internamente.

    Args:
        licitacion_id: ID de la licitacion
        items_licitacion: Lista de items de la licitacion (desde BD)
        productos_catalogo: Lista de productos del catalogo
        modelo: Modelo LLM a utilizar (ej: "gpt-4o")
        conn: Conexion activa a PostgreSQL

    Returns:
        dict con estructura:
        {
            "concepto": "HOMOLOGACION_PRODUCTOS",
            "licitacion_id": str,
            "resumen": {...},
            "homologaciones": [...]
        }
    """
    logger.info(
        "[HOMOLOGADOR] Iniciando homologacion | licitacion_id=%s | items=%d | productos=%d | modelo=%s",
        licitacion_id,
        len(items_licitacion),
        len(productos_catalogo),
        modelo
    )

    # IMPORTANTE: Priorizar item_key sobre nombre_item para mantener consistencia
    # con la tabla items_licitacion y el JOIN en obtener_items_homologados_con_candidatos
    items_detectados = [
        {
            "item_key": item.get("item_key") or item.get("nombre_item"),
            "descripcion_detectada": item.get("descripcion") or item.get("descripcion_detectada") or ""
        }
        for item in items_licitacion
    ]

    logger.info("[HOMOLOGADOR] Items detectados para homologacion:")
    for idx, item in enumerate(items_detectados):
        logger.info(f"  [{idx+1}] item_key={item['item_key']}")

    prompt = build_prompt_homologacion(items_detectados, productos_catalogo)

    logger.info("[HOMOLOGADOR] Enviando prompt al LLM | largo_prompt=%d", len(prompt))

    resultado_llm = run_llm_raw_with_tokens(prompt, overrides={"model": modelo})

    respuesta_texto = resultado_llm.get("respuesta", "")
    tokens_input = resultado_llm.get("tokens_input", 0)
    tokens_output = resultado_llm.get("tokens_output", 0)

    logger.info(
        "[HOMOLOGADOR] Respuesta LLM recibida | tokens_input=%d | tokens_output=%d",
        tokens_input,
        tokens_output
    )

    try:
        if respuesta_texto.startswith("```json"):
            respuesta_texto = respuesta_texto[7:]
        if respuesta_texto.startswith("```"):
            respuesta_texto = respuesta_texto[3:]
        if respuesta_texto.endswith("```"):
            respuesta_texto = respuesta_texto[:-3]
        respuesta_texto = respuesta_texto.strip()

        homologaciones = json.loads(respuesta_texto)
    except json.JSONDecodeError as e:
        logger.error("[HOMOLOGADOR] Error parseando respuesta LLM: %s", str(e))
        logger.error("[HOMOLOGADOR] Respuesta raw: %s", respuesta_texto[:500])
        homologaciones = []

    total_items_detectados = len(items_detectados)
    total_items_con_match = sum(1 for h in homologaciones if h.get("candidatos"))

    now = datetime.utcnow()

    for homologacion in homologaciones:
        item_key = homologacion.get("item_key")
        descripcion_detectada = homologacion.get("descripcion_detectada", "")
        razonamiento_general = homologacion.get("razonamiento_general")
        candidatos = homologacion.get("candidatos", [])

        homologacion_id = str(uuid4())

        logger.info("[HOMOLOGADOR] Insertando homologacion | item_key=%s", item_key)

        insertar_homologacion_producto(
            conn=conn,
            homologacion_id=homologacion_id,
            licitacion_id=licitacion_id,
            item_key=item_key,
            descripcion_detectada=descripcion_detectada,
            razonamiento_general=razonamiento_general,
            tokens_input=tokens_input,
            tokens_output=tokens_output,
            tokens_total=tokens_input + tokens_output,
            modelo_usado=modelo,
            fecha_homologacion=now,
        )

        for candidato in candidatos:
            producto = candidato.get("producto", {})

            insertar_candidato_homologacion(
                conn=conn,
                homologacion_id=homologacion_id,
                ranking=candidato.get("ranking"),
                producto_codigo=producto.get("codigo"),
                producto_nombre=producto.get("nombre"),
                producto_descripcion=producto.get("descripcion"),
                stock_disponible=producto.get("stock_disponible"),
                ubicacion_stock=producto.get("ubicacion_stock"),
                score_similitud=candidato.get("score_similitud"),
                razonamiento=candidato.get("razonamiento"),
            )

            logger.debug(
                "[HOMOLOGADOR] Candidato insertado | item_key=%s | producto=%s",
                item_key,
                producto.get("codigo")
            )

    conn.commit()

    logger.info(
        "[HOMOLOGADOR] Homologacion completada | licitacion_id=%s | items_con_match=%d/%d",
        licitacion_id,
        total_items_con_match,
        total_items_detectados
    )

    return {
        "concepto": "HOMOLOGACION_PRODUCTOS",
        "licitacion_id": licitacion_id,
        "resumen": {
            "total_items_detectados": total_items_detectados,
            "total_items_con_match": total_items_con_match,
            "tokens_input": tokens_input,
            "tokens_output": tokens_output,
            "tokens_total": tokens_input + tokens_output,
            "modelo_usado": modelo
        },
        "homologaciones": homologaciones
    }
