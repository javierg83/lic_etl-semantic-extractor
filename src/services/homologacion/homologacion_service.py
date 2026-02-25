"""
Servicio de homologacion automatica de productos.

Este servicio encapsula la ejecucion automatica del proceso de homologacion,
desacoplado de la extraccion semantica. Debe ejecutarse DESPUES de la
extraccion semantica de items.

La conexion a BD se recibe como parametro, siguiendo el mismo patron
que el flujo de FINANZAS.
"""
import logging
from uuid import UUID
from typing import Optional

from src.services.homologacion.homologador import homologar_productos_para_licitacion
from src.services.homologacion.product_loader import cargar_productos_catalogo
from src.services.licitacion_service import obtener_items_por_licitacion

logger = logging.getLogger(__name__)


def ejecutar_homologacion_automatica(
    licitacion_id: UUID,
    conn,
    modelo: str = "gpt-4o"
) -> Optional[dict]:
    """
    Ejecuta el proceso de homologacion de productos de forma automatica.

    Este metodo debe ser invocado DESPUES de la extraccion semantica de items.
    La conexion a BD debe ser proporcionada desde el flujo principal.

    Args:
        licitacion_id: UUID de la licitacion a procesar
        conn: Conexion activa a PostgreSQL (no se crea internamente)
        modelo: Modelo LLM a utilizar (default: gpt-4o)

    Returns:
        dict con el resultado de la homologacion, o None si falla

    Raises:
        ValueError: Si no se encuentran items para la licitacion
    """
    logger.info(
        "[HOMOLOGACION_SERVICE] Iniciando homologacion automatica | licitacion_id=%s | modelo=%s",
        licitacion_id,
        modelo
    )

    licitacion_id_str = str(licitacion_id)

    # Use the existing function to fetch items based on licitacion_id
    items_licitacion = obtener_items_por_licitacion(licitacion_id_str)
    
    if not items_licitacion:
        logger.warning(
            "[HOMOLOGACION_SERVICE] No se encontraron items para la licitacion | licitacion_id=%s",
            licitacion_id
        )
        return None

    logger.info(
        "[HOMOLOGACION_SERVICE] Items encontrados | licitacion_id=%s | total_items=%d",
        licitacion_id,
        len(items_licitacion)
    )

    productos_catalogo = cargar_productos_catalogo()
    if not productos_catalogo:
        logger.warning(
            "[HOMOLOGACION_SERVICE] No se encontraron productos en el catalogo"
        )
        raise ValueError("No se encontraron productos en el catalogo")

    logger.info(
        "[HOMOLOGACION_SERVICE] Productos del catalogo cargados | total_productos=%d",
        len(productos_catalogo)
    )

    resultado = homologar_productos_para_licitacion(
        licitacion_id=licitacion_id_str,
        items_licitacion=items_licitacion,
        productos_catalogo=productos_catalogo,
        modelo=modelo,
        conn=conn
    )

    logger.info(
        "[HOMOLOGACION_SERVICE] Homologacion completada | licitacion_id=%s | items_con_match=%s/%s",
        licitacion_id,
        resultado.get("resumen", {}).get("total_items_con_match", 0),
        resultado.get("resumen", {}).get("total_items_detectados", 0)
    )

    return resultado
