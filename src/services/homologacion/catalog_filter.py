import re
import logging
import unicodedata
from typing import List, Set

logger = logging.getLogger(__name__)

def _normalize_text(text: str) -> Set[str]:
    """
    Normaliza el texto: quita tildes, convierte a minúsculas y extrae palabras clave.
    """
    if not text:
        return set()
    
    # Quitar tildes
    text = "".join(
        c for c in unicodedata.normalize("NFD", str(text))
        if unicodedata.category(c) != "Mn"
    )
    # A minúsculas y caracteres alfanuméricos
    words = re.findall(r'[a-z0-9]+', text.lower())
    
    # Filtrar palabras cortas sin valor (conectoras)
    stop_words = {"de", "la", "el", "en", "para", "con", "y", "o", "a", "los", "las", "un", "una", "del"}
    return {w for w in words if len(w) > 2 and w not in stop_words}

def filter_catalog(batch_items: List[dict], full_catalog: List[dict], top_n: int = 120) -> List[dict]:
    """
    Filtra el catálogo completo devolviendo solo los productos con mayor 
    coincidencia de palabras clave para un lote de ítems.
    """
    logger.info("[FILTER] Iniciando filtrado de catálogo para lote de %d ítems", len(batch_items))
    
    # 1. Obtener palabras clave de todos los ítems del lote
    item_keywords = set()
    for item in batch_items:
        nombre = item.get("item_key", "")
        desc = item.get("descripcion_detectada", "")
        item_keywords.update(_normalize_text(nombre))
        item_keywords.update(_normalize_text(desc))
        
    if not item_keywords:
        logger.warning("[FILTER] No se detectaron palabras clave en los ítems. Retornando muestra del catálogo.")
        return full_catalog[:top_n]

    # 2. Puntuar productos del catálogo
    scored_products = []
    for product in full_catalog:
        prod_text = f"{product.get('nombre', '')} {product.get('descripcion', '')}"
        prod_keywords = _normalize_text(prod_text)
        
        # Intersección de palabras clave
        common_words = item_keywords.intersection(prod_keywords)
        score = len(common_words)
        
        if score > 0:
            scored_products.append((score, product))
            
    # 3. Ordenar por puntuación y devolver el Top N
    scored_products.sort(key=lambda x: x[0], reverse=True)
    
    selected_products = [p[1] for p in scored_products[:top_n]]
    
    # Si no hay suficientes coincidencias, rellenar con algunos productos base por si acaso
    if len(selected_products) < 20 and len(full_catalog) > len(selected_products):
        remaining = full_catalog[:20]
        for p in remaining:
            if p not in selected_products:
                selected_products.append(p)

    logger.info(
        "[FILTER] Filtrado completado. Catálogo reducido de %d a %d productos (Top %d)",
        len(full_catalog), len(selected_products), top_n
    )
    
    return selected_products
