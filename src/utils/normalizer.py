import logging

logger = logging.getLogger(__name__)

# Diccionario extendido de unidades comunes en Mercado Público
UNIDADES_MAP = {
    "EA": "Unidades",
    "UN": "Unidades",
    "UND": "Unidades",
    "BX": "Cajas",
    "CJ": "Cajas",
    "PAQ": "Paquetes",
    "BG": "Bolsas",
    "VI": "Frascos",
    "AMP": "Ampollas",
    "CMP": "Comprimidos",
    "SET": "Set",
    "KIT": "Kit",
    "PACK": "Pack",
    "GR": "Gramos",
    "KG": "Kilogramos",
    "KGM": "Kilogramos",
    "ML": "Mililitros",
    "LT": "Litros",
    "LTR": "Litros",
    "L": "Litros",
    "M": "Metros",
    "MTR": "Metros",
    "M2": "Metros Cuadrados",
    "M3": "Metros Cúbicos",
    "000": "Millar / Adhesivo",
    "HR": "Horas",
    "HUR": "Horas",
    "MES": "Meses",
    "DIA": "Días",
    "CJA": "Cajas",
    "TNE": "Toneladas",
    "GLI": "Galones",
}

def normalizar_unidad(unidad_cruda: str) -> str:
    """
    Recibe una unidad en texto crudo (ej. 'EA', 'KGM', 'unidades')
    y retorna su versión humanizada si existe en el diccionario.
    Si no existe, retorna el texto original limpio.
    """
    if not unidad_cruda:
        return unidad_cruda
        
    unidad_limpia = unidad_cruda.strip().upper()
    
    # Búsqueda exacta
    if unidad_limpia in UNIDADES_MAP:
        return UNIDADES_MAP[unidad_limpia]
        
    # Puede que el LLM ya traiga "Unidades" pero en mayúsculas u otra cosa,
    # en esos casos lo dejamos pasar o capitalizamos si queremos
    return unidad_cruda.strip()
