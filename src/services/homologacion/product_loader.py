import os
import pandas as pd
from typing import List
from src.services.homologacion.models.schema import ProductoCatalogo


def cargar_productos_catalogo(ruta_archivo: str = None) -> List[ProductoCatalogo]:
    """
    Carga productos desde un archivo Excel ubicado en /productos.
    Devuelve una lista de diccionarios con la estructura de ProductoCatalogo.
    """
    if ruta_archivo is None:
        # Se asume que el directorio productos esta en la raiz del proyecto `lic_etl-semantic-extractor`
        carpeta = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../productos"))
        if not os.path.exists(carpeta):
            os.makedirs(carpeta, exist_ok=True)
            raise FileNotFoundError(f"No se encontró el directorio {carpeta}")
            
        archivos = [f for f in os.listdir(carpeta) if f.endswith(".xls") or f.endswith(".xlsx")]
        if not archivos:
            raise FileNotFoundError(f"No se encontró ningún archivo Excel en {carpeta}")
        ruta_archivo = os.path.join(carpeta, archivos[0])

    df = pd.read_excel(ruta_archivo)

    columnas_requeridas = [
        "cod_prod", "producto", "descripcion",
        "cantidad", "ubicación", "cod_tienda"
    ]
    for col in columnas_requeridas:
        if col not in df.columns:
            raise ValueError(f"Falta columna requerida en Excel: {col}")

    productos: List[dict] = []
    for _, row in df.iterrows():
        productos.append({
            "codigo": str(row["cod_prod"]).strip(),
            "nombre": str(row["producto"]).strip(),
            "descripcion": str(row["descripcion"]).strip(),
            "stock_disponible": int(row["cantidad"]),
            "ubicacion_stock": str(row["ubicación"]).strip(),
            "codigo_tienda": str(row["cod_tienda"]).strip()
        })

    return productos
