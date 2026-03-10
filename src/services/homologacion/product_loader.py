import os
import pandas as pd
import unicodedata
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
            
        archivos = [f for f in os.listdir(carpeta) if (f.endswith(".xls") or f.endswith(".xlsx")) and not f.startswith("~$")]
        if not archivos:
            raise FileNotFoundError(f"No se encontró ningún archivo Excel válido en {carpeta}")
        
        # Priorizar productos.xlsx si existe
        if "productos.xlsx" in archivos:
            ruta_archivo = os.path.join(carpeta, "productos.xlsx")
        else:
            ruta_archivo = os.path.join(carpeta, archivos[0])

    try:
        raw_df = pd.read_excel(ruta_archivo, header=None)
    except ValueError as e:
        if "engine manually" in str(e):
            # Fallback for weird .xls files that might actually be html or xml
            try:
                raw_df = pd.read_excel(ruta_archivo, header=None, engine="openpyxl")
            except Exception:
                try:
                    raw_df = pd.read_html(ruta_archivo, header=None)[0]
                except Exception:
                    raw_df = pd.read_html(ruta_archivo)[0]
        else:
            raise e

    # Buscar la fila que contiene los encabezados ("cod_prod", "producto")
    header_idx = 0
    for idx, row in raw_df.iterrows():
        row_str = " ".join(row.astype(str).str.lower())
        if "cod_prod" in row_str and "producto" in row_str:
            header_idx = idx
            break

    # Asignar los nombres de las columnas y limpiar
    df = raw_df.copy()
    df.columns = df.iloc[header_idx].astype(str).str.strip().str.lower()
    df = df.iloc[header_idx+1:].reset_index(drop=True)

    # Normalizar columnas quitando tildes para evitar problemas
    def clean_col(c):
        c = str(c).strip().lower()
        return unicodedata.normalize('NFD', c).encode('ascii', 'ignore').decode('utf-8')
        
    df.columns = [clean_col(c) for c in df.columns]

    columnas_requeridas = [
        "cod_prod", "producto", "descripcion",
        "cantidad", "ubicacion", "cod_tienda"
    ]
    for col in columnas_requeridas:
        if col not in df.columns:
            raise ValueError(f"Falta columna requerida en Excel: {col}. Columnas encontradas: {list(df.columns)}")

    # Eliminar filas donde 'cod_prod' esté vacío (NaN) o donde no haya producto válido
    df = df.dropna(subset=["cod_prod"])

    productos: List[dict] = []
    for _, row in df.iterrows():
        # Validar si cod_prod es de verdad un codigo (saltar valores nulos o "nan" como string)
        cod_prod_str = str(row["cod_prod"]).strip()
        if cod_prod_str.lower() == "nan" or not cod_prod_str:
            continue
            
        cantidad = row.get("cantidad", 0)
        if pd.isna(cantidad):
            cantidad = 0
            
        productos.append({
            "codigo": cod_prod_str,
            "nombre": str(row["producto"]).strip(),
            "descripcion": str(row["descripcion"]).strip(),
            "stock_disponible": int(float(cantidad)),
            "ubicacion_stock": str(row["ubicacion"]).strip(),
            "codigo_tienda": str(row["cod_tienda"]).strip()
        })

    return productos
