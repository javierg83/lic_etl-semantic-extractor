import os
import json
import unicodedata
import re

def guardar_resultados(resultados, carpeta_destino, nombre_base="resultado"):
    """
    Guarda los resultados de la extracción en tres archivos:
    - JSON de páginas
    - TXT concatenado de texto
    - Archivo de tokens por página
    """
    if not os.path.exists(carpeta_destino):
        os.makedirs(carpeta_destino)

    base_nombre = os.path.splitext(nombre_base)[0]

    # JSON por páginas
    ruta_json = os.path.join(carpeta_destino, f"{base_nombre}_resultado_paginas.json")
    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
    print(f"[guardar_archivos] → Guardando JSON en {os.path.basename(ruta_json)}")

    # Texto plano
    texto_completo = ""
    for pagina in resultados:
        for elem in pagina.get("elementos", []):
            texto = elem.get("contenido", "")
            # Asegurar que el contenido es una cadena
            if isinstance(texto, list):
                texto = " ".join(str(t) for t in texto)
            else:
                texto = str(texto)
            if texto:
                texto_completo += texto + "\n"

    ruta_txt = os.path.join(carpeta_destino, f"{base_nombre}.txt")
    with open(ruta_txt, "w", encoding="utf-8") as f:
        f.write(texto_completo.strip())
    print(f"[guardar_archivos] → Guardando texto plano en {os.path.basename(ruta_txt)}")

    # Tokens por página
    ruta_tokens = os.path.join(carpeta_destino, f"{base_nombre}_tokens.txt")
    with open(ruta_tokens, "w", encoding="utf-8") as f:
        for pagina in resultados:
            num = pagina.get("pagina", "N/A")
            tokens = pagina.get("tokens", "N/A")
            f.write(f"Página {num}: {tokens} tokens\n")
    print(f"[guardar_archivos] → Guardando tokens en {os.path.basename(ruta_tokens)}")

    print("[guardar_archivos] ✅ Archivos guardados correctamente")

def normalizar_nombre(nombre):
    """
    Normaliza el nombre de un archivo o documento eliminando tildes, espacios y caracteres especiales.
    """
    nombre = nombre.lower()
    nombre = unicodedata.normalize('NFKD', nombre).encode('ascii', 'ignore').decode('ascii')
    nombre = re.sub(r'[^\w\s-]', '', nombre)
    nombre = re.sub(r'[-\s]+', '_', nombre)
    return nombre
