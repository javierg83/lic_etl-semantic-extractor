import re

def limpiar_texto(texto):
    if not texto or not isinstance(texto, str):
        return ""
    texto = re.sub(r'\s+', ' ', texto)
    return texto.strip()
