from openai import OpenAI
from src import config

client = OpenAI(api_key=config.API_KEY)

def generar_embedding(texto, model="text-embedding-3-small"):
    """
    Genera un embedding para el texto utilizando el modelo especificado.

    Args:
        texto (str): Texto para el cual se generará el embedding.
        model (str): Modelo de embedding a utilizar (por defecto: text-embedding-3-small).

    Returns:
        list: Vector de embedding generado.
    """
    try:
        respuesta = client.embeddings.create(
            model=model,
            input=texto
        )
        vector = respuesta.data[0].embedding
        return vector
    except Exception as e:
        print(f"[❌ ERROR] Al generar embedding: {e}")
        return []

def get_embeddings(textos, model="text-embedding-3-small"):
    """
    Genera embeddings para una lista de textos.

    Args:
        textos (list[str]): Lista de textos para los cuales se generarán embeddings.
        model (str): Modelo de embedding a utilizar.

    Returns:
        list[list[float]]: Lista de vectores de embeddings.
    """
    try:
        respuesta = client.embeddings.create(
            model=model,
            input=textos
        )
        return [r.embedding for r in respuesta.data]
    except Exception as e:
        print(f"[❌ ERROR] Al generar embeddings múltiples: {e}")
        return []
