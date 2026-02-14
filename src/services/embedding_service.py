import os
import json
import redis
import traceback
from urllib.parse import urlparse
from tqdm import tqdm
from src.embeddings import generar_embedding
from src.utils.clean_text import limpiar_texto
from src.utils.file_utils import normalizar_nombre
from datetime import datetime
from src.services.licitacion_service import get_or_create_licitacion
from src.config import REDIS_HOST, REDIS_PORT, REDIS_USERNAME, REDIS_PASSWORD, REDIS_DB

# ==========================================================
# ADAPTATION LAYER
# ==========================================================

MODEL_EMBEDDING = "text-embedding-3-small"
REDIS_URL = f"redis://{REDIS_USERNAME}:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

# ==========================================================
# REDIS CLIENT
# ==========================================================
redis_url = urlparse(REDIS_URL)
r = redis.Redis(
    host=redis_url.hostname,
    port=redis_url.port,
    username=redis_url.username,
    password=redis_url.password,
    db=0,
    ssl=False
)


def _contenido_a_texto(valor):
    if isinstance(valor, list):
        return "\n".join(str(v) for v in valor if v)
    if isinstance(valor, str):
        return valor
    return ""


def guardar_hash(clave, embedding, texto):
    try:
        r.hset(clave, mapping={
            "embedding": json.dumps(embedding),
            "texto": texto
        })
    except Exception as e:
        print(f"[❌ ERROR] No se pudo guardar en Redis ({clave}): {e}")


def run_embedding_batch(doc_id):
    from src.services.semantic_extraction.runner import run_semantic_extraction
    # from utils.file_utils import normalizar_nombre
    doc_id = normalizar_nombre(doc_id)

    ruta = os.path.join("archivos_texto", doc_id)
    if not os.path.exists(ruta):
        print(f"[❌ ERROR] Carpeta no encontrada: {ruta}")
        return

    archivos = [f for f in os.listdir(ruta) if f.endswith(".json") and "_pag_" in f]
    if not archivos:
        print(f"[⚠️] No se encontraron archivos JSON de páginas en {ruta}")
        return

    errores = []
    doc_id_normalizado = doc_id

    try:
        print("[📌] Registrando / obteniendo licitación…")
        licitacion_uuid = get_or_create_licitacion(doc_id)
        print(f"[✅] Licitación activa → UUID: {licitacion_uuid}")
    except Exception as e:
        print("[❌ ERROR] No se pudo registrar la licitación")
        traceback.print_exc()
        return

    for archivo in tqdm(archivos, desc=f"[🔍] Procesando {len(archivos)} páginas"):
        try:
            path_archivo = os.path.join(ruta, archivo)
            with open(path_archivo, encoding="utf-8") as f:
                data = json.load(f)

            pagina = data.get("pagina", -1)
            elementos = data.get("elementos", [])

            contenido_total = "\n\n".join(
                _contenido_a_texto(e.get("contenido"))
                for e in elementos
                if _contenido_a_texto(e.get("contenido"))
            )

            contenido_total = limpiar_texto(contenido_total)

            if contenido_total:
                clave_pag = f"doc_raw_page:{doc_id_normalizado}:p{pagina}_full"
                embedding = generar_embedding(contenido_total, model=MODEL_EMBEDDING)
                guardar_hash(clave_pag, embedding, contenido_total)

            for i, elem in enumerate(elementos):
                contenido = _contenido_a_texto(elem.get("contenido"))
                if not contenido:
                    continue
                contenido = limpiar_texto(contenido)
                if not contenido:
                    continue
                clave_elem = f"doc_raw_page:{doc_id_normalizado}:p{pagina}_e{i+1}"
                embedding = generar_embedding(contenido, model=MODEL_EMBEDDING)
                guardar_hash(clave_elem, embedding, contenido)

        except Exception as e:
            errores.append((archivo, f"❌ Error: {e}"))
            traceback.print_exc()

    try:
        texto_completo = ""
        for archivo in archivos:
            path_archivo = os.path.join(ruta, archivo)
            with open(path_archivo, encoding="utf-8") as f:
                data = json.load(f)

            elementos = data.get("elementos", [])

            texto_completo += "\n\n".join(
                _contenido_a_texto(e.get("contenido"))
                for e in elementos
                if _contenido_a_texto(e.get("contenido"))
            ) + "\n"

        texto_completo = limpiar_texto(texto_completo.strip())
        if texto_completo:
            emb_doc = generar_embedding(texto_completo, model=MODEL_EMBEDDING)
            r.hset(
                f"doc_raw:{doc_id_normalizado}",
                mapping={
                    "nombre_original": doc_id,
                    "doc_id": doc_id_normalizado,
                    "texto": texto_completo,
                    "content": texto_completo,
                    "embedding": json.dumps(emb_doc),
                    "pages_count": len(archivos),
                    "filename": doc_id,
                    "timestamp": datetime.now().isoformat(),
                },
            )
    except Exception as e:
        print(f"[❌ ERROR] Fallo embedding documento completo: {e}")
        traceback.print_exc()

    # ===============================
    # EXTRACCIÓN SEMÁNTICA
    # ===============================

    # --- DATOS BÁSICOS ---
    try:
        print("[SEMANTIC] Iniciando extraccion semantica: DATOS_BASICOS_LICITACION")
        run_semantic_extraction(
            licitacion_id=licitacion_uuid,
            concepto="DATOS_BASICOS_LICITACION",
            documento_ids=[doc_id],
            nombre_licitacion=doc_id,
            top_k=30,
            min_score=0.15,
            prompt_version="prompt_datos_basicos_licitacion_v1.txt",
            extractor_version="semantic_extractor_v1",
        )
        print("[SEMANTIC] Extraccion semantica DATOS_BASICOS_LICITACION ejecutada")
    except Exception:
        print("[ERROR] Fallo en extraccion semantica DATOS_BASICOS_LICITACION")
        traceback.print_exc()


    try:
        print("[SEMANTIC] Iniciando extraccion semantica: ITEMS_LICITACION")
        run_semantic_extraction(
            licitacion_id=licitacion_uuid,
            concepto="ITEMS_LICITACION",
            documento_ids=[doc_id_normalizado],
            nombre_licitacion=doc_id,  # ✅ Cambio añadido
            top_k=30,
            min_score=0.15,
            prompt_version="prompt_items_licitacion_v1.txt",
            extractor_version="semantic_extractor_v1",
        )
        print("[SEMANTIC] Extraccion semantica ITEMS_LICITACION ejecutada")

    except Exception:
        print("[ERROR] Fallo en extraccion semantica ITEMS_LICITACION")
        traceback.print_exc()

    try:
        print("[SEMANTIC] Iniciando extraccion semantica: FINANZAS_LICITACION")
        run_semantic_extraction(
            licitacion_id=licitacion_uuid,
            concepto="FINANZAS_LICITACION",
            documento_ids=[doc_id_normalizado],
            nombre_licitacion=doc_id,  # ✅ Cambio añadido
            top_k=30,
            min_score=0.15,
            prompt_version="prompt_finanzas_licitacion_v1.txt",
            extractor_version="semantic_extractor_v1",
        )
        print("[SEMANTIC] Extraccion semantica FINANZAS_LICITACION ejecutada")

    except Exception:
        print("[ERROR] Fallo en extraccion semantica FINANZAS_LICITACION")
        traceback.print_exc()

    if errores:
        log_path = os.path.join(ruta, "errores_embedding.log")
        with open(log_path, "w", encoding="utf-8") as f:
            for archivo, error in errores:
                f.write(f"{archivo}: {error}\n")
        print(f"[⚠️] Errores registrados en: {log_path}")
    else:
        print("✅ Embeddings generados correctamente para todas las páginas")


# ==========================================================
# UTILIDADES AUXILIARES (para chat_embedding, etc)
# ==========================================================

def list_raw_docs():
    claves = r.keys("doc_raw:*")
    nombres = [k.decode().replace("doc_raw:", "") for k in claves]
    return sorted(nombres)


def run_chat_embedding(user_id, mensaje, docs_normalizados, top_k=5):
    import numpy as np

    vector_consulta = generar_embedding(mensaje, model=MODEL_EMBEDDING)
    if not vector_consulta:
        return {
            "user_id": user_id,
            "message": mensaje,
            "resultados": []
        }

    todos_resultados = []
    for doc_id in docs_normalizados:
        patron = f"doc_raw_page:{doc_id}:*"
        claves = list(r.scan_iter(match=patron))

        for k in claves:
            datos = r.hgetall(k)
            if not datos:
                continue

            try:
                emb = json.loads(datos.get(b"embedding", b"[]").decode())
                txt = datos.get(b"texto", b"").decode()
                if not emb:
                    continue

                dist = np.linalg.norm(
                    json.loads(json.dumps(vector_consulta)) - json.loads(json.dumps(emb))
                )

                todos_resultados.append({
                    "documento": doc_id,
                    "clave": k.decode() if isinstance(k, bytes) else k,
                    "distancia": float(dist),
                    "contenido": txt
                })
            except Exception:
                continue

    todos_resultados.sort(key=lambda x: x["distancia"])
    return {
        "user_id": user_id,
        "message": mensaje,
        "resultados": todos_resultados[:top_k]
    }


def get_doc_pdf_filename(doc_id):
    try:
        doc_data = r.hgetall(f"doc_raw:{doc_id}")
        if doc_data:
            nombre_original = doc_data.get(b"nombre_original", b"").decode()
            filename = doc_data.get(b"filename", b"").decode()
            if nombre_original.lower().endswith(".pdf"):
                return nombre_original
            if filename:
                return f"{filename}.pdf" if not filename.lower().endswith(".pdf") else filename
        return None
    except Exception as e:
        print(f"[chat_embedding] Error obteniendo filename para {doc_id}: {e}")
        return None
