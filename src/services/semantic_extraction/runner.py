
import json
import os
import traceback
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urlparse
import re

import psycopg2
import redis

# Adapted imports
from src.config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD, REDIS_USERNAME
from src.services.embedding_service import generar_embedding
from src.services.semantic_extraction.registry import get_extractor

# MODO_DEBUG = True
MODO_DEBUG = os.getenv("MODO_DEBUG", "False").lower() == "true"
MODEL_EMBEDDING = "text-embedding-3-small" # Hardcoded or from config

def _json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# Redis configuration adapted to src/config.py variables
# The original code used a REDIS_URL, but our config uses individual fields.
# We will reconstruct connection here or use the utils.redis_client if preferred.
# For now, to keep it similar to the original, I'll instantiate it here.

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    username=REDIS_USERNAME,
    password=REDIS_PASSWORD,
    db=REDIS_DB,
    decode_responses=False # Original code used bytes for some operations
)

DATABASE_URL = os.getenv("DATABASE_URL")

def _get_pg_conn():
    if not DATABASE_URL:
        # Fallback to individual vars if needed, but original used DATABASE_URL
        raise RuntimeError("DATABASE_URL no está definido en el entorno")
    return psycopg2.connect(DATABASE_URL)

def load_documents_to_memory(documento_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Carga TODOS los chunks de los documentos solicitados en memoria RAM.
    Optimización: Evita ir a Redis por cada query.
    """
    import numpy as np
    cached_chunks = []
    print(f"[CACHE] Cargando documentos en memoria: {documento_ids}")
    
    for doc_id in documento_ids:
        # Intentar patrón 1: "doc_raw_page:{doc_id}:*_full" (Legacy / Semantic Loader)
        patterns = [
            f"doc_raw_page:{doc_id}*", # Match más amplio para capturar suffix como ':p1_full' o '_page1'
            f"pdf:{doc_id}:chunk:*"  # Pattern legacy
        ]
        
        found_any = False
        for pattern in patterns:
            # En lugar de scan_iter (que puede ser muy lento por red en bases grandes),
            # intentamos un keys directo si el patrón es muy restrictivo
            keys = redis_client.keys(pattern)
            if not keys:
                continue
                
            found_any = True
            
            # Usar pipeline para traer todos los hgetall de golpe
            pipe = redis_client.pipeline()
            # Convert bytes to string if keys are bytes
            decoded_keys = [k.decode() if isinstance(k, bytes) else k for k in keys]
            
            for key in decoded_keys:
                pipe.hgetall(key)
                
            resultados_pipe = pipe.execute()
            
            for key, data in zip(decoded_keys, resultados_pipe):
                if not data:
                    continue
                try:
                    emb_bytes = data.get(b"vector") or data.get(b"embedding") or b"[]"
                    texto_bytes = data.get(b"text") or data.get(b"texto") or b""
                    
                    if not emb_bytes or not texto_bytes:
                        continue

                    # Manejo de JSON string vs raw bytes
                    if isinstance(emb_bytes, bytes):
                        emb_str = emb_bytes.decode()
                    else:
                        emb_str = emb_bytes
                        
                    if isinstance(texto_bytes, bytes):
                        texto_str = texto_bytes.decode()
                    else:
                        texto_str = texto_bytes

                    emb = json.loads(emb_str)
                    
                    cached_chunks.append({
                        "redis_key": key,
                        "texto": texto_str,
                        "embedding": np.array(emb, dtype=np.float32)
                    })
                except Exception as e:
                    print(f"[⚠️] Error procesando pipeline Redis para la clave {key}: {e}")
                    continue
            
            if found_any:
                print(f"[CACHE] Encontrados {len(decoded_keys)} chunks con patrón: {pattern}")
    
    print(f"[CACHE] Total chunks cargados en RAM: {len(cached_chunks)}")
    return cached_chunks

def semantic_search_in_memory(query: str, cached_chunks: List[Dict[str, Any]], top_k: int, min_score: float) -> List[Dict[str, Any]]:
    import numpy as np

    print(f"[magnifier] Generando embedding para query: {query}")
    vector = generar_embedding(query, model=MODEL_EMBEDDING)
    if not vector:
        return []

    if not cached_chunks:
        return []

    q_vec = np.array(vector, dtype=np.float32)
    
    resultados = []
    # Vectorización podría ser aún más rápida con matrices, pero este loop simple en memoria ya es 100x más rápido que Redis roundtrip
    for chunk in cached_chunks:
        # Calcular distancia ecludiana (o coseno si estuvieran normalizados, asumimos ecludiana por el código anterior)
        dist = float(np.linalg.norm(q_vec - chunk["embedding"]))
        
        # Guardamos todo y filtramos por top_k después
        resultados.append({
            "redis_key": chunk["redis_key"],
            "texto": chunk["texto"],
            "distancia": dist,
        })

    resultados.sort(key=lambda x: x["distancia"])
    
    # Filtrado post-sort si fuera necesario, pero el top_k manda.
    finales = resultados[:top_k]
    
    print(f"[magnifier] Resultados en memoria para '{query}': {len(finales)} (Mejor dist={finales[0]['distancia'] if finales else 'N/A'})")

    for i, r in enumerate(finales):
        pass # Silenciar log verbose por cada query para ganar velocidad, o dejarlo si se requiere debug

    return finales

def build_context(chunks: List[Dict[str, Any]]) -> str:
    bloques = []
    import re
    for c in chunks:
        redis_key = c['redis_key']
        nombre_archivo = "Documento Desconocido"
        try:
            # Pattern: doc_raw_page:<lic_int>_<file_int>_<filename>:p<page>...
            match = re.search(r"doc_raw_page:\d+_\d+_(.+?):p\d+", redis_key)
            if match:
                nombre_archivo = match.group(1)
            else:
                match_legacy = re.search(r"pdf:([^:]+):chunk", redis_key)
                if match_legacy:
                    nombre_archivo = match_legacy.group(1)
        except Exception:
            pass

        bloques.append(f"[ARCHIVO: {nombre_archivo} | REDIS_KEY: {redis_key}]\n{c['texto']}")
    return "\n\n---\n\n".join(bloques)

def _call_llm(prompt: str) -> str:
    from src.services.llm_service import run_llm_raw
    return run_llm_raw(prompt)

def _sanitize(text):
    text = re.sub(r"[^a-zA-Z0-9_-]+", "_", text.strip().lower())
    return text or "sin_nombre"

def _guardar_json_en_disco(nombre_licitacion: str, concepto: str, result: dict):
    if not MODO_DEBUG:
        return
    base_dir = os.path.join("salida_json", _sanitize(nombre_licitacion))
    os.makedirs(base_dir, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"{concepto.lower()}_{timestamp}.json"
    path_completo = os.path.join(base_dir, nombre_archivo)
    with open(path_completo, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False, default=_json_serial)
    print(f"[📁] Resultado guardado en: {path_completo}")

def run_semantic_extraction(
    *,
    licitacion_id: str,
    concepto: str,
    documento_ids: List[str],
    nombre_licitacion: str = "sin_nombre",
    top_k: int = 30,
    min_score: float = 0.25,
    prompt_version: str | None = None,
    extractor_version: str | None = None,
) -> Dict[str, Any]:

    print(f"[SEMANTIC] Ejecutando extractor semantico: {concepto}")
    extractor_cls = get_extractor(concepto)
    extractor = extractor_cls(licitacion_id=licitacion_id)
    extractor.prompt_version = prompt_version
    extractor.extractor_version = extractor_version

    # --- OPTIMIZACIÓN: Cargar cache una sola vez ---
    cached_chunks = load_documents_to_memory(documento_ids)
    if not cached_chunks:
         print(f"[SEMANTIC] ⚠️ No se cargaron chunks en memoria. Posiblemente doc_id incorrecto o vacío.")

    semantic_chunks = []
    queries = extractor._call_build_queries()
    
    for query in queries:
        # Usar la búsqueda en memoria optimizada
        filtros = semantic_search_in_memory(query, cached_chunks, top_k, min_score)
        semantic_chunks.extend(filtros)

    if not semantic_chunks:
        # Fallback o error warning, pero no romper si no hay matches exactos
        print("[SEMANTIC] No se encontraron fragmentos relevantes (o cache vacia). continuando con contexto vacio.")
        # raise RuntimeError("No se encontraron fragmentos relevantes en Redis")

    # DEDUPLICAR CHUNKS para evitar enviar filas repetidas múltiples veces
    unique_chunks_dict = {}
    for c in semantic_chunks:
        k = c["redis_key"]
        # Quedarse con el chunk con mejor distancia (menor)
        if k not in unique_chunks_dict or c.get("distancia", 999.0) < unique_chunks_dict[k].get("distancia", 999.0):
            unique_chunks_dict[k] = c
    semantic_chunks = list(unique_chunks_dict.values())

    # --- DEBUGGING LÓGICA: GuardarChunks para análisis ---
    debug_log = {
        "licitacion_id": licitacion_id,
        "total_chunks_unicos": len(semantic_chunks),
        "chunks": [{"key": c["redis_key"], "distancia": c.get("distancia")} for c in semantic_chunks],
        "batches": []
    }
    # -----------------------------------------------------

    # REVISAR Y APLICAR BATCHING DINÁMICO (Por longitud de texto)
    is_batch_mode = False
    if concepto == "ITEMS_LICITACION" and len(semantic_chunks) > 0:
        is_batch_mode = True

    if is_batch_mode:
        print(f"[SEMANTIC] 📦 Activando MODO BATCH DINÁMICO para {len(semantic_chunks)} chunks totales.")
        
        all_items = []
        all_especificaciones = []
        all_warnings = []
        combined_resumen = {"observaciones": "Procesamiento en lotes.", "total_items_detectados": 0}
        
        # Ordenamos los chunks para mantener la secuencia o relevancia
        semantic_chunks.sort(key=lambda x: x.get("distancia", 999.0))
        
        MAX_CHARS_PER_BATCH = 12000  # Estimación segura para no saturar el LLM
        MAX_CHUNKS_PER_BATCH = 50    # Límite adicional para evitar "pereza" por exceso de instrucciones
        
        batches = []
        current_batch = []
        current_chars = 0
        
        for chunk in semantic_chunks:
            chunk_len = len(chunk.get("texto", ""))
            
            # Condición Mixta: Cortar el lote si excedemos caracteres o si alcanzamos el tope de chunks
            if (current_chars + chunk_len > MAX_CHARS_PER_BATCH or len(current_batch) >= MAX_CHUNKS_PER_BATCH) and current_batch:
                batches.append(current_batch)
                current_batch = [chunk]
                current_chars = chunk_len
            else:
                current_batch.append(chunk)
                current_chars += chunk_len
                
        if current_batch:
            batches.append(current_batch)

        total_batches = len(batches)
        
        for i, batch_chunks in enumerate(batches):
            print(f"[SEMANTIC] 📦 Procesando Batch {i+1}/{total_batches} ({len(batch_chunks)} chunks)...")
            
            context = build_context(list({c["redis_key"]: c for c in batch_chunks}.values()))
            try:
                # Resetear el flag _has_run del extractor para permitir múltiples pasadas en batch
                extractor._has_run = False
                batch_result = extractor.run(context)
                
                # Consolidar resultados
                items_generados = batch_result.get("items") or []
                all_items.extend(items_generados)
                all_especificaciones.extend(batch_result.get("especificaciones") or [])
                all_warnings.extend(batch_result.get("warnings") or [])
                
                print(f"   └─ Extraídos {len(items_generados)} ítems en este batch.")
                
                # --- DEBUGGING LÓGICA ---
                debug_log["batches"].append({
                    "batch_num": i + 1,
                    "chunks_enviados": len(batch_chunks),
                    "items_extraidos_llm": len(items_generados),
                    "items_names": [it.get("nombre_item") for it in items_generados]
                })
                # ------------------------
                
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"   ❌ Error en Batch {i+1}: {e}")
                
        # --- DEBUGGING LÓGICA: Guardar archivo ---
        if MODO_DEBUG:
            import time
            ts = time.strftime("%Y%m%d_%H%M%S")
            debug_filename = f"salida_json/debug_batching_{ts}.json"
            os.makedirs("salida_json", exist_ok=True)
            with open(debug_filename, "w", encoding="utf-8") as f:
                json.dump(debug_log, f, indent=2, ensure_ascii=False)
            print(f"[SEMANTIC] 📝 Log de debug guardado en {debug_filename}")
        # -----------------------------------------
        
        # Construir resultado maestro
        result = {
            "concepto": concepto,
            "resumen": {
                "observaciones": f"Lotes dinámicos completados ({total_batches} llamadas). Obtenidos {len(all_items)} ítems.",
                "total_items_detectados": len(all_items)
            },
            "items": all_items,
            "especificaciones": all_especificaciones,
            "warnings": all_warnings
        }
        
        print(f"[SEMANTIC] 📦 MODO BATCH FINALIZADO. Total ítems extraídos combinados: {len(all_items)}")

    else:
        # LÓGICA ESTÁNDAR ORIGINAL P/ OTROS CONCEPTOS NO ITEMS
        semantic_chunks.sort(key=lambda x: x.get("distancia", 999.0))
        safe_chunks = semantic_chunks[:80]
        
        context = build_context(safe_chunks)
        print(f"[SEMANTIC] Contexto final tiene {len(context)} caracteres (limitado a {len(safe_chunks)} chunks para seguridad)")
    
        print(f"[SEMANTIC] Ejecutando extractor.run()...")
        result = extractor.run(context)

    try:
        _guardar_json_en_disco(nombre_licitacion, concepto, result)
    except Exception as e:
        print(f"[⚠️] Error guardando archivo JSON en disco: {e}")

    if MODO_DEBUG:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        nombre_archivo = f"debug_semantic_{concepto}_{ts}.json"
        print(f"\n[DEBUG] Resultado normalizado:\n")
        print(json.dumps(result, indent=2, ensure_ascii=False, default=_json_serial))
        with open(nombre_archivo, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False, default=_json_serial)
        print(f"\n[DEBUG] Resultado guardado en archivo: {nombre_archivo}")
        return {
            "status": "DEBUG_ONLY",
            "concepto": concepto,
            "mensaje": "No se escribió en base de datos",
        }

    print("[💾] Guardando en base de datos...")
    conn = _get_pg_conn()
    cur = conn.cursor()
    semantic_run_id = None

    try:
        cur.execute("""
            UPDATE semantic_runs
            SET is_current = false
            WHERE licitacion_id = %s AND concepto = %s AND is_current = true
        """, (licitacion_id, concepto))

        cur.execute("""
            INSERT INTO semantic_runs
            (licitacion_id, concepto, is_current, prompt_version, extractor_version)
            VALUES (%s, %s, true, %s, %s)
            RETURNING id
        """, (licitacion_id, concepto, prompt_version, extractor_version))

        semantic_run_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO semantic_results (semantic_run_id, concepto, resultado_json)
            VALUES (%s, %s, %s)
        """, (semantic_run_id, concepto, json.dumps(result, default=_json_serial)))

        # Obtener mapa de UUIDs de archivos
        from src.services.licitacion_service import obtener_mapa_uuid_por_interno
        mapa_archivos = obtener_mapa_uuid_por_interno(licitacion_id)
        print(f"[SEMANTIC] Mapa de archivos cargado: {len(mapa_archivos)} documentos")

        for c in semantic_chunks:
            # Parsear metadata desde redis_key
            # Formato esperado: doc_raw_page:<lic_int>_<file_int>_<name>:p<page>...
            # Ejemplo: doc_raw_page:10_5_archivo.pdf:p3_full
            
            redis_key = c["redis_key"]
            pagina = None
            documento_uuid = None
            
            try:
                # 1. Extraer ID Interno del archivo
                # Buscamos el bloque entre "doc_raw_page:" y ":p"
                # Regex: doc_raw_page:\d+_(\d+)_.*:p(\d+)
                match = re.search(r"doc_raw_page:\d+_(\d+)_.+:p(\d+)", redis_key)
                if match:
                    file_int_id = match.group(1)
                    page_num = match.group(2)
                    
                    documento_uuid = mapa_archivos.get(file_int_id)
                    pagina = int(page_num)
            except Exception as e:
                print(f"[⚠️] Error parseando metadata de clave '{redis_key}': {e}")

            cur.execute("""
                INSERT INTO semantic_evidences (semantic_run_id, redis_key, texto_fragmento, score_similitud, pagina, documento_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                semantic_run_id, 
                c["redis_key"], 
                c["texto"],
                c.get("distancia"), # Score (distancia)
                pagina,
                documento_uuid
            ))

        conn.commit()
        print("[✅] Extracción semántica persistida correctamente")

    except Exception:
        conn.rollback()
        traceback.print_exc()
        raise

    finally:
        cur.close()

    # NOTE: The original code had specific logic for different concepts (ITEMS, FINANZAS, etc.)
    # calling services.licitacion_service. This service does not exist in this project yet.
    # For now, I will comment out the service calls to avoid import errors, 
    # but keep the structure to implement them later or ask the user for those services.
    
    try:
        if concepto == "ITEMS_LICITACION":
            from src.services.licitacion_service import guardar_items_licitacion, guardar_especificaciones_tecnicas
            
            # Guardar Items
            if result.get("items"):
                guardar_items_licitacion(conn, licitacion_id, semantic_run_id, result["items"])
            
            # Guardar Especificaciones (si existen)
            if "item_especificaciones" in result:
                guardar_especificaciones_tecnicas(conn, semantic_run_id, result["item_especificaciones"])

            # TODO: Homologación automática (descomentar cuando el servicio esté disponible)
            # if result.get("items"):
            #     try:
            #         from src.services.homologacion.homologacion_service import ejecutar_homologacion_automatica
            #         homologacion_resultado = ejecutar_homologacion_automatica(...)
            #     except Exception as e: ...

        elif concepto == "FINANZAS_LICITACION":
            from src.services.licitacion_service import guardar_finanzas_licitacion
            import datetime
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{now}] 🏦 Procesando resultado de FINANZAS_LICITACION para licitacion_id={licitacion_id}")
            
            if result.get("finanzas"):
                try:
                    guardar_finanzas_licitacion(conn, licitacion_id, result["finanzas"], semantic_run_id)
                    print(f"[{now}] ✅ Datos financieros guardados correctamente en BD")
                except Exception as e:
                    print(f"[{now}] ❌ Error al guardar datos financieros: {str(e)}")
                    # No hacemos raise para no abortar todo el flujo si falla finanzas
                    # raise 

        elif concepto == "DATOS_BASICOS_LICITACION":
            from src.services.licitacion_service import actualizar_datos_basicos_licitacion
            
            if result.get("datos_basicos"):
                actualizar_datos_basicos_licitacion(
                    licitacion_id,
                    result.get("datos_basicos", {}),
                    semantic_run_id
                )

        elif concepto == "ENTREGAS_LICITACION":
            from src.services.licitacion_service import guardar_entregas_licitacion
            import datetime
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{now}] 🚚 Procesando resultado de ENTREGAS_LICITACION para licitacion_id={licitacion_id}")
            
            try:
                guardar_entregas_licitacion(conn, licitacion_id, result, semantic_run_id)
                print(f"[{now}] ✅ Datos de entregas guardados correctamente en BD")
            except Exception as e:
                print(f"[{now}] ❌ Error al guardar datos de entregas: {str(e)}")

    finally:
        conn.close()

    return {
        "status": "OK",
        "concepto": concepto,
        "semantic_run_id": str(semantic_run_id),
    }

if __name__ == "__main__":
    import sys
    # Example usage for testing
    if len(sys.argv) < 3:
        print("Uso: python runner.py <CONCEPTO> <DOC_ID> [LICITACION_ID]")
        sys.exit(1)
    
    # ... (Rest of CLI logic if needed, but dependencies are not fully there so skipping for now)
