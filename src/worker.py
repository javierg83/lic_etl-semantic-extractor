import json
import redis
import time
import os
from src.config import REDIS_DB, REDIS_HOST, REDIS_PORT, REDIS_USERNAME, REDIS_PASSWORD
from src.graph.semantic_graph import build_semantic_graph
from src.graph.state import GraphState

def process_message(licitacion_id: str, documento_ids: list):
    print(f"🛠️ Procesando Semantic Extraction para ID: {licitacion_id} | Docs: {len(documento_ids)}")
    
    try:
        app = build_semantic_graph()
        
        initial_state = GraphState(
            licitacion_id=licitacion_id,
            documento_ids=documento_ids,
            document_text=None,
            extraction_finances=None,
            extraction_items=None,
            extraction_basic_data=None,
            extraction_entregas=None,
            homologation_result=None,
            status="init",
            errors=[],
            current_step="init"
        )
        
        # Invoke the graph
        result = app.invoke(initial_state)
        
        # Check output state
        if result.get("errors"):
             print(f"⚠️ Extracción finalizada con errores: {result.get('errors')}")
             from src.services.licitacion_service import actualizar_estado_licitacion
             from src.constants.states import LicitacionStatus
             actualizar_estado_licitacion(licitacion_id, LicitacionStatus.EXTRACCION_SEMANTICA_COMPLETADA)
        else:
             print(f"✅ Extracción completada exitosamente para {licitacion_id}")
             from src.services.licitacion_service import actualizar_estado_licitacion
             from src.constants.states import LicitacionStatus
             # Mark as Homologacion Completada if it reached the end of the graph correctly
             if result.get("current_step") == "homologation_completed" or "homologation_result" in result:
                actualizar_estado_licitacion(licitacion_id, LicitacionStatus.HOMOLOGACION_COMPLETADA)
             else:
                actualizar_estado_licitacion(licitacion_id, LicitacionStatus.EXTRACCION_SEMANTICA_COMPLETADA)
            
    except Exception as e:
        print(f"❌ Error procesando {licitacion_id}: {e}")
        import traceback
        traceback.print_exc()

def main():
    print(f"📡 Worker Semántico Iniciado.")
    print(f"🔗 Redis: {REDIS_HOST}:{REDIS_PORT}, DB: {REDIS_DB}")
    
    r = redis.Redis(
        host=REDIS_HOST, 
        port=REDIS_PORT, 
        db=REDIS_DB, 
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    
    QUEUE_NAME = "semantic_queue"
    
    while True:
        try:
            print(f"⏳ Esperando mensaje en '{QUEUE_NAME}'...")
            result = r.blpop(QUEUE_NAME, timeout=10)
            
            if result:
                _, message = result
                print(f"📥 Mensaje recibido: {message}")
                try:
                    data = json.loads(message)
                    lic_id = data.get("licitacion_id")
                    doc_ids = data.get("documento_ids", [])
                    
                    if lic_id and doc_ids:
                        # IMPORTANTE: El extractor de documentos guarda keys como "pdf:{filename}:chunk:{i}"
                        # El runner semántico espera que _load_documents_to_memory busque "doc_raw_page:{doc_id}:*"
                        # O debemos cambiar el runner o debemos adaptar aquí.
                        # Usamos la ejecución del grafo
                        from src.services.licitacion_service import actualizar_estado_licitacion
                        from src.constants.states import LicitacionStatus
                        
                        actualizar_estado_licitacion(lic_id, LicitacionStatus.EXTRACCION_SEMANTICA_EN_PROCESO)
                        process_message(lic_id, doc_ids)

                    else:
                        print("⚠️ Mensaje incompleto (falta licitacion_id o documento_ids)")
                except json.JSONDecodeError:
                    print(f"⚠️ Error decodificando JSON: {message}")
                    
        except redis.exceptions.ConnectionError:
            print("⚠️ Error de conexión con Redis. Reintentando...")
            time.sleep(5)
        except Exception as e:
            print(f"⚠️ Error inesperado en loop principal: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
