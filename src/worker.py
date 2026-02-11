import json
import redis
import time
from src.config import REDIS_DB, REDIS_HOST, REDIS_PORT, REDIS_USERNAME, REDIS_PASSWORD
from src.graph.semantic_graph import build_semantic_graph
from src.graph.state import GraphState

def process_message(licitacion_id: str):
    print(f"🛠️ Procesando Semantic Extraction para ID: {licitacion_id}")
    
    try:
        app = build_semantic_graph()
        
        initial_state: GraphState = {
            "licitacion_id": licitacion_id,
            "document_text": None,
            "extraction_finances": None,
            "extraction_items": None,
            "status": "init",
            "errors": [],
            "current_step": "init"
        }
        
        result = app.invoke(initial_state)
        
        if result.get("status") == "completed":
            print(f"✅ Extracción completada para {licitacion_id}")
        else:
            print(f"⚠️ Extracción finalizó con estado: {result.get('status')}")
            
    except Exception as e:
        print(f"❌ Error procesando {licitacion_id}: {e}")

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
    
    QUEUE_NAME = "semantic_extraction_queue"
    
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
                    if lic_id:
                        process_message(lic_id)
                    else:
                        print("⚠️ Mensaje sin licitacion_id")
                except json.JSONDecodeError:
                    print(f"⚠️ Error decodificando JSON: {message}")
                    
        except redis.exceptions.ConnectionError:
            print("⚠️ Error de conexión con Redis. Reintentando...")
            time.sleep(5)
        except Exception as e:
            print(f"⚠️ Error inesperado: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
