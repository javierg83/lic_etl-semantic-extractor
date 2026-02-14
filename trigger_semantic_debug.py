import redis
import json
import argparse
import sys
from src.config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_USERNAME, REDIS_PASSWORD

def trigger_semantic_extraction(licitacion_id, filename):
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            username=REDIS_USERNAME,
            password=REDIS_PASSWORD,
            decode_responses=True
        )

        queue_msg = json.dumps({
            "licitacion_id": licitacion_id,
            "documento_ids": [filename] 
        })
        
        queue_name = "semantic_queue"
        
        print(f"🔌 Conectando a Redis: {REDIS_HOST}:{REDIS_PORT}")
        print(f"📤 Enviando mensaje a '{queue_name}'...")
        print(f"📄 Payload: {queue_msg}")
        
        r.rpush(queue_name, queue_msg)
        
        print("✅ Mensaje enviado exitosamente.")
        
    except Exception as e:
        print(f"❌ Error al enviar mensaje: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python trigger_semantic_debug.py <licitacion_id> <filename>")
        print("Ejemplo: python trigger_semantic_debug.py 1234-56-LP20 my_document.pdf")
        sys.exit(1)
        
    lic_id = sys.argv[1]
    filename = sys.argv[2]
    
    trigger_semantic_extraction(lic_id, filename)
