import os
import requests
import urllib3
# from src.config import settings

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuración del Backend Local
# Asumimos que el backend de licitaciones está corriendo en localhost:8000 si no se configura
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

def log_ai_usage(licitacion_id: str, action: str, provider: str, model: str, input_tokens: int, output_tokens: int):
    """
    Registra el consumo de tokens en el Backend Central de Licitaciones
    """
    if licitacion_id == "default" or not licitacion_id:
        print(f"⚠️ [Metrics] Ignorando métricas sin licitacion_id: {action}")
        return
        
    url = f"{BACKEND_URL}/licitaciones/{licitacion_id}/token-usage"
    
    payload = {
        "action": action,
        "provider": provider,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code in [200, 201]:
            print(f"📊 [Metrics] Uso registrado OK: {input_tokens}in, {output_tokens}out ({action})")
        else:
            print(f"⚠️ [Metrics] Backend falló al registrar tokens: {response.text}")
    except Exception as e:
        print(f"❌ [Metrics] Error enviando métricas de IA al backend: {e}")
