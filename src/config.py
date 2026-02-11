import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def get_env_variable(name: str, default: str = None, required: bool = True) -> str:
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"❌ La variable de entorno {name} no está definida.")
    return value

# Variables exportadas explícitamente
REPOSITORY = get_env_variable("REPOSITORY")
REDIS_HOST = get_env_variable("REDIS_HOST")
REDIS_PORT = int(get_env_variable("REDIS_PORT"))
REDIS_USERNAME = get_env_variable("REDIS_USERNAME", "default")
REDIS_PASSWORD = get_env_variable("REDIS_PASSWORD")
REDIS_DB = int(get_env_variable("REDIS_DB", "0"))
OPENAI_API_KEY = get_env_variable("OPENAI_API_KEY")

print("✅ Configuración cargada y validada correctamente.")
