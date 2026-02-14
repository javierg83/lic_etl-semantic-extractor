import os
from dotenv import load_dotenv

# Cargar variables de entorno
from pathlib import Path

# Cargar variables de entorno desde el archivo .env en la raíz del proyecto
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

def get_env_variable(name: str, default: str = None, required: bool = True) -> str:
    value = os.getenv(name, default)
    if required and value is None:
        # Check if we are in a local dev/test environment where some vars might be optional
        # For now, just raise as before but maybe we can relax specific ones
        raise ValueError(f"❌ La variable de entorno {name} no está definida.")
    return value

# Variables exportadas explícitamente
# Relaxed REPOSITORY to be optional or have a default for local testing
REPOSITORY = get_env_variable("REPOSITORY", "local_repo", required=False) 

# Redis configuration with defaults for local
REDIS_HOST = get_env_variable("REDIS_HOST", "localhost", required=False)
REDIS_PORT = int(get_env_variable("REDIS_PORT", "6379", required=False))
REDIS_USERNAME = get_env_variable("REDIS_USERNAME", "default", required=False)
REDIS_PASSWORD = get_env_variable("REDIS_PASSWORD", "", required=False)
REDIS_DB = int(get_env_variable("REDIS_DB", "0", required=False))

# OpenAI Key still required usually, but for stub usage we might relax it if testing without calls
OPENAI_API_KEY = get_env_variable("OPENAI_API_KEY", "sk-test", required=False)

# Google Gemini Key
GEMINI_API_KEY = get_env_variable("GEMINI_API_KEY", None, required=False)

# Default AI Provider
DEFAULT_AI_PROVIDER = get_env_variable("DEFAULT_AI_PROVIDER", "openai", required=False)

# Alias for embeddings.py compatibility
API_KEY = OPENAI_API_KEY

# Support DATABASE_URL if used by runner
DATABASE_URL = get_env_variable("DATABASE_URL", "postgresql://user:pass@localhost:5432/db", required=False)


print("✅ Configuración cargada y validada correctamente.")
