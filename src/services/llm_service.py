from src.services.ai_engine.factory import AIProviderFactory
from src.services.ai_engine.prompt_loader import PromptLoader
from src import config
import json
from datetime import datetime
import os

def _guardar_llm_raw_json(raw_text: str, tag: str = "llm_response"):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"debug_llm_raw_{tag}_{ts}.json"

    try:
        parsed = json.loads(raw_text)
        contenido = parsed
    except Exception:
        contenido = {"raw_text": raw_text}

    # Ensure debug directory exists or write to current
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(contenido, f, indent=2, ensure_ascii=False)

    print(f"[🧪 DEBUG] Respuesta LLM cruda guardada en: {filename}")


def run_llm_raw(prompt_path_or_text: str, overrides: dict = None, licitacion_id: str = "default", action: str = "EXTRACCION_SEMANTICA") -> str:
    """
    Ejecuta una llamada al LLM. 
    Argumento 'prompt_path_or_text': 
      - Puede ser una ruta a un archivo .txt con YAML frontmatter.
      - O un string directo (en cuyo caso usa defaults).
    """
    
    # 1. Configuración por defecto
    # Use config from src.config or default
    default_provider = getattr(config, "DEFAULT_AI_PROVIDER", "openai")
    
    config_dict = {
        "engine": default_provider,
        "model": "gpt-4o" if default_provider == "openai" else "gemini-1.5-pro",
        "temperature": 0.0
    }
    
    prompt_text = prompt_path_or_text
    
    # 2. Intentar cargar desde archivo si parece una ruta y existe
    # Check if absolute or relative to project root
    if os.path.exists(prompt_path_or_text) and prompt_path_or_text.endswith(".txt"):
        print(f"[llm_service] 📂 Cargando prompt desde archivo: {prompt_path_or_text}")
        loaded_config, loaded_text = PromptLoader.load_prompt(prompt_path_or_text)
        config_dict.update(loaded_config)
        prompt_text = loaded_text
    else:
        # Es texto directo, asumimos config default o overrides
        pass

    # 3. Aplicar overrides manuales si existen
    if overrides:
        config_dict.update(overrides)

    print(f"[llm_service] 🧠 Usando Motor: {config_dict.get('engine')} | Modelo: {config_dict.get('model')}")

    # 4. Obtener Provider
    provider = AIProviderFactory.get_provider(config_dict)
    
    # 5. Ejecutar
    system_prompt = (
        "Eres un asistente experto en análisis de documentos públicos, "
        "legales y técnicos. Tu tarea es extraer información estructurada "
        "de forma precisa, sin inventar datos."
    )
    
    reply, usage = provider.generate_text(
        prompt=prompt_text,
        system_prompt=system_prompt,
        config=config_dict
    )

    print(f"[llm_service] ✅ Respuesta recibida. Tokens: {usage}")
    _guardar_llm_raw_json(reply, tag="generic_response")

    # Registrar uso de tokens
    from src.utils.metrics import log_ai_usage
    log_ai_usage(
        licitacion_id=licitacion_id,
        action=action,
        provider=config_dict.get('engine', 'openai'),
        model=config_dict.get('model', 'gpt-4o'),
        input_tokens=usage.get("input", 0),
        output_tokens=usage.get("output", 0)
    )

    return reply.strip()


def run_llm_raw_with_tokens(prompt_path_or_text: str, overrides: dict = None, licitacion_id: str = "default", action: str = "EXTRACCION_SEMANTICA") -> dict:
    """
    Versión que retorna también los tokens.
    """
    # Reutilizamos lógica (simplificada, duplicada por claridad para no romper compatibilidad firma)
    default_provider = getattr(config, "DEFAULT_AI_PROVIDER", "openai")
    
    config_dict = {
        "engine": default_provider,
        "model": "gpt-4o" if default_provider == "openai" else "gemini-1.5-pro",
        "temperature": 0.0
    }
    
    prompt_text = prompt_path_or_text

    if os.path.exists(prompt_path_or_text) and prompt_path_or_text.endswith(".txt"):
         print(f"[llm_service] 📂 Cargando prompt desde archivo: {prompt_path_or_text}")
         loaded_config, loaded_text = PromptLoader.load_prompt(prompt_path_or_text)
         config_dict.update(loaded_config)
         prompt_text = loaded_text

    if overrides:
        config_dict.update(overrides)
        
    print(f"[llm_service] 🧠 Init llamada LLM. Motor: {config_dict.get('engine')} Modelo: {config_dict.get('model')}")

    provider = AIProviderFactory.get_provider(config_dict)
    
    system_prompt = (
        "Eres un asistente experto en análisis de documentos públicos, "
        "legales y técnicos. Tu tarea es extraer información estructurada "
        "de forma precisa, sin inventar datos."
    )

    reply, usage = provider.generate_text(
        prompt=prompt_text,
        system_prompt=system_prompt,
        config=config_dict
    )
    
    _guardar_llm_raw_json(reply, tag="generic_response")

    # Registrar uso de tokens
    from src.utils.metrics import log_ai_usage
    log_ai_usage(
        licitacion_id=licitacion_id,
        action=action,
        provider=config_dict.get('engine', 'openai'),
        model=config_dict.get('model', 'gpt-4o'),
        input_tokens=usage.get("input", 0),
        output_tokens=usage.get("output", 0)
    )

    return {
        "respuesta": reply.strip(),
        "tokens_input": usage.get("input", 0),
        "tokens_output": usage.get("output", 0)
    }
