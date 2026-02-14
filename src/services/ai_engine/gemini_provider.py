from .base import BaseAIProvider
from typing import Dict, Any, Tuple
import google.generativeai as genai
import json
import re
import time
import random

class GeminiProvider(BaseAIProvider):
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)

    def generate_text(self, prompt: str, system_prompt: str, config: Dict[str, Any]) -> Tuple[str, Dict[str, int]]:
        model_name = config.get("model", "gemini-1.5-pro")
        temperature = config.get("temperature", 0.0)
        
        generation_config = {
            "temperature": temperature,
        }

        # Gemini maneja el system prompt al instanciar el modelo o en generate_content dependiendo de la versión
        # Usaremos la configuración de modelo system_instruction si está disponible, o lo concatenamos.
        model = genai.GenerativeModel(
            model_name=model_name,
            system_instruction=system_prompt
        )

        print(f"[GeminiProvider] 🚀 Enviando solicitud a {model_name}...")
        response = model.generate_content(
            prompt,
            generation_config=generation_config
        )

        reply = response.text
        # Gemini usage metadata access might vary, using simple placeholder or accessing usage_metadata if available
        # usage = response.usage_metadata
        # tokens_in = usage.prompt_token_count
        # tokens_out = usage.candidates_token_count
        
        # Fallback si no está disponible fácil
        usage = {
            "input": 0, 
            "output": 0
        }
        if hasattr(response, 'usage_metadata'):
             usage["input"] = response.usage_metadata.prompt_token_count
             usage["output"] = response.usage_metadata.candidates_token_count

        return reply, usage

    def analyze_image(self, image_b64: str, prompt: str, system_prompt: str, config: Dict[str, Any]) -> Tuple[Any, str, int, int]:
        model_name = config.get("model", "gemini-1.5-pro")
        temperature = config.get("temperature", 0.0)

        # Gemini necesita la imagen decodificada como objeto blob o similar
        # "mime_type": "image/jpeg", "data": ...
        
        model = genai.GenerativeModel(
            model_name=model_name,
            system_instruction=system_prompt
        )
        
        print(f"[GeminiProvider] 🚀 Enviando imagen a {model_name}...")
        
        # En Google GenAI SDK, pasamos un diccionario simple para blob
        # Asumimos PNG o JPEG.
        image_part = {
            "mime_type": "image/png", 
            "data": image_b64 
        }

        # Retry logic for 429 Quota Exceeded
        max_retries = 3
        base_delay = 5 # seconds
        
        for attempt in range(max_retries + 1):
            try:
                response = model.generate_content(
                    [prompt, image_part],
                    generation_config={"temperature": temperature}
                )
                break # Success
            except Exception as e:
                # Check for 429 or quota related errors
                error_str = str(e)
                if "429" in error_str or "quota" in error_str.lower():
                    if attempt < max_retries:
                        delay = base_delay * (2 ** attempt) + 1 # Exponential: 5+1, 10+1, 20+1 ...
                        print(f"[GeminiProvider] ⏳ Quota exceeded (429). Retrying in {delay}s... (Attempt {attempt+1}/{max_retries})")
                        time.sleep(delay)
                        continue
                    else:
                        print(f"[GeminiProvider] ❌ Max retries reached for 429 error.")
                        raise e
                else:
                    # Other errors, fail immediately
                    raise e

        raw = response.text
        
        # Limpieza básica
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
        raw = re.sub(r"\s*```$", "", raw, flags=re.MULTILINE)

        tokens_in = 0
        tokens_out = 0
        if hasattr(response, 'usage_metadata'):
             tokens_in = response.usage_metadata.prompt_token_count
             tokens_out = response.usage_metadata.candidates_token_count

        elementos = []
        try:
            data = json.loads(raw)
            elementos = data.get('elementos', [])
        except json.JSONDecodeError:
             print(f"[GeminiProvider] ⚠️ JSON inválido en respuesta.")

        return elementos, raw, tokens_in, tokens_out
