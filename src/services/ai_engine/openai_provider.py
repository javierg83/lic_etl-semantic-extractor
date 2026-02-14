from .base import BaseAIProvider
from typing import Dict, Any, Tuple
import openai
import time
import json
import re
from datetime import datetime

class OpenAIProvider(BaseAIProvider):
    def __init__(self, api_key: str):
        self.client = openai.OpenAI(api_key=api_key)

    def generate_text(self, prompt: str, system_prompt: str, config: Dict[str, Any]) -> Tuple[str, Dict[str, int]]:
        model = config.get("model", "gpt-4o")
        temperature = config.get("temperature", 0.0)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]

        print(f"[OpenAIProvider] 🚀 Enviando solicitud a {model}...")
        resp = self.client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature
        )

        reply = resp.choices[0].message.content
        usage = {
            "input": resp.usage.prompt_tokens,
            "output": resp.usage.completion_tokens
        }
        return reply, usage

    def analyze_image(self, image_b64: str, prompt: str, system_prompt: str, config: Dict[str, Any]) -> Tuple[Any, str, int, int]:
        model = config.get("model", "gpt-4o")
        temperature = config.get("temperature", 0.0)
        timeout = config.get("timeout", 60.0)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}}
            ]}
        ]
        
        # Simulación de conteo simple si no hay respuesta de usage
        def count_tokens_fallback(text):
            return len(text) // 4

        try:
            t0 = time.time()
            print(f"[OpenAIProvider] 🚀 Enviando imagen a {model}...")
            resp = self.client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                timeout=timeout
            )
            dt = time.time() - t0
            print(f"[OpenAIProvider] ⏱️ Tiempo respuesta: {dt:.2f}s")
            
            raw = resp.choices[0].message.content.strip()
            
            # Limpieza básica de Markdown
            raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
            raw = re.sub(r"\s*```$", "", raw, flags=re.MULTILINE)

            usage = getattr(resp, 'usage', None)
            tokens_in = usage.prompt_tokens if usage else count_tokens_fallback(system_prompt + prompt)
            tokens_out = usage.completion_tokens if usage else count_tokens_fallback(raw)

            # Intentar parsear JSON si se espera
            elementos = []
            try:
                data = json.loads(raw)
                elementos = data.get('elementos', []) # Asumiendo estructura estándar del proyecto
            except json.JSONDecodeError:
                print(f"[OpenAIProvider] ⚠️ JSON inválido en respuesta.")

            return elementos, raw, tokens_in, tokens_out

        except Exception as e:
            print(f"[OpenAIProvider] ❌ Error: {e}")
            raise e
