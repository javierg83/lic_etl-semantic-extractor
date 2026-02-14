from typing import Dict, Any, Optional
from src import config
from .base import BaseAIProvider
from .openai_provider import OpenAIProvider
from .gemini_provider import GeminiProvider

class AIProviderFactory:
    _instances: Dict[str, BaseAIProvider] = {}

    @staticmethod
    def get_provider(config_dict: Dict[str, Any]) -> BaseAIProvider:
        """
        Retorna una instancia del proveedor adecuado según la configuración.
        """
        engine = config_dict.get("engine", config.DEFAULT_AI_PROVIDER).lower()

        if engine in AIProviderFactory._instances:
            return AIProviderFactory._instances[engine]

        provider = None
        if engine == "openai":
            api_key = config.OPENAI_API_KEY
            if not api_key:
                raise ValueError("OPENAI_API_KEY not found in config")
            provider = OpenAIProvider(api_key)
        
        elif engine == "gemini":
            api_key = config.GEMINI_API_KEY
            if not api_key:
                raise ValueError("GEMINI_API_KEY not found in config")
            provider = GeminiProvider(api_key)
            
        else:
            raise ValueError(f"Unknown AI Engine: {engine}")

        AIProviderFactory._instances[engine] = provider
        return provider

    @staticmethod
    def create(provider_name: str) -> BaseAIProvider:
        """Helper rápido para crear por nombre"""
        return AIProviderFactory.get_provider({"engine": provider_name})
