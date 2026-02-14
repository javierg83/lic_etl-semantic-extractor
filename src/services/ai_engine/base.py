from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, Optional

class BaseAIProvider(ABC):
    """Clase base abstracta para proveedores de IA."""

    @abstractmethod
    def generate_text(self, prompt: str, system_prompt: str, config: Dict[str, Any]) -> Tuple[str, Dict[str, int]]:
        """
        Genera texto basado en un prompt.
        
        Args:
            prompt: El mensaje del usuario.
            system_prompt: El mensaje del sistema (contexto/rol).
            config: Diccionario de configuración (modelo, temperatura, etc).

        Returns:
            Tuple[str, Dict[str, int]]: 
                - Respuesta de texto generada.
                - Diccionario de uso de tokens {'input': int, 'output': int}.
        """
        pass

    @abstractmethod
    def analyze_image(self, image_b64: str, prompt: str, system_prompt: str, config: Dict[str, Any]) -> Tuple[Any, str, int, int]:
        """
        Analiza una imagen en base64.
        
        Args:
            image_b64: Imagen en base64.
            prompt: Instrucción específica para la imagen.
            system_prompt: Contexto del sistema.
            config: Diccionario de configuración.

        Returns:
            Tuple[Any, str, int, int]:
                - Elementos extraídos (depende de la lógica, normálmente lista o dict).
                - Respuesta raw (texto crudo).
                - Tokens input.
                - Tokens output.
        """
        pass
