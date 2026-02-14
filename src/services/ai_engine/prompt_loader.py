import yaml
import os
from typing import Dict, Any, Tuple

class PromptLoader:
    @staticmethod
    def load_prompt(file_path: str) -> Tuple[Dict[str, Any], str]:
        """
        Lee un archivo de prompt con formato YAML frontmatter.
        
        Args:
            file_path: Ruta absoluta al archivo .txt
            
        Returns:
            Tuple[Dict, str]: (configuración, contenido_texto_del_prompt)
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Prompt file not found: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Separar frontmatter (YAML) del contenido
        # Se asume que el archivo empieza con ---
        if content.startswith("---"):
            try:
                # Buscar el segundo separador '---'
                parts = content.split("---", 2)
                if len(parts) >= 3:
                    yaml_content = parts[1].strip()
                    text_content = parts[2].strip()
                    
                    config = yaml.safe_load(yaml_content) or {}
                    return config, text_content
            except Exception as e:
                print(f"[PromptLoader] ⚠️ Error parsing frontmatter for {file_path}: {e}")
        
        # Fallback para archivos sin frontmatter
        print(f"[PromptLoader] ℹ️ File {file_path} has no valid frontmatter. Using default config.")
        return {}, content.strip()
