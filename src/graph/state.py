from typing import TypedDict, List, Dict, Any, Optional

class GraphState(TypedDict):
    """
    Estado compartido del grafo de proceso de extracción semántica.
    """
    licitacion_id: str
    document_text: Optional[str]  # Texto concatenado o estructura de documentos
    extraction_finances: Optional[Dict[str, Any]]
    extraction_items: Optional[List[Dict[str, Any]]]
    status: str  # 'init', 'processing', 'completed', 'failed'
    errors: List[str]
    current_step: str
