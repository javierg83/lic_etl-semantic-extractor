import operator
from typing import TypedDict, List, Dict, Any, Optional, Annotated

class GraphState(TypedDict):
    """
    Estado compartido del grafo de proceso de extracción semántica.
    """
    licitacion_id: str
    documento_ids: List[str] # Added this to match usage in nodes
    document_text: Optional[str]
    extraction_finances: Optional[Dict[str, Any]]
    extraction_items: Optional[Dict[str, Any]] # Changed to Dict as extractor returns wrapper with 'items' key
    extraction_basic_data: Optional[Dict[str, Any]]
    status: str
    errors: Annotated[List[str], operator.add] # Accumulate errors from parallel branches
    current_step: str
