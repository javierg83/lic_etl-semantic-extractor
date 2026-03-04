from src.graph.state import GraphState
from src.nodes.base_node import BaseNode
from src.services.semantic_extraction.runner import run_semantic_extraction

class ExtractItemsNode(BaseNode):
    @classmethod
    def execute(cls, state: GraphState) -> dict: # Returns dict update
        licitacion_id = state.get("licitacion_id")
        documento_ids = state.get("documento_ids", [])
        
        if not licitacion_id:
            print(f"⚠️ [ExtractItemsNode] No licitacion_id provided in state.")
            return {}

        print(f"📦 [ExtractItemsNode] Ejecutando extracción de Ítems para licitacion_id={licitacion_id}")
        
        try:
            result = run_semantic_extraction(
                licitacion_id=licitacion_id,
                concepto="ITEMS_LICITACION",
                documento_ids=documento_ids,
                nombre_licitacion=f"lic_{licitacion_id}", 
                top_k=1000, 
                min_score=0.25
            )
            
            print(f"✅ [ExtractItemsNode] Extracción finalizada.")
            return {"extraction_items": result}
            
        except Exception as e:
            print(f"❌ [ExtractItemsNode] Error en extracción: {e}")
            return {"errors": [f"Items: {str(e)}"]}
