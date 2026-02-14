from src.graph.state import GraphState
from src.nodes.base_node import BaseNode
from src.services.semantic_extraction.runner import run_semantic_extraction

class ExtractBasicDataNode(BaseNode):
    @classmethod
    def execute(cls, state: GraphState) -> dict:
        licitacion_id = state.get("licitacion_id")
        documento_ids = state.get("documento_ids", [])
        
        if not licitacion_id:
            print(f"⚠️ [ExtractBasicDataNode] No licitacion_id provided in state.")
            return {}

        print(f"📋 [ExtractBasicDataNode] Ejecutando extracción de Datos Básicos para licitacion_id={licitacion_id}")
        
        try:
            result = run_semantic_extraction(
                licitacion_id=licitacion_id,
                concepto="DATOS_BASICOS_LICITACION",
                documento_ids=documento_ids,
                nombre_licitacion=f"lic_{licitacion_id}",
                top_k=15, 
                min_score=0.3
            )
            
            print(f"✅ [ExtractBasicDataNode] Extracción de datos básicos finalizada.")
            return {"extraction_basic_data": result}

        except Exception as e:
            print(f"❌ [ExtractBasicDataNode] Error en extracción: {e}")
            return {"errors": [f"BasicData: {str(e)}"]}
