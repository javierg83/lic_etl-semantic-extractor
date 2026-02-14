from src.graph.state import GraphState
from src.nodes.base_node import BaseNode

class LoadDataNode(BaseNode):
    @classmethod
    def execute(cls, state: GraphState) -> dict:
        licitacion_id = state.get("licitacion_id")
        print(f"📥 [LoadDataNode] Iniciando flujo semántico para licitación: {licitacion_id}")
        
        doc_ids = state.get("documento_ids", [])
        if not doc_ids:
            print(f"⚠️ [LoadDataNode] No se encontraron 'documento_ids' en el estado inicial.")
        else:
            print(f"📄 [LoadDataNode] IDs de documentos a procesar: {len(doc_ids)}")

        # Clean errors if any from previous runs (though this is new run)
        # Return updates
        return {
            "current_step": "load_data",
            "status": "processing"
        }
