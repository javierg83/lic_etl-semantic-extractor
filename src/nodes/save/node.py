from src.graph.state import GraphState
from src.nodes.base_node import BaseNode

class SaveNode(BaseNode):
    @classmethod
    def execute(cls, state: GraphState) -> GraphState:
        print(f"💾 [SaveNode] Guardando resultados...")
        
        # Simulación de guardado
        print(f"   -> Finanzas guardadas: {state.get('extraction_finances')}")
        
        items = state.get('extraction_items') or []
        print(f"   -> Ítems guardados: {len(items)} items")
        
        state["status"] = "completed"
        return state
