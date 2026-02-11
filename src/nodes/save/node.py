from src.graph.state import GraphState
from src.nodes.base_node import BaseNode

class SaveNode(BaseNode):
    @classmethod
    def execute(cls, state: GraphState) -> GraphState:
        print(f"💾 [SaveNode] Guardando resultados...")
        
        # Simulación de guardado
        print(f"   -> Finanzas guardadas: {state.get('extraction_finances')}")
        print(f"   -> Ítems guardados: {len(state.get('extraction_items', []))} items")
        
        state["status"] = "completed"
        return state
