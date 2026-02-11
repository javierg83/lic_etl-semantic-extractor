from src.graph.state import GraphState
from src.nodes.base_node import BaseNode

class LoadDataNode(BaseNode):
    @classmethod
    def execute(cls, state: GraphState) -> GraphState:
        print(f"📥 [LoadDataNode] Cargando datos para licitación: {state['licitacion_id']}")
        
        # Simulación de carga de texto
        state["document_text"] = "Texto simulado del documento de licitación..."
        state["current_step"] = "load_data"
        
        return state
