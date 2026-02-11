from src.graph.state import GraphState
from src.nodes.base_node import BaseNode

class ExtractItemsNode(BaseNode):
    @classmethod
    def execute(cls, state: GraphState) -> GraphState:
        print(f"📦 [ExtractItemsNode] Extrayendo ítems...")
        
        # Simulación de extracción
        state["extraction_items"] = [
            {"item": "Laptop", "cantidad": 10},
            {"item": "Mouse", "cantidad": 10}
        ]
        
        return state
