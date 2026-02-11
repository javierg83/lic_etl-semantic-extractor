from src.graph.state import GraphState
from src.nodes.base_node import BaseNode

class ExtractFinancesNode(BaseNode):
    @classmethod
    def execute(cls, state: GraphState) -> GraphState:
        print(f"💰 [ExtractFinancesNode] Extrayendo información financiera...")
        
        # Simulación de extracción
        state["extraction_finances"] = {"presupuesto": 1000000, "moneda": "CLP"}
        
        return state
