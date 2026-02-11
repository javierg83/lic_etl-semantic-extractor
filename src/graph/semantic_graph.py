from typing import Literal
from langgraph.graph import StateGraph, END
from src.graph.state import GraphState

# Importación de nodos
from src.nodes.load_data.node import LoadDataNode
from src.nodes.finances.node import ExtractFinancesNode
from src.nodes.items.node import ExtractItemsNode
from src.nodes.save.node import SaveNode

# Wrappers de ejecución
def node_load_data(state: GraphState) -> GraphState: return LoadDataNode.execute(state)
def node_extract_finances(state: GraphState) -> GraphState: return ExtractFinancesNode.execute(state)
def node_extract_items(state: GraphState) -> GraphState: return ExtractItemsNode.execute(state)
def node_save(state: GraphState) -> GraphState: return SaveNode.execute(state)

def router_extraction(state: GraphState) -> list:
    """
    Router que decide qué extracciones ejecutar.
    En este caso, ejecutamos ambas en paralelo (finances e items).
    LangGraph permite retornar una lista de nodos a ejecutar en paralelo desde un nodo anterior o ConditionalEdge.
    """
    return ["extract_finances", "extract_items"]

def build_semantic_graph():
    workflow = StateGraph(GraphState)

    # Añadir nodos
    workflow.add_node("load_data", node_load_data)
    workflow.add_node("extract_finances", node_extract_finances)
    workflow.add_node("extract_items", node_extract_items)
    workflow.add_node("save", node_save)

    # Definir flujo
    workflow.set_entry_point("load_data")

    # Desde load_data, vamos a las extracciones (podría ser paralelo o secuencial)
    # Aquí lo haremos secuencial o paralelo según soporte simple. 
    # Para paralelo real en LangGraph: Desde A -> [B, C]. Y luego B->D y C->D.
    
    workflow.add_edge("load_data", "extract_finances")
    workflow.add_edge("load_data", "extract_items")
    
    # Convergencia hacia Save
    workflow.add_edge("extract_finances", "save")
    workflow.add_edge("extract_items", "save")
    
    workflow.add_edge("save", END)

    return workflow.compile()
