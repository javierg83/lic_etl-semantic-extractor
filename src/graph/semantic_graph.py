from typing import Literal
from langgraph.graph import StateGraph, END
from src.graph.state import GraphState

# Importación de nodos
from src.nodes.load_data.node import LoadDataNode
from src.nodes.finances.node import ExtractFinancesNode
from src.nodes.items.node import ExtractItemsNode
from src.nodes.basic_data.node import ExtractBasicDataNode
from src.nodes.save.node import SaveNode

# Wrappers de ejecución
def node_load_data(state: GraphState) -> GraphState: return LoadDataNode.execute(state)
def node_extract_finances(state: GraphState) -> GraphState: return ExtractFinancesNode.execute(state)
def node_extract_items(state: GraphState) -> GraphState: return ExtractItemsNode.execute(state)
def node_extract_basic_data(state: GraphState) -> GraphState: return ExtractBasicDataNode.execute(state)
def node_save(state: GraphState) -> GraphState: return SaveNode.execute(state)

def build_semantic_graph():
    workflow = StateGraph(GraphState)

    # Añadir nodos
    workflow.add_node("load_data", node_load_data)
    workflow.add_node("extract_finances", node_extract_finances)
    workflow.add_node("extract_items", node_extract_items)
    workflow.add_node("extract_basic_data", node_extract_basic_data)
    workflow.add_node("save", node_save)

    # Definir flujo
    workflow.set_entry_point("load_data")

    # Desde load_data, "fan-out" a las tres extracciones
    workflow.add_edge("load_data", "extract_finances")
    workflow.add_edge("load_data", "extract_items")
    workflow.add_edge("load_data", "extract_basic_data")
    
    # Convergencia ("fan-in") hacia Save
    # LangGraph espera que todas las ramas terminen antes de ir a un nodo común si fluyen hacia él?
    # En versiones recientes, simplemente se ejecutan.
    workflow.add_edge("extract_finances", "save")
    workflow.add_edge("extract_items", "save")
    workflow.add_edge("extract_basic_data", "save")
    
    workflow.add_edge("save", END)

    return workflow.compile()
