from typing import Literal
from langgraph.graph import StateGraph, END
from src.graph.state import GraphState

# Importación de nodos
from src.nodes.load_data.node import LoadDataNode
from src.nodes.finances.node import ExtractFinancesNode
from src.nodes.items.node import ExtractItemsNode
from src.nodes.basic_data.node import ExtractBasicDataNode
from src.nodes.entregas.node import ExtractEntregasNode
from src.nodes.save.node import SaveNode
from src.nodes.homologation.node import HomologationNode

# Wrappers de ejecución
def node_load_data(state: GraphState) -> GraphState: return LoadDataNode.execute(state)
def node_extract_finances(state: GraphState) -> GraphState: return ExtractFinancesNode.execute(state)
def node_extract_items(state: GraphState) -> GraphState: return ExtractItemsNode.execute(state)
def node_extract_basic_data(state: GraphState) -> GraphState: return ExtractBasicDataNode.execute(state)
def node_extract_entregas(state: GraphState) -> GraphState: return ExtractEntregasNode.execute(state)
def node_save(state: GraphState) -> GraphState: return SaveNode.execute(state)
def node_homologation(state: GraphState) -> GraphState: return HomologationNode.execute(state)

def build_semantic_graph():
    workflow = StateGraph(GraphState)

    # Añadir nodos
    workflow.add_node("load_data", node_load_data)
    workflow.add_node("extract_finances", node_extract_finances)
    workflow.add_node("extract_items", node_extract_items)
    workflow.add_node("extract_basic_data", node_extract_basic_data)
    workflow.add_node("extract_entregas", node_extract_entregas)
    workflow.add_node("save", node_save)
    workflow.add_node("homologation", node_homologation)

    # Definir flujo
    workflow.set_entry_point("load_data")

    # Desde load_data, "fan-out" a las cuatro extracciones
    workflow.add_edge("load_data", "extract_finances")
    workflow.add_edge("load_data", "extract_items")
    workflow.add_edge("load_data", "extract_basic_data")
    workflow.add_edge("load_data", "extract_entregas")
    
    # Convergencia ("fan-in") hacia Save
    workflow.add_edge("extract_finances", "save")
    workflow.add_edge("extract_items", "save")
    workflow.add_edge("extract_basic_data", "save")
    workflow.add_edge("extract_entregas", "save")
    
    # Después de guardar, ejecutar homologación
    workflow.add_edge("save", "homologation")
    workflow.add_edge("homologation", END)

    return workflow.compile()
