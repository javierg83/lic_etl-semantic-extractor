from abc import ABC, abstractmethod
from typing import Dict, Any
from src.graph.state import GraphState

class BaseNode(ABC):
    """
    Clase base para todos los nodos del grafo.
    """

    @classmethod
    @abstractmethod
    def execute(cls, state: GraphState) -> GraphState:
        """
        Método principal de ejecución del nodo.
        Debe recibir el estado actual y retornar el nuevo estado.
        """
        pass
