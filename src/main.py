from src.graph.semantic_graph import build_semantic_graph
from src.graph.state import GraphState

def main():
    print("🚀 Iniciando extracción semántica (Local Test)...")
    
    app = build_semantic_graph()
    
    initial_state: GraphState = {
        "licitacion_id": "TEST_123",
        "document_text": None,
        "extraction_finances": None,
        "extraction_items": None,
        "status": "init",
        "errors": [],
        "current_step": "init"
    }
    
    result = app.invoke(initial_state)
    
    print("\n✅ Proceso Finalizado.")
    print(f"Estado Final: {result.get('status')}")
    print(f"Errores: {result.get('errors')}")

if __name__ == "__main__":
    main()
