import sys
from src.graph.semantic_graph import build_semantic_graph
from src.graph.state import GraphState

def main():
    print("🚀 Iniciando extracción semántica (Local Test)...")
    
    # Simple CLI argument parsing
    lic_id = "TEST_123"
    doc_ids = []

    if len(sys.argv) > 1:
        lic_id = sys.argv[1]
    
    if len(sys.argv) > 2:
        doc_ids = sys.argv[2].split(",")
    else:
        # Default fallback only if not provided
        doc_ids = ["doc_1"]

    print(f"📋 Parametros: licitacion_id={lic_id}, doc_ids={doc_ids}")
    
    app = build_semantic_graph()
    
    initial_state = GraphState(
        licitacion_id=lic_id,
        documento_ids=doc_ids,
        document_text=None,
        extraction_finances=None,
        extraction_items=None,
        extraction_basic_data=None,
        status="init",
        errors=[],
        current_step="init"
    )
    
    print("▶️ Ejecutando grafo...")
    result = app.invoke(initial_state)
    
    print("\n✅ Proceso Finalizado.")
    print(f"Estado Final: {result.get('status')}")
    print(f"Errores: {result.get('errors')}")
    
    if result.get("extraction_items"):
        print(f"📦 Items extraidos: {len(result['extraction_items'].get('resultado_json', {}).get('items', []))}")
    
    if result.get("extraction_finances"):
        print(f"💰 Finanzas extraidas: {result['extraction_finances'].get('resultado_json', {}).get('finanzas')}")

    if result.get("extraction_basic_data"):
        print(f"📋 Datos básicos extraidos: {result['extraction_basic_data'].get('resultado_json', {}).get('datos_basicos')}")

if __name__ == "__main__":
    main()
