import sys
import os
import argparse
from src.services.embedding_service import run_embedding_batch
from src.main import main as run_graph

def main():
    parser = argparse.ArgumentParser(description="Ejecutar Pipeline de Extracción Semántica")
    parser.add_argument("doc_id", help="ID del documento principal (nombre carpeta en archivos_texto)")
    parser.add_argument("--lic_id", help="ID de la licitación (opcional, default=doc_id)", default=None)
    parser.add_argument("--skip-embedding", action="store_true", help="Saltar generación de embeddings si ya existen")
    
    args = parser.parse_args()
    
    doc_id = args.doc_id
    lic_id = args.lic_id or doc_id  # Si no se da lic_id, usar doc_id
    
    print(f"🚀 [PIPELINE] Iniciando proceso para Documento: {doc_id} | Licitación: {lic_id}")
    
    # 1. Generar Embeddings (si no se salta)
    if not args.skip_embedding:
        print("\n--- 1. GENERANDO EMBEDDINGS ---")
        try:
            run_embedding_batch(doc_id)
        except Exception as e:
            print(f"❌ Error en embeddings: {e}")
            sys.exit(1)
    else:
        print("\n--- 1. EMBEDDINGS SKIPPED ---")

    # 2. Ejecutar Grafo de Extracción
    print("\n--- 2. EJECUTANDO EXTRACTION GRAPH ---")
    try:
        # Hack para pasar argumentos a main.py que usa sys.argv
        # Restaurar sys.argv original al final si fuera necesario, pero aquí el script termina
        sys.argv = ["main.py", lic_id, doc_id]
        run_graph()
    except Exception as e:
        print(f"❌ Error en grafo: {e}")
        sys.exit(1)

    print("\n✅ [PIPELINE] Proceso completo.")

if __name__ == "__main__":
    main()
