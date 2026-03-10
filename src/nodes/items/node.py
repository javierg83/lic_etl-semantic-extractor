from src.graph.state import GraphState
from src.nodes.base_node import BaseNode
from src.graph.items_subgraph import build_items_subgraph
from src.services.semantic_extraction.runner import _get_pg_conn

class ExtractItemsNode(BaseNode):
    @classmethod
    def execute(cls, state: GraphState) -> dict: # Returns dict update
        licitacion_id = state.get("licitacion_id")
        documento_ids = state.get("documento_ids", [])
        
        if not licitacion_id:
            print(f"⚠️ [ExtractItemsNode] No licitacion_id provided in state.")
            return {}

        print(f"📦 [ExtractItemsNode] Ejecutando SubGrafo de Ítems Híbrido para licitacion_id={licitacion_id}")
        
        # Resolviendo IDs internos para Redis
        # Redis guarda claves como doc_raw_page:{lic_int}_{file_int}... 
        internal_doc_prefixes = []
        try:
            conn = _get_pg_conn()
            cur = conn.cursor()
            
            # Buscar el lic_int basado en licitacion_id (codigo_licitacion/uuid)
            cur.execute("SELECT id, id_interno FROM licitaciones WHERE id::text = %s OR codigo_licitacion = %s", (licitacion_id, licitacion_id))
            lic_row = cur.fetchone()
            lic_uuid = str(lic_row[0]) if lic_row else licitacion_id
            lic_int_id = lic_row[1] if lic_row else None
            print(f"   => lic_int_id (id_interno): {lic_int_id}, lic_uuid: {lic_uuid}")
            
            if lic_int_id:
                # Buscar los file_int basados en los UUIDs de documento_ids
                if documento_ids:
                    # Separar los que parecen UUIDs de los que ya son sufijos de Redis (ej. '84_126_3724-9.pdf')
                    valid_uuids = [d for d in documento_ids if len(str(d)) == 36 and '-' in str(d)]
                    raw_prefixes = [d for d in documento_ids if d not in valid_uuids]
                    
                    if valid_uuids:
                        placeholders = ', '.join(['%s'] * len(valid_uuids))
                        cur.execute(f"SELECT id_interno FROM licitacion_archivos WHERE id::text IN ({placeholders})", tuple(valid_uuids))
                        doc_rows = cur.fetchall()
                        print(f"   => doc_rows found (id_interno): {doc_rows}")
                        for doc_row in doc_rows:
                            doc_int_id = doc_row[0]
                            internal_doc_prefixes.append(f"{lic_int_id}_{doc_int_id}")
                    
                    # Añadir directamente los prefijos crudos (que provienen puramente de la cola del Document Worker)
                    import re
                    for raw in raw_prefixes:
                        # Extraer patron "XX_YY" del inicio (ej: "84_126_3724-9-COT26.pdf" -> "84_126")
                        match = re.match(r"^(\d+_\d+)", str(raw))
                        if match:
                            internal_doc_prefixes.append(match.group(1))
                        else:
                            internal_doc_prefixes.append(raw)
                else:
                    # Si no hay doc UUIDs especificos, pasamos el prefijo de la licitacion
                    internal_doc_prefixes.append(f"{lic_int_id}")
            
            conn.close()
        except Exception as e:
            print(f"⚠️ [ExtractItemsNode] No se pudo obtener lic_int_id / doc_int_id: {e}")
            internal_doc_prefixes = documento_ids # Fallback al id original
        
        print(f"   => internal_doc_prefixes resolved to: {internal_doc_prefixes}")
        
        try:
            subgraph = build_items_subgraph()
            
            # Inicializar estado inicial del subgrafo
            initial_state = {
                "licitacion_id": lic_uuid if 'lic_uuid' in locals() else licitacion_id,
                "documento_ids": internal_doc_prefixes if internal_doc_prefixes else documento_ids,
                "semantic_chunks": [],
                "pre_extracted_items": [],
                "final_items_result": {},
                "errors": []
            }
            
            # Ejecutar el subgrafo
            final_substate = subgraph.invoke(initial_state)
            
            print(f"✅ [ExtractItemsNode] SubGrafo de extracción finalizado.")
            return {"extraction_items": final_substate.get("final_items_result", {})}
            
        except Exception as e:
            print(f"❌ [ExtractItemsNode] Error en SubGrafo: {e}")
            import traceback
            traceback.print_exc()
            return {"errors": [f"Items SubGraph: {str(e)}"]}

