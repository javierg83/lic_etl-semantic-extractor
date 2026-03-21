import operator
from typing import TypedDict, List, Dict, Any, Optional, Annotated
from langgraph.graph import StateGraph, END

# --- SubGraph State ---
class ItemsSubGraphState(TypedDict):
    licitacion_id: str
    documento_ids: List[str]
    semantic_chunks: List[Dict[str, Any]]
    pre_extracted_items: List[Dict[str, Any]]
    final_items_result: Dict[str, Any]
    errors: Annotated[List[str], operator.add]

# --- Shared Configuration ---
UNIDADES_MAP = {
    "EA": "Unidades",
    "UN": "Unidades",
    "UND": "Unidades",
    "BX": "Cajas",
    "CJ": "Cajas",
    "PAQ": "Paquetes",
    "BG": "Bolsas",
    "VI": "Frascos",
    "AMP": "Ampollas",
    "CMP": "Comprimidos",
    "SET": "Set",
    "KIT": "Kit",
    "PACK": "Pack",
    "GR": "Gramos",
    "KG": "Kilogramos",
    "ML": "Mililitros",
    "LT": "Litros",
    "M": "Metros",
    "M2": "Metros Cuadrados",
    "M3": "Metros Cúbicos",
    "000": "Millar / Adhesivo"
}

# --- Nodes ---
def node_semantic_locator(state: ItemsSubGraphState) -> ItemsSubGraphState:
    print(f" [ItemsSubGraph] Ejecutando SemanticLocatorNode...")
    from src.services.semantic_extraction.runner import (
        load_documents_to_memory,
        semantic_search_in_memory
    )
    from src.services.semantic_extraction.registry import get_extractor
    
    licitacion_id = state.get("licitacion_id")
    documento_ids = state.get("documento_ids", [])
    
    extractor_cls = get_extractor("ITEMS_LICITACION")
    extractor = extractor_cls(licitacion_id=licitacion_id)
    queries = extractor._call_build_queries()
    
    cached_chunks = load_documents_to_memory(documento_ids)
    semantic_chunks = []
    
    if cached_chunks:
        for query in queries:
            filtros = semantic_search_in_memory(query, cached_chunks, top_k=1000, min_score=0.25)
            semantic_chunks.extend(filtros)
            
    # Deduplicar
    unique_chunks_dict = {}
    for c in semantic_chunks:
        k = c["redis_key"]
        if k not in unique_chunks_dict or c.get("distancia", 999.0) < unique_chunks_dict[k].get("distancia", 999.0):
            unique_chunks_dict[k] = c
            
    all_chunks = list(unique_chunks_dict.values())
    
    # Ordenamiento lógico (Archivo_ID -> num_pagina) para secuencialidad en lugar de semántico
    import re
    def get_sort_key(c):
        redis_key = c['redis_key']
        file_int = 0
        page_num = 0
        try:
            match = re.search(r"doc_raw_page:\d+_(\d+)_.+:p(\d+)", redis_key)
            if match:
                file_int = int(match.group(1))
                page_num = int(match.group(2))
            else:
                match_legacy = re.search(r"pdf:([^:]+):chunk:(\d+)", redis_key)
                if match_legacy:
                    file_int = 0
                    page_num = int(match_legacy.group(2))
        except Exception:
            pass
        return (file_int, page_num)
        
    all_chunks.sort(key=get_sort_key)
    print(f"   -- Total chunks unicos ordenados logicamente: {len(all_chunks)}")
    
    return {"semantic_chunks": all_chunks}

def node_format_parser(state: ItemsSubGraphState) -> ItemsSubGraphState:
    print(f" [ItemsSubGraph] Ejecutando FormatParserRouterNode...")
    semantic_chunks = state.get("semantic_chunks", [])
    
    if not semantic_chunks:
        print(f"   -- No hay chunks semanticos. Saltando parser.")
        return {"pre_extracted_items": []}
        
    import json
    
    # 1. Intentar extracción prioritaria desde JSON (Ficha Compra Agil)
    items_json = []
    json_found = False
    
    for chunk in semantic_chunks:
        redis_key = chunk.get("redis_key", "")
        if ".json" in redis_key.lower():
            try:
                print(f"   -- Encontrado chunk de JSON: {redis_key}")
                data = json.loads(chunk["texto"])
                
                payload = data.get("payload") if isinstance(data, dict) else None
                productos = []
                if payload and isinstance(payload, dict):
                    productos = payload.get("productos_solicitados", [])
                elif isinstance(data, list):
                    productos = data 
                elif isinstance(data, dict) and "productos_solicitados" in data:
                    productos = data["productos_solicitados"]
                
                if productos:
                    json_found = True
                    for i, p in enumerate(productos):
                        u_code = p.get("unidad_medida") or p.get("unidad") or "N/A"
                        u_text = UNIDADES_MAP.get(u_code, u_code)

                        item_base = {
                            "item_key": f"json_{i+1}",
                            "nombre_item": p.get("nombre") or p.get("nombre_producto"),
                            "cantidad": p.get("cantidad"),
                            "unidad": u_text,
                            "descripcion": p.get("descripcion"),
                            "codigo_producto": p.get("codigo_producto"),
                            "fuente_resumen": "JSON Oficial (Mercado Público)"
                        }
                        items_json.append(item_base)
                    
                    print(f"   -- ✅ Extraídos {len(items_json)} ítems literales del JSON.")
                    break
            except Exception as e:
                print(f"   -- ⚠️ Error parseando JSON de chunk {redis_key}: {e}")

    if json_found and items_json:
        return {"pre_extracted_items": items_json}
        
    # 3. Fallback a parser determinístico
    from src.services.semantic_extraction.extractors.items_licitacion.items_licitacion_stateful_parser import ItemsLicitacionStatefulParser
    
    parser = ItemsLicitacionStatefulParser()
    texto_completo = "\n".join([c["texto"] for c in semantic_chunks])
    
    items_extraidos = parser.parsear_texto(texto_completo)
    print(f"   -- Parser deterministico encontro {len(items_extraidos)} items base.")
    
    return {"pre_extracted_items": items_extraidos}

def node_llm_verification(state: ItemsSubGraphState) -> ItemsSubGraphState:
    print(f" [ItemsSubGraph] Ejecutando LLMVerificationNode...")
    from src.services.semantic_extraction.runner import build_context
    from src.services.semantic_extraction.registry import get_extractor
    import json
    
    licitacion_id = state.get("licitacion_id")
    semantic_chunks = state.get("semantic_chunks", [])
    pre_extracted_items = state.get("pre_extracted_items", [])
    
    extractor_cls = get_extractor("ITEMS_LICITACION")
    extractor = extractor_cls(licitacion_id=licitacion_id)
    
    context = build_context(semantic_chunks)
    
    if pre_extracted_items:
        context += f"\n\n==============================\n<ITEMS_PRE_EXTRAIDOS>\n"
        context += json.dumps(pre_extracted_items, indent=2, ensure_ascii=False)
        context += f"\n</ITEMS_PRE_EXTRAIDOS>\n==============================\n"
        
    try:
        result = extractor.run(context)
        print(f"   -- LLM devolvio {len(result.get('items', []))} items consolidados.")
        
        from src.services.semantic_extraction.runner import _guardar_json_en_disco, _get_pg_conn, MODO_DEBUG
        from src.services.licitacion_service import guardar_items_licitacion, guardar_especificaciones_tecnicas
        
        try:
            nombre_licitacion = f"lic_{licitacion_id}"
            _guardar_json_en_disco(nombre_licitacion, "ITEMS_LICITACION", result)
            
            if not MODO_DEBUG:
                conn = _get_pg_conn()
                cur = conn.cursor()
                try:
                    cur.execute("UPDATE semantic_runs SET is_current = false WHERE licitacion_id = %s AND concepto = %s AND is_current = true", (licitacion_id, "ITEMS_LICITACION"))
                    cur.execute("INSERT INTO semantic_runs (licitacion_id, concepto, is_current) VALUES (%s, %s, true) RETURNING id", (licitacion_id, "ITEMS_LICITACION"))
                    run_id = cur.fetchone()[0]
                    
                    cur.execute("INSERT INTO semantic_results (semantic_run_id, concepto, resultado_json) VALUES (%s, %s, %s)", (run_id, "ITEMS_LICITACION", json.dumps(result)))
                    
                    for c in semantic_chunks:
                        cur.execute("INSERT INTO semantic_evidences (semantic_run_id, redis_key, texto_fragmento, score_similitud) VALUES (%s, %s, %s, %s)", (run_id, c["redis_key"], c["texto"], c.get("distancia")))
                    
                    if "items" in result and result["items"]:
                        guardar_items_licitacion(conn, licitacion_id, str(run_id), result["items"])
                        guardar_especificaciones_tecnicas(conn, str(run_id), result.get("especificaciones_tecnicas", []))

                    conn.commit()
                    result["semantic_run_id"] = str(run_id)
                except Exception as e:
                    conn.rollback()
                    print(f" Error DB Persistencia: {e}")
                finally:
                    cur.close()
                    conn.close()
        except Exception as file_e:
            print(f" Error guardando JSON: {file_e}")
            
        return {"final_items_result": result}
    except Exception as e:
        print(f"   -- Error en LLM: {e}")
        return {"errors": [f"LLM Error: {str(e)}"]}

# --- Build SubGraph ---
def build_items_subgraph():
    workflow = StateGraph(ItemsSubGraphState)
    
    workflow.add_node("semantic_locator", node_semantic_locator)
    workflow.add_node("format_parser", node_format_parser)
    workflow.add_node("llm_verification", node_llm_verification)
    
    workflow.set_entry_point("semantic_locator")
    
    workflow.add_edge("semantic_locator", "format_parser")
    workflow.add_edge("format_parser", "llm_verification")
    workflow.add_edge("llm_verification", END)
    
    return workflow.compile()
