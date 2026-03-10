from src.graph.state import GraphState
from src.nodes.base_node import BaseNode
from src.services.semantic_extraction.runner import run_semantic_extraction

class ExtractFinancesNode(BaseNode):
    @classmethod
    def execute(cls, state: GraphState) -> dict:
        licitacion_id = state.get("licitacion_id")
        documento_ids = state.get("documento_ids", [])
        
        if not licitacion_id:
            print(f"⚠️ [ExtractFinancesNode] No licitacion_id provided in state.")
            return {}

        print(f"💰 [ExtractFinancesNode] Ejecutando extracción Financiera para licitacion_id={licitacion_id}")
        
        import re
        internal_doc_prefixes = []
        for raw in documento_ids:
            match = re.match(r"^(\d+_\d+)", str(raw))
            if match:
                internal_doc_prefixes.append(match.group(1))
            else:
                internal_doc_prefixes.append(raw)
                
        try:
            result = run_semantic_extraction(
                licitacion_id=licitacion_id,
                concepto="FINANZAS_LICITACION",
                documento_ids=internal_doc_prefixes if internal_doc_prefixes else documento_ids,
                nombre_licitacion=f"lic_{licitacion_id}",
                top_k=20,
                min_score=0.3
            )
            
            print(f"✅ [ExtractFinancesNode] Extracción fianciera finalizada.")
            return {"extraction_finances": result}

        except Exception as e:
            print(f"❌ [ExtractFinancesNode] Error en extracción: {e}")
            return {"errors": [f"Finances: {str(e)}"]}
