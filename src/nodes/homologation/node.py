import os
import psycopg2
from src.graph.state import GraphState
from src.nodes.base_node import BaseNode
from src.services.homologacion.homologacion_service import ejecutar_homologacion_automatica

class HomologationNode(BaseNode):
    @classmethod
    def execute(cls, state: GraphState) -> GraphState:
        print(f"🔄 [HomologationNode] Iniciando homologación de la licitación {state['licitacion_id']}...")
        
        # Obtener conexion a base de datos
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            print("⚠️ [HomologationNode] DATABASE_URL no está definido. Omitiendo homologación.")
            state["errors"].append("DATABASE_URL no definida en el nodo de homologacion")
            return state

        try:
            conn = psycopg2.connect(db_url)
        except Exception as e:
            print(f"⚠️ [HomologationNode] Error conectando a DB: {e}")
            state["errors"].append(f"DB Error (Homologacion): {str(e)}")
            return state

        try:
            import uuid
            licitacion_uuid = uuid.UUID(state["licitacion_id"])
            
            resultado = ejecutar_homologacion_automatica(
                licitacion_id=licitacion_uuid,
                conn=conn,
                modelo="gpt-4o"
            )
            
            if resultado:
                state["homologation_result"] = resultado
                
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"⚠️ [HomologationNode] Error en proceso de homologación: {e}")
            state["errors"].append(f"Error homologacion: {str(e)}")
        finally:
            conn.close()

        # Update step to represent completion of homologation
        state["current_step"] = "homologation_completed"
        return state
