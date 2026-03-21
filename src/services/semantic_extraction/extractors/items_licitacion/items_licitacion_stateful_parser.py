import re
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class ItemsLicitacionStatefulParser:
    """
    Parser determinístico (stateful) para extraer ítems de licitaciones (ej. Compra Ágil)
    basado en un flujo secuencial a través de las páginas.
    """
    
    def __init__(self):
        self.items: List[Dict[str, Any]] = []
        self.item_actual: Optional[Dict[str, Any]] = None
        self.en_seccion_relevante = False
        
    def reset(self):
        self.items = []
        self.item_actual = None
        self.en_seccion_relevante = False

    def _crear_item_vacio(self) -> Dict[str, Any]:
        return {
            "item_key": None,
            "nombre_item": None,
            "cantidad": None,
            "unidad": None,
            "descripcion": None,
            "especificaciones": [],
            "criterios_cumplimiento": [],
            "exclusiones_o_prohibiciones": [],
            "razonamiento": "Extraído automáticamente mediante parser determinístico (Stateful)",
            "fuentes": [],
            "confianza_item": 0.9,
            "notas": "Pre-extraído por patrón visual"
        }

    def _limpiar_linea(self, linea: str) -> str:
        return linea.strip()

    def _aplanar_json_texto(self, texto: str) -> str:
        # Intenta decodificar si el texto es un JSON array serializado (ej. tabla OCR)
        # y lo aplana preservando los saltos de línea físicos.
        import json
        try:
            data = json.loads(texto)
            if isinstance(data, list):
                lineas = []
                for row in data:
                    if isinstance(row, list):
                        for cell in row:
                            lineas.extend(str(cell).split('\n'))
                    else:
                        lineas.extend(str(row).split('\n'))
                return "\n".join(lineas)
        except Exception:
            pass
        
        # Eliminar las comillas dobles externas y desescapar si es un json simple truncado
        # a veces el OCR retorna '["foo\\nbar"]'
        texto_limpio = texto.replace('\\n', '\n')
        return texto_limpio

    def parsear_texto(self, texto: str, documento_nombre: str = "Documento", pagina_num: int = 1) -> List[Dict[str, Any]]:
        """
        Recibe texto concatenado (idealmente página por página) y extrae ítems usando buffer de estado.
        """
        texto_procesado = self._aplanar_json_texto(texto)
        lineas = texto_procesado.split('\n')
        
        # Regex utiles
        regex_id = re.compile(r"^ID:\s*(\d+)")
        regex_cantidad = re.compile(r"^(\d+(?:[.,]\d+)?)\s+([A-Za-z]+.*)$", re.IGNORECASE)
        
        for linea_raw in lineas:
            linea = self._limpiar_linea(linea_raw)
            if not linea:
                continue

            # Detectar inicio de sección útil (Específico para formatos conocidos como Compra Ágil)
            headers_items = ["Listado de productos solicitados", "Productos Solicitados", "Descripción de los bienes", "Ítems de la licitación"]
            if any(h in linea for h in headers_items):
                self.en_seccion_relevante = True
                continue

            match_id = regex_id.search(linea)

            if not self.en_seccion_relevante:
                # Fallback: Si vemos un ID claro, forzamos la entrada a la sección relevante
                # ya que en búsquedas semánticas el chunk del título podría haberse omitido.
                if match_id:
                    self.en_seccion_relevante = True
                else:
                    continue

            if self.item_actual is None:
                self.item_actual = self._crear_item_vacio()

            # Lógica heurística:
            # 1. ¿Es una línea de ID?
            match_id = regex_id.search(linea)
            if match_id:
                self.item_actual["item_key"] = f"item_{match_id.group(1)}"
                continue

            # 2. ¿Es cantidad y unidad? (marca el cierre del ítem actual)
            match_cant = regex_cantidad.search(linea)
            # Evitar matches falsos si la línea es muy larga (Suele ser una descripción)
            if match_cant and len(linea) < 30 and self.item_actual["item_key"] is not None:
                self.item_actual["cantidad"] = float(match_cant.group(1).replace(',', '.'))
                self.item_actual["unidad"] = match_cant.group(2).strip()
                
                # Guardar el origen
                self.item_actual["fuentes"].append({
                    "documento": documento_nombre,
                    "pagina": pagina_num,
                    "parrafo": f"ID={self.item_actual['item_key']} | Desc={self.item_actual['nombre_item']} | Cant={linea}",
                    "redis_key": "N/A" # Lo llenará el LLM o lo ignoramos
                })
                
                # Cerrar ítem
                self.items.append(self.item_actual)
                self.item_actual = None
                continue

            # 3. Categoría (precede al ID)
            if self.item_actual.get("item_key") is None:
                # Si no tenemos ID aún, asumimos que estamos en la Categoría
                # Lo guardaremos temporalmente en notas o desc 
                if not self.item_actual.get("notas"):
                    self.item_actual["notas"] = f"Categoría: {linea}"
                continue

            # 4. Descripción (entre el ID y la Cantidad)
            if self.item_actual.get("item_key") is not None and self.item_actual.get("cantidad") is None:
                if self.item_actual.get("nombre_item") is None:
                    self.item_actual["nombre_item"] = linea
                else:
                    if self.item_actual["descripcion"] is None:
                        self.item_actual["descripcion"] = linea
                    else:
                        self.item_actual["descripcion"] += f" {linea}"
                continue

        # Si un ítem quedó abierto, se mantiene en self.item_actual
        # Al pasarle el texto de la siguiente página, continuará completándolo
        
        return self.items

    def obtener_items_cerrados(self) -> List[Dict[str, Any]]:
        return self.items
