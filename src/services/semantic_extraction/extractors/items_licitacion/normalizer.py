
from typing import Any, Dict, List
from datetime import datetime
import unicodedata
import re


def _normalize_text(text: str) -> str:
    """
    Normaliza texto para llaves internas:
    - minúsculas
    - sin tildes
    - sin caracteres especiales
    """
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9_ ]+", "", text)
    text = re.sub(r"\s+", "_", text).strip("_")
    return text


def _build_fuente_resumen(fuentes: List[Dict[str, Any]]) -> str | None:
    """
    Genera un resumen corto de fuentes tipo:
    'p.12; p.15; p.18'
    """
    paginas = []
    for f in fuentes:
        p = f.get("pagina")
        if isinstance(p, int):
            paginas.append(p)

    if not paginas:
        return None

    paginas = sorted(set(paginas))
    return "; ".join(f"p.{p}" for p in paginas)


def _detect_embedded_items(texto: str) -> bool:
    """
    Detecta numeraciones tipo 1.-, 2.- dentro de un texto.
    """
    if not texto or not isinstance(texto, str):
        return False
    return len(re.findall(r"\b\d{1,2}\.\-?", texto)) >= 2


def normalize_items_licitacion(
    parsed_output: Dict[str, Any],
    *,
    licitacion_id: str,
    semantic_run_id: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convierte el JSON del extractor ITEMS_LICITACION en
    estructuras listas para persistir en BD.

    IMPORTANTE:
    - NO genera UUIDs
    - Usa item_key como referencia lógica
    - La resolución a item_id (UUID real) se hace en el service
    """

    items_rows: List[Dict[str, Any]] = []
    specs_rows: List[Dict[str, Any]] = []
    item_evidences: List[Dict[str, Any]] = []

    now = datetime.utcnow()

    items = parsed_output.get("items", [])

    if len(items) == 1:
        desc = items[0].get("descripcion", "") or ""
        if _detect_embedded_items(desc):
            parsed_output.setdefault("warnings", []).append(
                "⚠️ Se detectaron posibles ítems embebidos dentro de una sola descripción."
            )

    for item in items:
        item_key = item.get("item_key") or _normalize_text(item.get("nombre_item", ""))

        nombre = item.get("nombre_item")
        cantidad = item.get("cantidad")
        unidad = item.get("unidad")
        descripcion = item.get("descripcion") or ""
        especificaciones = item.get("especificaciones", [])

        motivos: List[str] = []
        if not nombre or not nombre.strip():
            motivos.append("nombre ausente")
        if not unidad or not isinstance(unidad, str):
            motivos.append("unidad ausente")
        if cantidad in [None, 0]:
            motivos.append("cantidad vacía")

        tiene_descripcion_tecnica = bool(descripcion.strip()) and len(especificaciones) > 0

        item_row = {
            "licitacion_id": licitacion_id,
            "semantic_run_id": semantic_run_id,
            "item_key": item_key,
            "nombre_item": nombre,
            "cantidad": cantidad,
            "unidad": unidad,
            "descripcion": descripcion,
            "observaciones": item.get("notas"),
            "fuente_resumen": _build_fuente_resumen(item.get("fuentes", [])),
            "created_at": now,
            "incompleto": len(motivos) > 0,
            "incompleto_motivos": motivos if motivos else None,
            "tiene_descripcion_tecnica": tiene_descripcion_tecnica,
        }

        items_rows.append(item_row)

        for spec in especificaciones:
            specs_rows.append({
                "item_key": item_key,   # 👈 clave lógica, NO UUID
                "especificacion": spec,
                "created_at": now,
            })

        for fuente in item.get("fuentes", []):
            if fuente.get("redis_key"):
                item_evidences.append({
                    "item_key": item_key,
                    "redis_key": fuente.get("redis_key"),
                    "documento_id": fuente.get("documento_id"),
                    "pagina": fuente.get("pagina"),
                    "created_at": now,
                })

    return {
        "items": items_rows,
        "item_especificaciones": specs_rows,
        "item_evidences": item_evidences,
    }
