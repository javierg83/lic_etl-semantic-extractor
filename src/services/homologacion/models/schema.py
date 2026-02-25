from typing import List, Optional
from pydantic import BaseModel
from uuid import UUID
from datetime import datetime


class ProductoCatalogo(BaseModel):
    codigo: str
    nombre: str
    descripcion: str
    stock_disponible: int
    ubicacion_stock: str
    codigo_tienda: str


class ProductoHomologado(BaseModel):
    codigo: str
    nombre: str
    descripcion: Optional[str] = None
    stock_disponible: Optional[int] = None
    ubicacion_stock: Optional[str] = None


class CandidatoHomologacion(BaseModel):
    ranking: int
    producto: ProductoHomologado
    score_similitud: float
    razonamiento: str


class ResultadoHomologacion(BaseModel):
    item_key: str
    descripcion_detectada: str
    razonamiento_general: Optional[str] = None
    candidatos: List[CandidatoHomologacion]


class ResumenHomologacionProductos(BaseModel):
    total_items_detectados: int
    total_items_con_match: int
    tokens_input: int
    tokens_output: int
    tokens_total: int
    modelo_usado: str


class RespuestaHomologacionProductos(BaseModel):
    concepto: str = "HOMOLOGACION_PRODUCTOS"
    licitacion_id: UUID
    resumen: ResumenHomologacionProductos
    homologaciones: List[ResultadoHomologacion]
