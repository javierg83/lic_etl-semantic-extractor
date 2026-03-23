from typing import Optional
from marshmallow import Schema, fields, validate, ValidationError

class EntregasLicitacionSchema(Schema):
    concepto = fields.String(validate=validate.Equal("ENTREGAS_LICITACION"), required=True)
    licitacion_id = fields.String(required=True)
    
    # Campos de la base de datos (licitacion_entregas)
    direccion_entrega = fields.String(allow_none=True)
    comuna_entrega = fields.String(allow_none=True)
    plazo_entrega = fields.String(allow_none=True)
    fecha_entrega = fields.String(allow_none=True)
    contacto_entrega = fields.String(allow_none=True)
    horario_entrega = fields.String(allow_none=True)
    instrucciones_entrega = fields.String(allow_none=True)
    
    # Manejo de fallos / notas
    notas = fields.String(allow_none=True)
    warnings = fields.List(fields.String(), load_default=list)

def validate_entregas_licitacion_schema(data: dict):
    schema = EntregasLicitacionSchema()
    try:
        return schema.load(data)
    except ValidationError as e:
        raise EntregasLicitacionSchemaError(f"Error validando esquema de Entregas: {e.messages}")

class EntregasLicitacionSchemaError(Exception):
    pass
