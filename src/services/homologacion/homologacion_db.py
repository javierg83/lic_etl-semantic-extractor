"""
Modulo de persistencia para homologacion de productos.

Este modulo contiene las funciones para insertar y consultar
datos de homologacion en la base de datos.

IMPORTANTE: Todas las funciones reciben la conexion (conn) como parametro.
NO se crean conexiones internamente.

Tablas utilizadas:
- homologaciones_productos: Registro principal de homologacion por item
- candidatos_homologacion: Candidatos de productos para cada homologacion
"""
import psycopg2
from typing import Optional
from datetime import datetime


def insertar_homologacion_producto(
    conn,
    homologacion_id: str,
    licitacion_id: str,
    item_key: str,
    descripcion_detectada: str,
    razonamiento_general: Optional[str],
    tokens_input: int,
    tokens_output: int,
    tokens_total: int,
    modelo_usado: str,
    fecha_homologacion: datetime
) -> None:
    """
    Inserta un registro de homologacion de producto en la tabla homologaciones_productos.
    """
    print(f"[HOMOLOGACION_DB] Insertando homologacion | lid={licitacion_id} | item={item_key}")

    sql = """
        INSERT INTO homologaciones_productos (
            id,
            licitacion_id,
            item_key,
            descripcion_detectada,
            razonamiento_general,
            input_tokens,
            output_tokens,
            modelo_usado,
            fecha_homologacion
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        # Note: mapping tokens_input to input_tokens, tokens_output to output_tokens based on db schema
        cur.execute(sql, (
            homologacion_id,
            licitacion_id,
            item_key,
            descripcion_detectada,
            razonamiento_general,
            tokens_input,
            tokens_output,
            modelo_usado,
            fecha_homologacion
        ))
    print(f"[HOMOLOGACION_DB] Homologacion insertada OK | item_key={item_key}")


def insertar_candidato_homologacion(
    conn,
    homologacion_id: str,
    ranking: int,
    producto_codigo: str,
    producto_nombre: str,
    producto_descripcion: Optional[str],
    stock_disponible: Optional[int],
    ubicacion_stock: Optional[str],
    score_similitud: float,
    razonamiento: Optional[str]
) -> None:
    """
    Inserta un candidato de homologacion en la tabla candidatos_homologacion.
    """
    print(f"[HOMOLOGACION_DB] Insertando candidato | hid={homologacion_id} | rank={ranking} | cod={producto_codigo}")

    sql = """
        INSERT INTO candidatos_homologacion (
            homologacion_id,
            ranking,
            producto_codigo,
            producto_nombre,
            producto_descripcion,
            stock_disponible,
            ubicacion_stock,
            score_similitud,
            razonamiento
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            homologacion_id,
            ranking,
            producto_codigo,
            producto_nombre,
            producto_descripcion,
            stock_disponible,
            ubicacion_stock,
            score_similitud,
            razonamiento
        ))
    print(f"[HOMOLOGACION_DB] Candidato insertado OK | codigo={producto_codigo}")
