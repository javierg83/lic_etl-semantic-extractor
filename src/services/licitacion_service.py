import psycopg2
import os
import uuid
import json
from datetime import datetime
import traceback

DATABASE_URL = os.getenv("DATABASE_URL")

def get_pg_conn():
    if not DATABASE_URL:
        # Fallback for local testing if env var is not set, or raise error
        # Assuming config.DATABASE_URL might be available, otherwise let it fail if not in env
        raise RuntimeError("DATABASE_URL no configurado en variables de entorno")
    return psycopg2.connect(DATABASE_URL)

# --------------------------------------------------
# FUNCIONES DE CONSULTA
# --------------------------------------------------

def obtener_licitacion_por_id(licitacion_id: str) -> dict | None:
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, codigo_licitacion, nombre, descripcion, estado, organismo_solicitante, fecha_carga, estado_publicacion
            FROM licitaciones
            WHERE id = %s
        """, (str(licitacion_id),))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "codigo_licitacion": row[1],
            "nombre": row[2],
            "descripcion": row[3],
            "estado": row[4],
            "organismo_solicitante": row[5],
            "fecha_carga": row[6].isoformat() if row[6] else None,
            "estado_publicacion": row[7]
        }
    finally:
        cur.close()
        conn.close()

def obtener_todas_las_licitaciones() -> list[dict]:
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, codigo_licitacion, nombre, descripcion, estado, fecha_carga, estado_publicacion
            FROM licitaciones
            ORDER BY fecha_carga DESC
        """)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "codigo_licitacion": r[1],
                "nombre": r[2],
                "descripcion": r[3],
                "estado": r[4],
                "fecha_carga": r[5].isoformat() if r[5] else None,
                "estado_publicacion": r[6]
            }
            for r in rows
        ]
    finally:
        cur.close()
        conn.close()

def obtener_items_por_licitacion(licitacion_id: str) -> list[dict]:
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT nombre_item, cantidad, unidad, descripcion, observaciones,
                   fuente_resumen, incompleto, incompleto_motivos, tiene_descripcion_tecnica
            FROM items_licitacion
            WHERE licitacion_id = %s
            ORDER BY nombre_item
        """, (str(licitacion_id),))
        rows = cur.fetchall()
        return [
            {
                "nombre_item": r[0],
                "cantidad": r[1],
                "unidad": r[2],
                "descripcion": r[3],
                "observaciones": r[4],
                "fuente_resumen": r[5],
                "incompleto": r[6],
                "incompleto_motivos": r[7],
                "tiene_descripcion_tecnica": r[8],
            }
            for r in rows
        ]
    finally:
        cur.close()
        conn.close()


def obtener_items_homologados_con_candidatos(licitacion_id: str) -> list[dict]:
    """
    Obtiene los items homologados con sus candidatos para una licitacion.
    """
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        print(f"[HOMOLOGACION_READ] Consultando items homologados | licitacion_id={licitacion_id}")

        cur.execute("""
            SELECT
                hp.id AS homologacion_id,
                hp.item_key,
                hp.descripcion_detectada,
                il.nombre_item,
                il.cantidad,
                ch.ranking,
                ch.producto_codigo,
                ch.producto_nombre,
                ch.producto_descripcion,
                ch.stock_disponible,
                ch.ubicacion_stock,
                ch.score_similitud,
                ch.razonamiento
            FROM homologaciones_productos hp
            LEFT JOIN items_licitacion il
                ON il.licitacion_id = hp.licitacion_id::text
               AND (
                   LOWER(TRIM(il.item_key)) = LOWER(TRIM(hp.item_key))
                   OR LOWER(TRIM(il.nombre_item)) = LOWER(TRIM(hp.item_key))
               )
            LEFT JOIN candidatos_homologacion ch
                ON ch.homologacion_id = hp.id
            WHERE hp.licitacion_id = %s
            ORDER BY hp.fecha_homologacion DESC, hp.item_key, ch.ranking
        """, (str(licitacion_id),))

        rows = cur.fetchall()
        print(f"[HOMOLOGACION_READ] Filas encontradas en query: {len(rows)}")

        items_dict = {}

        for r in rows:
            homologacion_id = str(r[0])
            item_key = r[1]
            descripcion_detectada = r[2]
            nombre_item = r[3] or item_key
            cantidad = r[4]

            if homologacion_id not in items_dict:
                items_dict[homologacion_id] = {
                    "homologacion_id": homologacion_id,
                    "item_key": item_key,
                    "nombre_item": nombre_item,
                    "cantidad": cantidad,
                    "descripcion_detectada": descripcion_detectada,
                    "candidatos": []
                }

            if r[5] is not None:
                items_dict[homologacion_id]["candidatos"].append({
                    "ranking": r[5],
                    "codigo": r[6],
                    "nombre": r[7],
                    "descripcion": r[8],
                    "stock": r[9],
                    "ubicacion": r[10],
                    "score": float(r[11]) if r[11] is not None else None,
                    "razonamiento": r[12]
                })

        resultados = list(items_dict.values())

        print(f"[HOMOLOGACION_READ] Total items homologados: {len(resultados)}")
        return resultados

    finally:
        cur.close()
        conn.close()

# --------------------------------------------------
# FUNCIONES DE PERSISTENCIA
# --------------------------------------------------

def get_or_create_licitacion(nombre_archivo: str) -> str:
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        # Primero intentamos buscar exacto
        cur.execute("SELECT id FROM licitaciones WHERE nombre = %s", (nombre_archivo,))
        row = cur.fetchone()
        if row:
            return str(row[0])

        # Si no existe, creamos
        nuevo_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO licitaciones (id, nombre, estado, fecha_carga)
            VALUES (%s, %s, 'PENDIENTE', now())
        """, (nuevo_id, nombre_archivo))
        conn.commit()
        return nuevo_id
    finally:
        cur.close()
        conn.close()

def obtener_mapa_uuid_por_interno(licitacion_id: str) -> dict:
    """
    Retorna un diccionario {str(id_interno): str(id_uuid)} para los archivos de una licitación.
    Útil para mapear desde Redis (que usa id_interno) a Postgres (que usa UUID).
    """
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id_interno, id
            FROM licitacion_archivos
            WHERE licitacion_id = %s
        """, (str(licitacion_id),))
        rows = cur.fetchall()
        # Clave: id_interno como string (porque viene así del regex)
        # Valor: id (uuid) como string
        return {str(r[0]): str(r[1]) for r in rows}
    finally:
        cur.close()
        conn.close()

def guardar_items_licitacion(conn, licitacion_id, semantic_run_id, items: list[dict]):
    with conn.cursor() as cur:
        # Ojo: conn viene de fuera, NO cerrarla aquí
        cur.execute("DELETE FROM items_licitacion WHERE semantic_run_id = %s", (semantic_run_id,))
        for item in items:
            cur.execute("""
                INSERT INTO items_licitacion (
                    licitacion_id,
                    semantic_run_id,
                    item_key,
                    nombre_item,
                    cantidad,
                    unidad,
                    descripcion,
                    observaciones,
                    fuente_resumen,
                    created_at,
                    incompleto,
                    incompleto_motivos,
                    tiene_descripcion_tecnica
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                licitacion_id,
                semantic_run_id,
                item.get("item_key"),
                item.get("nombre_item"),
                item.get("cantidad"),
                item.get("unidad"),
                item.get("descripcion"),
                item.get("observaciones"),
                item.get("fuente_resumen"),
                item.get("created_at") or datetime.utcnow(),
                item.get("incompleto") or False,
                # ARRAY handling for incomplete motives
                item.get("incompleto_motivos") if isinstance(item.get("incompleto_motivos"), list) else None,
                item.get("tiene_descripcion_tecnica") or False
            ))
        conn.commit()

def guardar_especificaciones_tecnicas(conn, semantic_run_id: str, especificaciones: list[dict]):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM item_licitacion_especificaciones WHERE semantic_run_id = %s", (semantic_run_id,))
        
        # Mapeamos item_key -> item_id buscando en lo que acabamos de insertar (o ya existia)
        # IMPORTANTE: Esto asume que los items ya fueron insertados con el mismo semantic_run_id
        cur.execute("""
            SELECT item_key, id
            FROM items_licitacion
            WHERE semantic_run_id = %s
        """, (semantic_run_id,))
        rows = cur.fetchall()
        key_to_id = {r[0]: r[1] for r in rows}

        errores = []

        for spec in especificaciones:
            item_key = spec.get("item_key")
            item_id = key_to_id.get(item_key)

            if not item_id:
                errores.append(item_key)
                continue

            cur.execute("""
                INSERT INTO item_licitacion_especificaciones (
                    semantic_run_id,
                    item_id,
                    especificacion,
                    created_at
                ) VALUES (%s, %s, %s, %s)
            """, (
                semantic_run_id,
                item_id,
                spec.get("especificacion"),
                spec.get("created_at") or datetime.utcnow()
            ))

        if errores:
            print(f"[⚠️] No se pudieron mapear specs para claves: {errores}")

        conn.commit()

# --------------------------------------------------
# --------------------------------------------------
# ✅ FINANZAS_LICITACION
# --------------------------------------------------

def guardar_finanzas_licitacion(conn, licitacion_id, finanzas: dict):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def _ensure_json(val):
        if not val:
            return None
        if isinstance(val, (list, dict)):
            return json.dumps(val, ensure_ascii=False)
        if isinstance(val, str):
            # Si es string, ver si es un JSON valido
            try:
                json.loads(val)
                return val # Es un JSON valido en string
            except json.JSONDecodeError:
                # No es JSON, lo envolvemos
                return json.dumps({"texto_detectado": val}, ensure_ascii=False)
        return json.dumps({"valor": str(val)}, ensure_ascii=False)

    garantias_txt = _ensure_json(finanzas.get("garantias"))
    multas_txt = _ensure_json(finanzas.get("multas"))

    valores = {
        "licitacion_id": licitacion_id,
        "presupuesto_referencial": finanzas.get("presupuesto_referencial"),
        "moneda": finanzas.get("moneda"),
        "forma_pago": finanzas.get("forma_pago"),
        "plazo_pago": finanzas.get("plazo_pago"),
        "fuente_financiamiento": finanzas.get("fuente_financiamiento"),
        "garantias": garantias_txt,
        "multas": multas_txt,
        "otros": str(finanzas.get("otros", "")),
        "resumen": finanzas.get("resumen")
    }

    # Upsert logic manual
    sql_update = """
        UPDATE finanzas_licitacion
        SET
            presupuesto_referencial = %(presupuesto_referencial)s,
            moneda = %(moneda)s,
            forma_pago = %(forma_pago)s,
            plazo_pago = %(plazo_pago)s,
            fuente_financiamiento = %(fuente_financiamiento)s,
            garantias = %(garantias)s,
            multas = %(multas)s,
            otros = %(otros)s,
            resumen = %(resumen)s,
            updated_at = now()
        WHERE licitacion_id = %(licitacion_id)s
    """

    sql_insert = """
        INSERT INTO finanzas_licitacion (
            licitacion_id,
            presupuesto_referencial,
            moneda,
            forma_pago,
            plazo_pago,
            fuente_financiamiento,
            garantias,
            multas,
            otros,
            resumen
        ) VALUES (
            %(licitacion_id)s,
            %(presupuesto_referencial)s,
            %(moneda)s,
            %(forma_pago)s,
            %(plazo_pago)s,
            %(fuente_financiamiento)s,
            %(garantias)s,
            %(multas)s,
            %(otros)s,
            %(resumen)s
        )
    """

    try:
        with conn.cursor() as cur:
            cur.execute(sql_update, valores)
            if cur.rowcount == 0:
                cur.execute(sql_insert, valores)
        conn.commit()
        print(f"[{now}] ✅ Finanzas persistidas correctamente | licitacion_id={licitacion_id}")
    except Exception:
        print(f"[{now}] ❌ Error persistiendo finanzas | licitacion_id={licitacion_id}")
        traceback.print_exc()
        conn.rollback()
        raise

    valores = {
        "licitacion_id": licitacion_id,
        "presupuesto_referencial": finanzas.get("presupuesto_referencial"),
        "moneda": finanzas.get("moneda"),
        "forma_pago": finanzas.get("forma_pago"),
        "plazo_pago": finanzas.get("plazo_pago"),
        "fuente_financiamiento": finanzas.get("fuente_financiamiento"),
        "garantias": garantias_txt,
        "multas": multas_txt,
        "otros": str(finanzas.get("otros", "")),
        "resumen": finanzas.get("resumen")
    }

    # Upsert logic manual
    sql_update = """
        UPDATE finanzas_licitacion
        SET
            presupuesto_referencial = %(presupuesto_referencial)s,
            moneda = %(moneda)s,
            forma_pago = %(forma_pago)s,
            plazo_pago = %(plazo_pago)s,
            fuente_financiamiento = %(fuente_financiamiento)s,
            garantias = %(garantias)s,
            multas = %(multas)s,
            otros = %(otros)s,
            resumen = %(resumen)s,
            updated_at = now()
        WHERE licitacion_id = %(licitacion_id)s
    """

    sql_insert = """
        INSERT INTO finanzas_licitacion (
            licitacion_id,
            presupuesto_referencial,
            moneda,
            forma_pago,
            plazo_pago,
            fuente_financiamiento,
            garantias,
            multas,
            otros,
            resumen
        ) VALUES (
            %(licitacion_id)s,
            %(presupuesto_referencial)s,
            %(moneda)s,
            %(forma_pago)s,
            %(plazo_pago)s,
            %(fuente_financiamiento)s,
            %(garantias)s,
            %(multas)s,
            %(otros)s,
            %(resumen)s
        )
    """

    try:
        with conn.cursor() as cur:
            cur.execute(sql_update, valores)
            if cur.rowcount == 0:
                cur.execute(sql_insert, valores)
        conn.commit()
        print(f"[{now}] ✅ Finanzas persistidas correctamente | licitacion_id={licitacion_id}")
    except Exception:
        print(f"[{now}] ❌ Error persistiendo finanzas | licitacion_id={licitacion_id}")
        traceback.print_exc()
        conn.rollback()
        raise

def obtener_finanzas_por_licitacion(licitacion_id: str) -> dict | None:
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT presupuesto_referencial, moneda, forma_pago, plazo_pago, fuente_financiamiento
            FROM finanzas_licitacion
            WHERE licitacion_id = %s
        """, (str(licitacion_id),))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "presupuesto_referencial": row[0],
            "moneda": row[1],
            "forma_pago": row[2],
            "plazo_pago": row[3],
            "fuente_financiamiento": row[4]
        }
    finally:
        cur.close()
        conn.close()

def actualizar_datos_basicos_licitacion(licitacion_id: str, datos: dict) -> None:
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        update_fields = []
        update_values = []

        # Mapeo de campos JSON a columnas DB
        # datos viene del JSON extractor, puede tener claves distintas?
        # Asumimos que el extractor usa las mismas keys o muy similares.
        
        # Extractor keys vs DB columns:
        # titulo -> nombre
        # descripcion -> descripcion
        # organismo -> organismo_solicitante
        # numero_licitacion -> codigo_licitacion
        
        mapping = {
            # "titulo": "nombre", # COMENTADO: No actualizar nombre/titulo para respetar el original
            "descripcion": "descripcion",
            "entidad_solicitante": "entidad_solicitante",
            "unidad_compra": "unidad_compra",
            "numero_licitacion": "codigo_licitacion",
            # Si vienen ya con nombre de columna:
            # "nombre": "nombre", # COMENTADO: No actualizar nombre para respetar el original
            "codigo_licitacion": "codigo_licitacion"
        }

        for key, val in datos.items():
            col = mapping.get(key)
            if col and val:
                update_fields.append(f"{col} = %s")
                update_values.append(val)
        
        # También estado si viniera
        if "estado" in datos:
            update_fields.append("estado_publicacion = %s")
            update_values.append(datos["estado"])

        if not update_fields:
            # Si solo venia el nombre y lo ignoramos, puede quedar vacío
            print(f"⚠️ No hay campos para actualizar en datos básicos de {licitacion_id} (Nombre ignorado)")
            return

        update_values.append(str(licitacion_id))
        query = f"""
            UPDATE licitaciones
            SET {', '.join(update_fields)}
            WHERE id = %s
        """
        cur.execute(query, tuple(update_values))
        conn.commit()
        print(f"✅ Datos básicos actualizados para {licitacion_id}")
    except Exception as e:
        print(f"❌ Error actualizando datos básicos: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def actualizar_estado_licitacion(licitacion_id: str, nuevo_estado: str) -> None:
    """Actualiza solo el estado de la licitación."""
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE licitaciones SET estado = %s WHERE id = %s", (nuevo_estado, str(licitacion_id)))
        conn.commit()
        print(f"🔄 Estado de Licitación {licitacion_id} actualizado a: {nuevo_estado}")
    except Exception as e:
        print(f"❌ Error actualizando estado de licitación: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
