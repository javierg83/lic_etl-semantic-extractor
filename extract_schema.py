import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Configuración de conexión (usando las mismas variables que config.py o backend)
# Ajusta si las variables se llaman diferente en tu entorno
DB_HOST = os.getenv("DB_POSTGRES_HOST") or os.getenv("POSTGRES_HOST")
DB_NAME = os.getenv("DB_POSTGRES_NAME") or os.getenv("POSTGRES_DB")
DB_USER = os.getenv("DB_POSTGRES_USER") or os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("DB_POSTGRES_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
DB_PORT = os.getenv("DB_POSTGRES_PORT") or os.getenv("POSTGRES_PORT")

def get_schema_info():
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        print("❌ Faltan variables de entorno para la conexión a la base de datos.")
        print(f"Host: {DB_HOST}, User: {DB_USER}, DB: {DB_NAME}")
        return

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cur = conn.cursor()

        # Obtener todas las tablas
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()

        print("\n📊 ESQUEMA DE BASE DE DATOS ACTUAL:\n")

        for table in tables:
            table_name = table[0]
            print(f"🔹 Tabla: {table_name}")
            
            # Obtener columnas de cada tabla
            cur.execute(f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position;
            """)
            columns = cur.fetchall()
            
            for col in columns:
                col_name, data_type, nullable, default = col
                print(f"   - {col_name} ({data_type}) [Null: {nullable}] [Def: {default}]")
            print("-" * 40)

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error al conectar o consultar la base de datos: {e}")

if __name__ == "__main__":
    get_schema_info()
