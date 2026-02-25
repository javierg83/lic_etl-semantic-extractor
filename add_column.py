import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv("DATABASE_URL")
if not db_url:
    print("Error: DATABASE_URL not found.")
    exit(1)

conn = psycopg2.connect(db_url)
try:
    with conn.cursor() as cur:
        cur.execute("""
        ALTER TABLE public.homologaciones_productos
        ADD COLUMN IF NOT EXISTS razonamiento_general TEXT;
        """)
        conn.commit()
    print("Columna razonamiento_general agregada exitosamente.")
except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
