from sentence_transformers import SentenceTransformer
import psycopg2, json, os, time # Añadir os y time

# --- Configuración de DB ---
DB_HOST = os.getenv('DB_HOST', 'db')
DB_NAME = os.getenv('DB_NAME', 'cinesense_ai_db')
DB_USER = os.getenv('DB_USER', 'cinesense_user')
DB_PASS = os.getenv('DB_PASS', 'cinesense_pass')

print("Generando embeddings...")

model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

while True:
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Conexión a la Base de Datos establecida.")
        break
    except psycopg2.OperationalError:
        print("Esperando conexión con la DB...")
        time.sleep(3)

cur = conn.cursor()

cur.execute("SELECT id, sinopsis_original FROM Peliculas WHERE embedding_sinopsis IS NULL;")
rows = cur.fetchall()

for id_p, sinopsis in rows:
    vec = model.encode(sinopsis)
    cur.execute(
        "UPDATE Peliculas SET embedding_sinopsis = %s WHERE id = %s;",
        (json.dumps(vec.tolist()), id_p)
    )
    print(f"película {id_p} procesada")

conn.commit()
conn.close()
print("Embeddings generados para todas las películas.")
