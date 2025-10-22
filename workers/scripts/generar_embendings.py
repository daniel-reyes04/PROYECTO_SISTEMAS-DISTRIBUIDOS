from sentence_transformers import SentenceTransformer
import psycopg2, json

print("Generando embeddings...")

model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
conn = psycopg2.connect(
    host="db",
    dbname="cinesense_ai_db",
    user="cinesense_user",
    password="cinesense_pass"
)
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
