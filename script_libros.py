import psycopg2
from faker import Faker
import random
import csv
import time

fake = Faker()

# Configuracion de PostgreSQL
conn = psycopg2.connect(
    host='54.227.94.148',
    port=4001,
    user='postgres',
    password='root',
    dbname='library_db'
)
cursor = conn.cursor()

# ---------- Insertar Autores ----------
def insertar_autores(n=100):
    autor_ids = []
    nombres_insertados = set()
    print(f"\nInsertando {n} autores...")

    for _ in range(n):
        while True:
            nombre = fake.name()
            if nombre not in nombres_insertados:
                nombres_insertados.add(nombre)
                break
        cursor.execute("INSERT INTO authors (name) VALUES (%s) RETURNING id;", (nombre,))
        autor_id = cursor.fetchone()[0]
        autor_ids.append((autor_id, nombre))

    conn.commit()
    print("Autores insertados.")
    return autor_ids

# ---------- Insertar Libros en bloques ----------
def insertar_libros(total=20000, bloque=1000, autores=[]):
    print(f"\nInsertando {total} libros en bloques de {bloque}...")

    for inicio in range(0, total, bloque):
        print(f"🚀 Insertando libros {inicio + 1} a {inicio + bloque}...")
        for _ in range(bloque):
            titulo = fake.sentence(nb_words=4).replace('.', '')
            author_id = random.choice(autores)[0]
            quantity = random.randint(1, 20)
            year = random.randint(1950, 2024)

            cursor.execute(
                "INSERT INTO books (title, author_id, quantity, published_year) VALUES (%s, %s, %s, %s);",
                (titulo, author_id, quantity, year)
            )

        conn.commit()
        print(f"✅ Bloque {inicio + 1}-{inicio + bloque} insertado.")

# ---------- Ejecución principal ----------
start = time.time()

autores = insertar_autores(100)
insertar_libros(total=20000, bloque=100, autores=autores)

cursor.close()
conn.close()

# Guardar autores en CSV
with open("autores_insertados.csv", "w", newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["author_id", "name"])
    for autor_id, nombre in autores:
        writer.writerow([autor_id, nombre])

print(f"\n✅ Inserción completa de autores y libros.")
print(f"🕒 Tiempo total: {time.time() - start:.2f} segundos")
