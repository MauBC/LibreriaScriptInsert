import pymongo
import random
import csv
from datetime import datetime, timedelta
import time

# 📦 Cargar IDs desde archivos CSV
def cargar_ids(path):
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return [row['user_id'] if 'user_id' in row else row['id'] for row in reader]

# 🧠 Generar préstamos para usuarios
def generar_prestamos_para_usuario(user_id, book_ids, max_prestamos=3):
    prestamos = []
    libros_prestados = random.sample(book_ids, min(max_prestamos, len(book_ids)))
    for book_id in libros_prestados:
        fecha_prestamo = datetime.now() - timedelta(days=random.randint(1, 30))
        fecha_devolucion = fecha_prestamo + timedelta(days=7)
        status = random.choice(["active", "returned"])
        prestamo = {
            "user_id": str(user_id),
            "book_id": str(book_id),
            "loan_date": fecha_prestamo,
            "return_date": fecha_devolucion,
            "status": status,
            "createdAt": datetime.now(),
            "updatedAt": datetime.now()
        }
        prestamos.append(prestamo)
    return prestamos

# 🔌 Conexión a MongoDB
cliente = pymongo.MongoClient("mongodb://3.87.92.174:4003")
db = cliente["loan"]
coleccion = db["loan"]

# 📥 Cargar IDs
user_ids = cargar_ids("usuarios_insertados.csv")
book_ids = list(range(1, 20001))  # Simulando 20,000 libros

# 🧪 Configuración
BLOQUE = 150
prestamos_buffer = []
total_insertados = 0

start = time.time()

for idx, user_id in enumerate(user_ids):
    prestamos = generar_prestamos_para_usuario(user_id, book_ids, max_prestamos=3)
    prestamos_buffer.extend(prestamos)

    if len(prestamos_buffer) >= BLOQUE:
        coleccion.insert_many(prestamos_buffer[:BLOQUE])
        prestamos_buffer = prestamos_buffer[BLOQUE:]
        total_insertados += BLOQUE
        print(f"✅ {total_insertados} préstamos insertados...")

# Insertar los restantes
if prestamos_buffer:
    coleccion.insert_many(prestamos_buffer)
    total_insertados += len(prestamos_buffer)
    print(f"✅ {total_insertados} préstamos insertados en total.")

# Asegurar al menos 1 préstamo si no hay ninguno
if total_insertados == 0:
    uno = {
        "user_id": str(random.choice(user_ids)),
        "book_id": str(random.choice(book_ids)),
        "loan_date": datetime.now(),
        "return_date": datetime.now() + timedelta(days=7),
        "status": "active",
        "createdAt": datetime.now(),
        "updatedAt": datetime.now()
    }
    coleccion.insert_one(uno)
    total_insertados = 1
    print("✅ Se insertó un préstamo mínimo por requisito.")

print(f"\n📚 Inserción finalizada con éxito.")
print(f"🧾 Total de préstamos: {total_insertados}")
print(f"🕒 Tiempo total: {time.time() - start:.2f} segundos")
