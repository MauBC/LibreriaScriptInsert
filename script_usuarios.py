import pymysql
from faker import Faker
import bcrypt
import random
import csv
import time
from datetime import datetime

fake = Faker('es')

distritos = ['Miraflores', 'San Isidro', 'Surco', 'Barranco', 'La Molina', 'Callao', 'San Miguel']
departamentos = ['Lima', 'Cusco', 'Arequipa', 'La Libertad', 'Piura', 'Junín', 'Lambayeque']

conn = pymysql.connect(
    host='54.227.94.148',
    port=4002,
    user='root',
    password='admin',
    database='clientes_db',
    charset='utf8mb4'
)

cursor = conn.cursor()

HASH_FIJO = bcrypt.hashpw("Contrasena123!".encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def generar_usuario():
    return (
        fake.name(),
        fake.unique.email(),
        HASH_FIJO,
        '9' + ''.join([str(random.randint(0, 9)) for _ in range(8)]),
        fake.street_address(),
        random.choice(distritos),
        random.choice(departamentos),
        'user',
        True,
        datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    )

sql = """
INSERT INTO usuarios (nombre, email, password, telefono, direccion, distrito, departamento, rol, estado, fecha_registro)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

TOTAL_USUARIOS = 20000
BLOQUE = 50
inserted_ids = []
start = time.time()

for bloque in range(0, TOTAL_USUARIOS, BLOQUE):
    print(f"\n🚀 Insertando usuarios {bloque+1} a {bloque+BLOQUE}...")
    for i in range(BLOQUE):
        user = generar_usuario()
        try:
            cursor.execute(sql, user)
            user_id = cursor.lastrowid
            inserted_ids.append(user_id)
        except Exception as e:
            print(f"❌ Error en el usuario {bloque + i + 1}: {e}")
    conn.commit()
    print(f"✅ Bloque {bloque+1}-{bloque+BLOQUE} insertado.")

cursor.close()
conn.close()

# Guardar en CSV
with open("usuarios_insertados.csv", "w", newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["user_id"])
    for user_id in inserted_ids:
        writer.writerow([user_id])

print(f"\n✅ Inserción completa de {len(inserted_ids)} usuarios.")
print(f"🕒 Tiempo total: {time.time() - start:.2f} segundos")
print("📝 IDs guardados en usuarios_insertados.csv")
