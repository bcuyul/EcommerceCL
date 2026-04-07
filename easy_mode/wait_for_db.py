import socket
import time

HOST = "db"
PORT = 5432

print("[PLATFORM] Esperando PostgreSQL...")

while True:
    try:
        s = socket.socket()
        s.connect((HOST, PORT))
        s.close()
        print("[PLATFORM] PostgreSQL disponible.")
        break
    except Exception:
        print("[PLATFORM] DB aún no responde. Reintentando en 3s...")
        time.sleep(3)