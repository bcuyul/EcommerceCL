#!/bin/bash
set -e

echo "--------------------------------------------------------"
echo "[INIT] Esperando disponibilidad de PostgreSQL..."

until python - <<EOF
import socket
s = socket.socket()
try:
    s.connect(("db", 5432))
    s.close()
    exit(0)
except:
    exit(1)
EOF
do
  echo "[INIT] Postgres aún no responde. Reintentando..."
  sleep 2
done

echo "[INIT] PostgreSQL detectado. Iniciando bootstrap..."

echo "[INIT] Ejecutando migraciones..."
superset db upgrade

echo "[INIT] Creando usuario admin (si no existe)..."
superset fab create-admin \
  --username admin \
  --firstname Superset \
  --lastname Admin \
  --email admin@example.com \
  --password admin || true

echo "[INIT] Inicializando Superset..."
superset init

echo "[INIT] Bootstrap completado con éxito."
echo "--------------------------------------------------------"
exit 0