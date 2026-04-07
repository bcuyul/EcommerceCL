# Solución de Problemas (Troubleshooting) — Ecommerce CL v3

Este documento enumera los errores más comunes y sus soluciones para que el sistema esté siempre operativo.

---

## 1. Problemas de Postgres (Docker)
- **Error**: `db:5432 - Port conflict`
- **Solución**: En `easy_mode/docker-compose.yml`, cambia el mapeo `ports: - "5433:5432"` por otro como `5434:5432`.
- **Error**: `Is the server running on host "localhost" (127.0.0.1) and accepting TCP/IP connections on port 5432?`
- **Solución**: Verifica que el contenedor Postgres esté arriba con `docker ps`. Asegura que el `.env` coincide con el puerto local configurado.

## 2. Errores de Permisos (RBAC)
- **Error**: `permission denied for schema internal` (como usuario pbi_executive).
- **Solución**: Es por diseño. Las vistas del schema `warehouse` y las tablas de hechos deben ser consultadas directamente. Si necesitas más permisos, corre `04_rbac.sql` de nuevo o ajusta los `GRANT` manualmente.

## 3. Fallas en el Pipeline OLAP
- **Error**: `Error processing event... Sent to DLQ`.
- **Solución**: Consulta `SELECT * FROM warehouse.dlq_eventos_error`. Analiza el campo `error_mensaje` para diagnosticar fallas en el payload o violaciones de integridad referencial. El pipeline sigue funcionando a pesar de estos errores individuales.

## 4. Superset no Carga
- **Error**: `CSRF token missing or invalid`.
- **Solución**: Limpia las cookies de tu navegador o asegúrate de que `SUPERSET_SECRET_KEY` esté configurada en el `.env` (en `easy_mode/superset/superset_config.py`).
- **Error**: `No such database: ecommerce_cl`.
- **Solución**: Superset necesita que conectes la DB manualmente en la UI (Settings -> Database Connections). Usa el URI: `postgresql://pbi_executive:pbi123@db:5432/ecommerce_cl`.

---
Si el error persiste, revisa los logs detallados con:
`docker compose logs -f`
