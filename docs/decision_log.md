# Registro de Decisiones de Diseño (Decision Log) — Ecommerce CL v3

Este log captura las decisiones arquitectónicas tomadas para garantizar la robustez del "Ecommerce CL v3".

---

## [ADR-01] Uso de LISTEN/NOTIFY para OLAP en Tiempo Real
- **Decisión**: Se eligió PostgreSQL `LISTEN/NOTIFY` en lugar de una cola de mensajes externa (como Kafka/RabbitMQ).
- **Razón**: Reducción drástica en la complejidad del stack para un ecommerce mediano ("open source", sin cloud pago). Mantiene todo dentro de una única base de datos transaccional con integridad ACID garantizada para el pipeline.

## [ADR-02] SCD Tipo 2 en Warehouse
- **Decisión**: Implementar `Slowly Changing Dimensions (SCD2)` con `inicio_vigencia`, `fin_vigencia` y `es_actual`.
- **Razón**: Permite análisis histórico exacto. Si el nombre de un producto cambia hoy, los reportes de ventas de ayer seguirán mostrando el nombre antiguo, lo que es vital para cuadratura financiera y de inventario.

## [ADR-03] Uso de `asyncpg` y `asyncio`
- **Decisión**: El motor Python debe ser asíncrono.
- **Razón**: Escalar bajo alta concurrencia de eventos sin depender de hilos (threads). Facilita manejar cientos de pedidos por segundo con una huella de memoria mínima.

---
Para más detalles sobre los errores corregidos, ver `bugfix_log.md`.
