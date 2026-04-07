# Registro de Errores Corregidos (Bugfix Log) — Ecommerce CL v3

Este documento detalla los problemas técnicos resueltos durante el desarrollo y la professionalización de la plataforma.

---

## [FIX-V3-12] Aislamiento de Metadatos de BI
- **Problema**: Superset usaba `ecommerce_cl` como backend de metadatos, contaminando las tablas de negocio.
- **Corrección**: Se creó una base de datos dedicada `superset_db` dentro de la orquestación de Docker Compose para Easy Mode.

## [FIX-V3-13] Conflictos de Puertos (PostgreSQL)
- **Problema**: El puerto por defecto `5432` solía colisionar con instalaciones locales de Postgres.
- **Corrección**: En Easy Mode, el puerto expuesto al host es ahora `5433` mediante mapeo en `docker-compose.yml`.

## [FIX-V3-14] Bootstrap Automatizado en Docker
- **Problema**: Al levantar el stack desde cero, los scripts fallaban si las bases de datos no existían previamente.
- **Corrección**: Se implementó `00_bootstrap.sql` para crear DBs y Roles antes del resto de los scripts del schema.

## [FIX-V3-15] Naming de Motores (v2 vs v3)
- **Problema**: Referencias cruzadas a `run_platform_v2.py` causaban confusión en la trazabilidad.
- **Corrección**: Se renombró unificadamente a `run_platform_v3.py` en todo el repositorio.

---
Para más detalles sobre decisiones de arquitectura, ver `decision_log.md`.
