# Easy Mode — Levanta la plataforma en 1 comando

Esta carpeta contiene todo lo necesario para correr la plataforma completa con Docker. No necesitas instalar Python, PostgreSQL ni nada más — Docker lo gestiona todo por ti.

---

## Antes de empezar

Asegúrate de tener instalado y corriendo **Docker Desktop**:
- [Descargar Docker Desktop](https://www.docker.com/products/docker-desktop)
- RAM disponible recomendada: **4 GB** (Apache Superset lo necesita)
- CPU mínimo: **2 núcleos**

---

## Iniciar la plataforma

Desde esta carpeta (`easy_mode/`), ejecuta:

```bash
docker compose up -d
```

Para verificar que todo está corriendo:

```bash
docker compose ps
```

> El servicio `superset-init` aparecerá como `Exited (0)` — eso es normal. Solo corre una vez para configurar Superset y luego se detiene.

---

## Crear los usuarios de Superset

Una vez que el stack esté levantado, ejecuta estos tres comandos para crear los usuarios:

**Usuario admin** (control total):
```bash
docker exec -it ecommerce_superset_v3 superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@superset.com \
  --password admin
```

**Usuario pbi_executive** (solo lectura):
```bash
docker exec -it ecommerce_superset_v3 superset fab create-user \
  --role Gamma \
  --username pbi_executive \
  --firstname Executive \
  --lastname BI \
  --email pbi_executive@ecommerce.cl \
  --password pbi123
```

**Usuario pbi_analytical** (exploración):
```bash
docker exec -it ecommerce_superset_v3 superset fab create-user \
  --role Alpha \
  --username pbi_analytical \
  --firstname Analyst \
  --lastname BI \
  --email pbi_analytical@ecommerce.cl \
  --password data_pbi_123
```

| Usuario | Contraseña | Acceso |
|---|---|---|
| `admin` | `admin` | Control total |
| `pbi_executive` | `pbi123` | Solo ve dashboards |
| `pbi_analytical` | `data_pbi_123` | Puede crear charts |

---

## Acceder al dashboard

Abre: **[http://localhost:8088](http://localhost:8088)**

---

## Conectar Superset a la base de datos

Con el usuario `admin`, una sola vez:

1. **Settings → Database Connections → + Database → PostgreSQL**
2. Completa los campos:

   | Campo | Valor |
   |---|---|
   | **Host** | `db` |
   | **Port** | `5432` |
   | **Database name** | `ecommerce_cl` |
   | **Username** | `postgres` |
   | **Password** | `postgres` |

3. **Test Connection** → **Connect**

---

## Puertos utilizados

| Servicio | Puerto |
|---|---|
| Dashboard (Superset) | `8088` → [http://localhost:8088](http://localhost:8088) |
| Base de datos | `5433` (DBeaver, TablePlus, etc.) |

---

## Detener la plataforma

```bash
docker compose down
```

Borrar datos y empezar desde cero:

```bash
docker compose down -v
```

---

## ¿Algo no funciona?

```bash
docker compose logs -f

# Solo logs del init:
docker compose logs superset-init
```