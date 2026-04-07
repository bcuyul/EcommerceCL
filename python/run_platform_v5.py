"""
Plataforma de datos ecommerce v2 — Simulador OLTP + Motor de Ciclo de Vida + Procesador OLAP.

Arquitectura:
  Motor OLTP         → genera tráfico transaccional asíncrono (pedidos, pagos, stock, carritos)
  Motor Ciclo de Vida→ NEW en v2: avanza pedidos deterministicamente por toda la cadena de estados
  Motor OLAP         → escucha pg_notify en tiempo real + polling; carga Star Schema Kimball SCD2

═══════════════════════════════════════════════════════════════════════════════
CORRECCIONES v5 — ANÁLISIS DE CAUSA RAÍZ Y SOLUCIONES
═══════════════════════════════════════════════════════════════════════════════

[BUG-01] FACT_ENVIOS VACÍA — CAUSA RAÍZ: MÁQUINA DE ESTADOS ROTA
  Diagnóstico: Con PROB nuevo_pedido=0.30 y avanzar_estado=0.25, por cada ciclo
    se crean ~1.2× más pedidos de los que avanzan. Cada pedido necesita 6 avances
    para llegar a 'enviado'. Matemáticamente: los pedidos se acumulan sin progresar.
    accion_avanzar_estado() elige un pedido ALEATORIO de todos los estados intermedios,
    por lo que la probabilidad de que un pedido específico llegue a 'enviado' es
    minúscula cuando hay miles de pedidos atascados.
  Solución: [FIX-V5-01] motor_ciclo_vida() — coroutine dedicada que escanea pedidos
    por estado y los avanza sistemáticamente, desacoplado del ciclo OLTP aleatorio.

[BUG-02] ESTADOS INTERMEDIOS SIN REGISTRAR
  Diagnóstico: fact_pedidos.estado_fulfillment solo se actualizaba en 'enviado'
    y 'entregado'. Estados intermedios ('preparando','empaquetado') no tenían
    representación en OLAP.
  Solución: [FIX-V5-02] olap_process_pedido_actualizado() ahora mapea TODOS los estados
    del flujo a estado_fulfillment correctamente.

[BUG-03] actualizado_en NO EXISTE EN envios (ERROR SILENCIOSO CRÍTICO)
  Diagnóstico: La tabla envios (DDL 01_oltp.sql) NO tiene columna actualizado_en.
    v4 ejecutaba UPDATE envios SET estado_envio=..., actualizado_en=NOW() → ERROR.
    Esto hacía que accion_devolucion() fallara silenciosamente al actualizar envíos.
  Solución: [FIX-V5-03] Eliminado actualizado_en de TODOS los UPDATEs sobre envios.

[BUG-04] carrito_id EN pedidos: COLUMNA INEXISTENTE EN DDL
  Diagnóstico: El DDL de pedidos (01_oltp.sql) no incluye carrito_id. El INSERT
    condicional de v4 era frágil: probaba la existencia de la columna al startup pero
    podía fallar en escenarios de race condition o reconexión.
  Solución: [FIX-V5-04] Verificación robusta al inicio con flag global _SCHEMA_TIENE_CARRITO_ID.
    El INSERT usa la versión sin carrito_id si la columna no existe. No se asume estructura.

[BUG-05] CARRITOS SUBREPRESENTADOS (5% → 15%)
  Diagnóstico: PROB carrito=0.05 generaba muy pocos carritos → fact_carritos
    con ~100 filas frente a ~2000 de fact_pedidos. Tasa de conversión incalculable.
  Solución: [FIX-V5-05] PROB carrito=0.15, PROB_CARRITO_CONVERSION=0.70.
    Dataset útil para métricas de funnel y análisis de abandono.

[BUG-06] PROBABILIDADES DE ACCIONES DESBALANCEADAS
  Diagnóstico: avanzar_estado=0.25 + nuevo_pedido=0.30 generaban backlog creciente.
    Poco peso en acciones que dependen de estado 'entregado' (devolucion=0.04).
  Solución: [FIX-V5-06] Redistribución: motor_ciclo_vida absorbe el avance de estados;
    las acciones OLTP se reasignan para mayor cobertura de tipos de eventos.

[BUG-07] accion_avanzar_estado: SELECCIÓN ALEATORIA SIN PRIORIDAD
  Diagnóstico: _random_id con TABLESAMPLE() podía devolver pedidos recién creados
    en lugar de los más antiguos atascados. Los pedidos "viejos" no progresaban.
  Solución: [FIX-V5-07] accion_avanzar_estado() ahora prioriza el pedido MÁS ANTIGUO
    en cada estado intermedio, con costo de un ORDER BY + LIMIT 1.

[BUG-08] dias_entrega NULL CUANDO FLUJO NO LLEGA A ENTREGA
  Diagnóstico: olap_process_envio() calculaba dias=None si enviado_en o entregado_en
    eran NULL. Con el flujo roto, la mayoría de envíos tenían dias_entrega=NULL.
  Solución: [FIX-V5-08] motor_ciclo_vida garantiza que cada envío llega a 'entregado'.
    Además, si por cualquier razón los timestamps faltan, se imputa un valor razonable
    basado en distribución normal truncada (1-10 días, media 3.5).

[BUG-09] OLAP fact_pedidos: estado_fulfillment IGNORABA ESTADOS INTERMEDIOS
  Diagnóstico: olap_process_pedido_actualizado solo actualizaba fulfillment para
    'enviado','entregado','devuelto'. Estados como 'preparando','empaquetado' no
    se reflejaban en el warehouse.
  Solución: [FIX-V5-09] Mapa completo ESTADO_FULFILLMENT_MAP en el handler OLAP.

[BUG-10] SESGO EN DATOS: DATASET DEMASIADO PERFECTO
  Diagnóstico: Intervalos fijos, probabilidades exactas → datos con distribuciones
    artificialmente uniformes. No útil para demos de BI real.
  Solución: [FIX-V5-10] Variabilidad: beta-distribution para cantidades, lognormal
    para precios, Poisson implícito via jitter en intervalos. Ciclo_vida usa delays
    variables por estado para simular SLA realista.

Uso:
  python run_platform_v5.py                        # Ambos motores + ciclo_vida
  python run_platform_v5.py --solo-oltp            # Solo OLTP + ciclo_vida
  python run_platform_v5.py --solo-olap            # Solo OLAP
  python run_platform_v5.py --interval 2 6         # Intervalo OLTP personalizado
  python run_platform_v5.py --max-pedidos 50000    # Límite de pedidos
  python run_platform_v5.py --cv-interval 5        # Ciclo de vida cada 5s (default: 8)
"""

import asyncio
import json
import logging
import math
import os
import random
import signal
import string
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("platform")

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME",     "ecommerce_cl"),
    "user":     os.getenv("DB_USER",     "ecommerce"),
    "password": os.getenv("DB_PASS",     "ecommerce123"),
}

OLTP_INTERVAL_MIN   = float(os.getenv("OLTP_INTERVAL_MIN",   "3"))
OLTP_INTERVAL_MAX   = float(os.getenv("OLTP_INTERVAL_MAX",   "8"))
OLTP_MAX_PEDIDOS    = int(os.getenv("OLTP_MAX_PEDIDOS",       "100000"))
OLAP_BATCH_SIZE     = int(os.getenv("OLAP_BATCH_SIZE",        "100"))
OLAP_POLL_INTERVAL  = float(os.getenv("OLAP_POLL_INTERVAL",   "2"))
OLAP_HEALTH_EVERY   = int(os.getenv("OLAP_HEALTH_EVERY",      "60"))
MAX_OLAP_RETRIES    = int(os.getenv("MAX_OLAP_RETRIES",       "3"))

# [FIX-V5-06] Intervalo del motor de ciclo de vida
CV_INTERVAL         = float(os.getenv("CV_INTERVAL",          "8"))
# Máxima edad (segundos) para que un pedido avance automáticamente
CV_MIN_AGE_SECONDS  = int(os.getenv("CV_MIN_AGE_SECONDS",     "30"))
# Máximo de pedidos a avanzar por ciclo por estado
CV_BATCH_PER_STATE  = int(os.getenv("CV_BATCH_PER_STATE",     "15"))

# [FIX-V5-05] Probabilidad de conversión de carrito
PROB_CARRITO_CONVERSION = float(os.getenv("PROB_CARRITO_CONVERSION", "0.70"))

# [FIX-V5-06] Probabilidades redistribuidas
# motor_ciclo_vida se encarga del avance de estados → se reduce avanzar_estado
PROB = {
    "nuevo_pedido":    0.28,   # ligeramente reducido para no saturar el pipeline
    "avanzar_estado":  0.12,   # reducido: ciclo_vida absorbe el grueso del avance
    "procesar_pago":   0.17,
    "cancelar_pedido": 0.04,
    "reponer_stock":   0.10,
    "devolucion":      0.06,
    "carrito":         0.15,   # [FIX-V5-05] aumentado desde 0.05
    "actualizar_envio": 0.08,
}

# Cadena completa de estados de un pedido
ESTADOS_FLUJO = [
    "pendiente", "confirmado", "pagado",
    "preparando", "empaquetado", "enviado", "entregado"
]

# [FIX-V5-09] Mapa estado_actual → estado_fulfillment
ESTADO_FULFILLMENT_MAP = {
    "pendiente":   "pendiente",
    "confirmado":  "pendiente",
    "pagado":      "pendiente",
    "preparando":  "preparando",
    "empaquetado": "listo",
    "enviado":     "enviado",
    "entregado":   "entregado",
    "cancelado":   "pendiente",
    "reembolsado": "pendiente",
    "devuelto":    "devuelto",
}

REGIONES       = ["Metropolitana", "Valparaiso", "Biobio", "Araucania", "Antofagasta",
                  "Coquimbo", "Maule", "O'Higgins", "Los Lagos", "Tarapaca"]
COMUNAS        = ["Santiago", "Providencia", "Las Condes", "Nunoa", "Vitacura",
                  "Vina del Mar", "Concepcion", "Temuco", "Antofagasta", "La Serena",
                  "Rancagua", "Talca", "Puerto Montt", "Iquique", "Maipu"]
METODOS_PAGO   = ["tarjeta_credito", "tarjeta_debito", "transferencia",
                  "billetera_virtual", "cuotas"]
PROV_PAGO      = ["Transbank", "MercadoPago", "Flow", "Khipu", "Kushki"]
TRANSPORTISTAS = ["Chilexpress", "Starken", "BlueExpress", "Correos Chile", "Urbano"]
CANALES        = ["web", "web", "web", "mobile", "mobile", "marketplace"]
MOTIVOS_DEV    = ["producto_defectuoso", "no_cumple_expectativa",
                  "talla_incorrecta", "cambio_decision"]

# [FIX-V5-04] Flag global: si pedidos.carrito_id existe en el schema real
_SCHEMA_TIENE_CARRITO_ID = False

running = True
oltp_stats = {
    "pedidos": 0, "pagos": 0, "estados": 0, "cancelaciones": 0,
    "reposiciones": 0, "devoluciones": 0,
    "carritos_convertidos": 0, "carritos_abandonados": 0,
    "envios": 0, "ciclos": 0, "errores": 0,
    "cv_avances": 0,  # [FIX-V5-01] avances realizados por motor_ciclo_vida
}
olap_stats = {
    "procesados": 0, "errores": 0, "dim_upserts": 0,
    "facts_inserted": 0, "facts_updated": 0, "dlq": 0,
}


def _rand_str(length=8):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def _dias_entrega_simulado():
    """[FIX-V5-10] Distribución realista de días de entrega (1-10, media ~3.5)."""
    # Lognormal truncada: mayoría en 2-5 días, cola hasta 10
    val = int(round(random.lognormvariate(1.1, 0.5)))
    return max(1, min(10, val))


def _signal_handler(sig, frame):
    global running
    log.info("Señal de detención recibida. Finalizando motores...")
    running = False


signal.signal(signal.SIGINT,  _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# =============================================================================
# HELPERS COMPARTIDOS
# =============================================================================

async def oltp_emit_evento(conn, tipo_entidad, entidad_id, tipo_evento, payload):
    """Inserta evento de negocio — dispara pg_notify via trigger."""
    await conn.execute("""
        INSERT INTO eventos_negocio (tipo_entidad, entidad_id, tipo_evento, payload)
        VALUES ($1, $2, $3, $4::jsonb)
    """, tipo_entidad, entidad_id, tipo_evento, json.dumps(payload))


async def _random_id(conn, table, pk_col, condition="TRUE"):
    """PK aleatorio via TABLESAMPLE + fallback OFFSET."""
    row = await conn.fetchrow(
        f"SELECT {pk_col} FROM {table} TABLESAMPLE BERNOULLI(5) WHERE {condition} LIMIT 1"
    )
    if row:
        return row[0]
    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
    if not count:
        return None
    offset = random.randint(0, count - 1)
    row = await conn.fetchrow(
        f"SELECT {pk_col} FROM {table} WHERE {condition} LIMIT 1 OFFSET {offset}"
    )
    return row[0] if row else None


async def _random_ids(conn, table, pk_col, condition="TRUE", n=5):
    """N PKs aleatorios via TABLESAMPLE + fallback."""
    rows = await conn.fetch(
        f"SELECT DISTINCT {pk_col} FROM {table} TABLESAMPLE BERNOULLI(10) "
        f"WHERE {condition} LIMIT {n * 4}"
    )
    if len(rows) >= n:
        return [r[0] for r in random.sample(rows, n)]
    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
    if not count:
        return []
    offset = random.randint(0, max(0, count - n))
    rows = await conn.fetch(
        f"SELECT {pk_col} FROM {table} WHERE {condition} LIMIT {n} OFFSET {offset}"
    )
    return [r[0] for r in rows]


# =============================================================================
# OLTP — HELPERS DE CONSTRUCCIÓN DE PEDIDO
# =============================================================================

async def _build_items_data(conn, var_ids):
    """
    Dado un conjunto de variante_ids, devuelve (items_data, subtotal, desc_tot,
    imp_tot, costo_envio, total_final) listos para INSERT.
    [FIX-V5-10] Variabilidad: cantidades con distribución beta, descuentos variables.
    """
    variantes = await conn.fetch("""
        SELECT vp.variante_id, pp.precio_lista, vp.sku,
               p.nombre || ' ' || COALESCE(vp.color, '') AS nombre_snap
        FROM variantes_producto vp
        JOIN precios_producto pp ON pp.variante_id = vp.variante_id
        JOIN productos p          ON p.producto_id  = vp.producto_id
        WHERE vp.variante_id = ANY($1::bigint[]) AND vp.activa = TRUE
    """, var_ids)
    if not variantes:
        return None, None, None, None, None, None

    # [FIX-V5-10] número de ítems con distribución más realista (Zipf-like)
    num_items = random.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0]
    num_items = min(num_items, len(variantes))
    items     = random.sample(list(variantes), num_items)

    subtotal   = Decimal("0")
    items_data = []

    for v in items:
        # [FIX-V5-10] cantidades con sesgo hacia 1 unidad
        cant        = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
        precio      = Decimal(str(v["precio_lista"]))
        linea_bruta = precio * cant
        # descuento de línea (~15% de probabilidad)
        desc_l      = (linea_bruta * Decimal(str(round(random.uniform(0.05, 0.15), 2)))).quantize(
            Decimal("0.01")) if random.random() > 0.85 else Decimal("0")
        linea_neta  = linea_bruta - desc_l
        imp_l       = (linea_neta * Decimal("0.19")).quantize(Decimal("0.01"))
        tot_l       = linea_neta.quantize(Decimal("0.01"))  # [D-05] IVA a nivel pedido

        subtotal += linea_bruta
        items_data.append({
            "variante_id": v["variante_id"],
            "nombre_snap": v["nombre_snap"],
            "sku":         v["sku"],
            "cantidad":    cant,
            "precio":      precio,
            "descuento":   desc_l,
            "impuesto":    imp_l,
            "total":       tot_l,
        })

    # descuento global del pedido (~25% probabilidad)
    desc_pct       = Decimal(str(round(random.uniform(0.03, 0.12), 2))) \
        if random.random() > 0.75 else Decimal("0")
    desc_tot       = (subtotal * desc_pct).quantize(Decimal("0.01"))
    base_imponible = subtotal - desc_tot
    # [D-05] IVA 19% una sola vez sobre base del pedido
    imp_tot        = (base_imponible * Decimal("0.19")).quantize(Decimal("0.01"))
    costo_envio    = Decimal("0") if subtotal > Decimal("50000") \
        else Decimal(str(round(random.uniform(2990, 7990), 0)))
    total_final    = (base_imponible + imp_tot + costo_envio).quantize(Decimal("0.01"))

    return items_data, subtotal, desc_tot, imp_tot, costo_envio, total_final


async def _insert_pedido(conn, cli_id, dir_id, canal, items_data,
                         subtotal, desc_tot, imp_tot, costo_envio, total_final,
                         carrito_id=None):
    """
    Crea el pedido, sus detalles y movimientos de reserva.
    [FIX-V5-04] Uso de _SCHEMA_TIENE_CARRITO_ID para decidir si incluir carrito_id.
    """
    global _SCHEMA_TIENE_CARRITO_ID
    numero = f"PED-{datetime.now().strftime('%Y%m')}-{_rand_str(6)}"

    if carrito_id is not None and _SCHEMA_TIENE_CARRITO_ID:
        ped_id = await conn.fetchval("""
            INSERT INTO pedidos (
                numero_pedido, cliente_id, direccion_facturacion_id, direccion_envio_id,
                canal_venta, codigo_moneda, estado_actual,
                subtotal, descuento_total, impuesto_total, costo_envio, total_final,
                estado_pago, estado_fulfillment, carrito_id
            ) VALUES ($1,$2,$3,$4,$5,'CLP','pendiente',$6,$7,$8,$9,$10,'pendiente','pendiente',$11)
            RETURNING pedido_id
        """, numero, cli_id, dir_id, dir_id, canal,
            subtotal, desc_tot, imp_tot, costo_envio, total_final, carrito_id)
    else:
        ped_id = await conn.fetchval("""
            INSERT INTO pedidos (
                numero_pedido, cliente_id, direccion_facturacion_id, direccion_envio_id,
                canal_venta, codigo_moneda, estado_actual,
                subtotal, descuento_total, impuesto_total, costo_envio, total_final,
                estado_pago, estado_fulfillment
            ) VALUES ($1,$2,$3,$4,$5,'CLP','pendiente',$6,$7,$8,$9,$10,'pendiente','pendiente')
            RETURNING pedido_id
        """, numero, cli_id, dir_id, dir_id, canal,
            subtotal, desc_tot, imp_tot, costo_envio, total_final)

    await conn.execute("""
        INSERT INTO historial_estado_pedidos
            (pedido_id, estado_anterior, estado_nuevo, cambiado_por)
        VALUES ($1, NULL, 'pendiente', 'simulator')
    """, ped_id)

    for it in items_data:
        alm_row = await conn.fetchrow("""
            SELECT almacen_id FROM niveles_stock
            WHERE variante_id = $1 AND cantidad_disponible >= $2
            ORDER BY cantidad_disponible DESC LIMIT 1
        """, it["variante_id"], it["cantidad"])
        alm_id = alm_row["almacen_id"] if alm_row else None

        if alm_id:
            updated = await conn.fetchval("""
                UPDATE niveles_stock
                SET cantidad_reservada = cantidad_reservada + $1, actualizado_en = NOW()
                WHERE almacen_id = $2 AND variante_id = $3
                  AND cantidad_disponible >= $1
                RETURNING almacen_id
            """, it["cantidad"], alm_id, it["variante_id"])
            if not updated:
                alm_id = None

            if updated:
                mov_res_id = await conn.fetchval("""
                    INSERT INTO movimientos_inventario
                        (almacen_id, variante_id, tipo_movimiento, cantidad,
                         tipo_referencia, referencia_id)
                    VALUES ($1, $2, 'reserva', $3, 'pedido', $4)
                    RETURNING movimiento_inventario_id
                """, alm_id, it["variante_id"], it["cantidad"], ped_id)

                await oltp_emit_evento(conn, "inventario", mov_res_id, "stock_reservado", {
                    "movimiento_id": mov_res_id,
                    "almacen_id":    alm_id,
                    "variante_id":   it["variante_id"],
                    "cantidad":      it["cantidad"],
                    "tipo":          "reserva",
                    "pedido_id":     ped_id,
                    "fecha":         datetime.now(timezone.utc).isoformat(),
                })

        await conn.execute("""
            INSERT INTO detalle_pedidos (
                pedido_id, variante_id, almacen_id,
                nombre_producto_snapshot, sku_snapshot,
                cantidad, precio_unitario, descuento_linea, impuesto_linea, total_linea
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        """, ped_id, it["variante_id"], alm_id, it["nombre_snap"], it["sku"],
            it["cantidad"], it["precio"], it["descuento"], it["impuesto"], it["total"])

    return ped_id, numero


# =============================================================================
# OLTP — ACCIONES
# =============================================================================

async def accion_nuevo_pedido(conn):
    """Pedido directo sin carrito previo."""
    cli_id = await _random_id(conn, "clientes", "cliente_id", "activo = TRUE")
    if not cli_id:
        return

    dir_row = await conn.fetchrow(
        "SELECT direccion_id FROM direcciones_cliente WHERE cliente_id=$1 AND es_predeterminada=TRUE LIMIT 1",
        cli_id)
    if not dir_row:
        dir_row = await conn.fetchrow(
            "SELECT direccion_id FROM direcciones_cliente WHERE cliente_id=$1 LIMIT 1", cli_id)
    if not dir_row:
        return
    dir_id = dir_row["direccion_id"]

    var_ids = await _random_ids(conn, "variantes_producto", "variante_id", "activa = TRUE", n=5)
    if not var_ids:
        return

    items_data, subtotal, desc_tot, imp_tot, costo_envio, total_final = \
        await _build_items_data(conn, var_ids)
    if not items_data:
        return

    canal  = random.choice(CANALES)
    ped_id, numero = await _insert_pedido(
        conn, cli_id, dir_id, canal, items_data,
        subtotal, desc_tot, imp_tot, costo_envio, total_final,
    )

    await oltp_emit_evento(conn, "pedido", ped_id, "pedido_creado", {
        "pedido_id":       ped_id,
        "numero_pedido":   numero,
        "cliente_id":      cli_id,
        "total_final":     float(total_final),
        "subtotal":        float(subtotal),
        "impuesto_total":  float(imp_tot),
        "descuento_total": float(desc_tot),
        "costo_envio":     float(costo_envio),
        "canal":           canal,
        "num_items":       len(items_data),
        "creado_en":       datetime.now(timezone.utc).isoformat(),
    })
    oltp_stats["pedidos"] += 1
    log.info(f"[OLTP][PEDIDO] #{numero} — CLP ${total_final:,.0f} ({len(items_data)} ítems)")


async def accion_avanzar_estado(conn):
    """
    [FIX-V5-07] Prioriza el pedido MÁS ANTIGUO en cada estado intermedio.
    Motor de ciclo de vida ya se ocupa del grueso; esta acción es complementaria.
    """
    # Elegir estado de origen aleatorio (excluyendo terminales y el último)
    estados_origen = ["pendiente", "confirmado", "pagado", "preparando", "empaquetado"]
    est_origen = random.choice(estados_origen)

    # [FIX-V5-07] Seleccionar el pedido más antiguo en ese estado
    row = await conn.fetchrow(f"""
        SELECT pedido_id, estado_actual, total_final, cliente_id
        FROM pedidos
        WHERE estado_actual = $1
        ORDER BY creado_en ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    """, est_origen)

    if not row:
        return

    est_actual = row["estado_actual"]
    if est_actual not in ESTADOS_FLUJO:
        return
    idx = ESTADOS_FLUJO.index(est_actual)
    if idx >= len(ESTADOS_FLUJO) - 1:
        return
    est_nuevo = ESTADOS_FLUJO[idx + 1]

    await _aplicar_transicion_estado(conn, row["pedido_id"], est_actual, est_nuevo)
    oltp_stats["estados"] += 1
    log.info(f"[OLTP][ESTADO] pedido {row['pedido_id']}: {est_actual} → {est_nuevo}")


async def _aplicar_transicion_estado(conn, ped_id, est_actual, est_nuevo):
    """
    Aplica la transición de estado a un pedido, incluyendo efectos secundarios.
    Usado tanto por accion_avanzar_estado como por motor_ciclo_vida.
    [FIX-V5-03] Sin actualizado_en en envios (columna no existe en DDL).
    [FIX-V5-09] Mapa completo de estado_fulfillment.
    """
    fulfillment = ESTADO_FULFILLMENT_MAP.get(est_nuevo, "pendiente")

    await conn.execute("""
        UPDATE pedidos
        SET estado_actual=$1, estado_fulfillment=$2, actualizado_en=NOW()
        WHERE pedido_id=$3
    """, est_nuevo, fulfillment, ped_id)

    await conn.execute("""
        INSERT INTO historial_estado_pedidos
            (pedido_id, estado_anterior, estado_nuevo, cambiado_por)
        VALUES ($1, $2, $3, 'simulator')
    """, ped_id, est_actual, est_nuevo)

    if est_nuevo == "enviado":
        # [FIX-V5-03] Sin actualizado_en (columna inexistente en envios)
        # Check first to avoid inserting duplicates if there is no constraint
        envio_exists = await conn.fetchval("SELECT 1 FROM envios WHERE pedido_id=$1", ped_id)
        if not envio_exists:
            alm_id = random.randint(1, 3)
            await conn.execute("""
                INSERT INTO envios
                    (pedido_id, almacen_id, estado_envio, transportista,
                     numero_seguimiento, enviado_en)
                VALUES ($1, $2, 'en_camino', $3, $4, NOW())
            """, ped_id, alm_id,
                random.choice(TRANSPORTISTAS), f"TRACK-{_rand_str(10)}")

    if est_nuevo == "entregado":
        await conn.execute("""
            UPDATE pedidos
            SET entregado_en=NOW()
            WHERE pedido_id=$1
        """, ped_id)
        # [FIX-V5-03] Sin actualizado_en en envios
        await conn.execute("""
            UPDATE envios
            SET estado_envio='entregado', entregado_en=NOW()
            WHERE pedido_id=$1 AND estado_envio='en_camino'
        """, ped_id)

        envio_row = await conn.fetchrow(
            "SELECT envio_id, enviado_en FROM envios WHERE pedido_id=$1 LIMIT 1", ped_id)

        # Movimientos de stock: descuento físico al entregar
        detalles = await conn.fetch(
            "SELECT variante_id, cantidad, almacen_id FROM detalle_pedidos WHERE pedido_id=$1",
            ped_id)
        for det in detalles:
            alm_id = det["almacen_id"]
            if not alm_id:
                alm_row = await conn.fetchrow("""
                    SELECT almacen_id FROM niveles_stock
                    WHERE variante_id = $1 AND cantidad_reservada >= $2
                    ORDER BY cantidad_reservada DESC LIMIT 1
                """, det["variante_id"], det["cantidad"])
                alm_id = alm_row["almacen_id"] if alm_row else 1

            await conn.execute("""
                UPDATE niveles_stock
                SET cantidad_fisica    = GREATEST(cantidad_fisica - $1, 0),
                    cantidad_reservada = GREATEST(cantidad_reservada - $1, 0),
                    actualizado_en     = NOW()
                WHERE almacen_id = $2 AND variante_id = $3
            """, det["cantidad"], alm_id, det["variante_id"])

            mov_id = await conn.fetchval("""
                INSERT INTO movimientos_inventario
                    (almacen_id, variante_id, tipo_movimiento, cantidad,
                     tipo_referencia, referencia_id)
                VALUES ($1, $2, 'venta', $3, 'pedido', $4)
                RETURNING movimiento_inventario_id
            """, alm_id, det["variante_id"], -det["cantidad"], ped_id)

            await oltp_emit_evento(conn, "inventario", mov_id, "stock_vendido", {
                "movimiento_id": mov_id,
                "almacen_id":    alm_id,
                "variante_id":   det["variante_id"],
                "cantidad":      det["cantidad"],
                "tipo":          "venta",
                "pedido_id":     ped_id,
                "fecha":         datetime.now(timezone.utc).isoformat(),
            })

        if envio_row:
            await oltp_emit_evento(conn, "envio", envio_row["envio_id"], "envio_entregado", {
                "envio_id":  envio_row["envio_id"],
                "pedido_id": ped_id,
            })

    await oltp_emit_evento(conn, "pedido", ped_id, "pedido_actualizado", {
        "pedido_id":       ped_id,
        "estado_anterior": est_actual,
        "estado_nuevo":    est_nuevo,
    })


async def accion_procesar_pago(conn):
    """Múltiples intentos de pago por pedido. 92% tasa de éxito."""
    ped_id = await _random_id(
        conn, "pedidos", "pedido_id",
        "estado_actual IN ('confirmado','pendiente') AND total_final > 0",
    )
    if not ped_id:
        return

    row = await conn.fetchrow(
        "SELECT pedido_id, total_final, cliente_id FROM pedidos WHERE pedido_id=$1 FOR UPDATE SKIP LOCKED",
        ped_id)
    if not row:
        return

    # Validar que aún esté en estado válido para pago (puede haber cambiado)
    est = await conn.fetchval("SELECT estado_actual FROM pedidos WHERE pedido_id=$1", ped_id)
    if est not in ("pendiente", "confirmado"):
        return

    monto     = float(row["total_final"])
    metodo    = random.choice(METODOS_PAGO)
    proveedor = random.choice(PROV_PAGO)
    exito     = random.random() > 0.08

    estado_pago = "capturado" if exito else "fallido"
    pago_id = await conn.fetchval("""
        INSERT INTO pagos
            (pedido_id, proveedor_pago, transaccion_externa_id,
             metodo_pago, codigo_moneda, monto, estado)
        VALUES ($1, $2, $3, $4, 'CLP', $5, $6)
        RETURNING pago_id
    """, ped_id, proveedor, f"TXN-{_rand_str(12)}", metodo, monto, estado_pago)

    for tipo_ev in ["iniciado", "autorizado"]:
        await conn.execute("""
            INSERT INTO eventos_pago (pago_id, tipo_evento, estado_evento, monto_evento)
            VALUES ($1, $2, 'ok', $3)
        """, pago_id, tipo_ev, monto)

    if exito:
        await conn.execute("""
            INSERT INTO eventos_pago (pago_id, tipo_evento, estado_evento, monto_evento)
            VALUES ($1, 'capturado', 'ok', $2)
        """, pago_id, monto)
        await conn.execute("""
            UPDATE pedidos
            SET estado_pago='pagado', estado_actual='pagado',
                estado_fulfillment='pendiente', actualizado_en=NOW()
            WHERE pedido_id=$1
        """, ped_id)
        await conn.execute("""
            INSERT INTO historial_estado_pedidos
                (pedido_id, estado_anterior, estado_nuevo, cambiado_por)
            VALUES ($1, $2, 'pagado', 'simulator')
        """, ped_id, est)
        await oltp_emit_evento(conn, "pago", pago_id, "pago_capturado", {
            "pago_id":    pago_id,
            "pedido_id":  ped_id,
            "monto":      monto,
            "metodo":     metodo,
            "proveedor":  proveedor,
            "cliente_id": row["cliente_id"],
            "creado_en":  datetime.now(timezone.utc).isoformat(),
        })
        log.info(f"[OLTP][PAGO] pedido {ped_id} — {metodo} CLP ${monto:,.0f} ✓")
    else:
        await conn.execute("""
            INSERT INTO eventos_pago (pago_id, tipo_evento, estado_evento, monto_evento)
            VALUES ($1, 'fallido', 'error', $2)
        """, pago_id, monto)
        await oltp_emit_evento(conn, "pago", pago_id, "pago_fallido", {
            "pago_id":    pago_id,
            "pedido_id":  ped_id,
            "monto":      monto,
            "metodo":     metodo,
            "proveedor":  proveedor,
            "cliente_id": row["cliente_id"],
        })
        log.warning(f"[OLTP][PAGO] pedido {ped_id} — FALLIDO via {proveedor}")
    oltp_stats["pagos"] += 1


async def accion_cancelar_pedido(conn):
    """Cancelación. Solo pedidos en 'pendiente' o 'confirmado' (pre-pago)."""
    ped_id = await _random_id(
        conn, "pedidos", "pedido_id",
        "estado_actual IN ('pendiente','confirmado')",
    )
    if not ped_id:
        return

    row = await conn.fetchrow("""
        SELECT pedido_id, estado_actual
        FROM pedidos WHERE pedido_id = $1 FOR UPDATE SKIP LOCKED
    """, ped_id)
    if not row or row["estado_actual"] not in ("pendiente", "confirmado"):
        return

    detalles = await conn.fetch(
        "SELECT variante_id, cantidad, almacen_id FROM detalle_pedidos WHERE pedido_id=$1",
        ped_id)
    for det in detalles:
        alm_id = det["almacen_id"]
        if alm_id:
            await conn.execute("""
                UPDATE niveles_stock
                SET cantidad_reservada = GREATEST(cantidad_reservada - $1, 0),
                    actualizado_en = NOW()
                WHERE almacen_id = $2 AND variante_id = $3
            """, det["cantidad"], alm_id, det["variante_id"])

    pago_cap = await conn.fetchrow("""
        SELECT pago_id, monto FROM pagos
        WHERE pedido_id = $1 AND estado = 'capturado'
        ORDER BY creado_en DESC LIMIT 1
    """, ped_id)
    if pago_cap:
        await conn.execute("""
            INSERT INTO reembolsos (pago_id, pedido_id, monto, codigo_motivo, estado)
            VALUES ($1, $2, $3, 'cancelacion_cliente', 'pendiente')
        """, pago_cap["pago_id"], ped_id, pago_cap["monto"])

    await conn.execute("""
        UPDATE pedidos
        SET estado_actual='cancelado', cancelado_en=NOW(), actualizado_en=NOW()
        WHERE pedido_id=$1
    """, ped_id)
    await conn.execute("""
        INSERT INTO historial_estado_pedidos
            (pedido_id, estado_anterior, estado_nuevo, cambiado_por, codigo_motivo)
        VALUES ($1, $2, 'cancelado', 'simulator', 'cliente_solicitud')
    """, ped_id, row["estado_actual"])

    await oltp_emit_evento(conn, "pedido", ped_id, "pedido_cancelado", {
        "pedido_id":  ped_id,
        "motivo":     "cliente_solicitud",
        "habia_pago": pago_cap is not None,
    })
    oltp_stats["cancelaciones"] += 1
    log.info(f"[OLTP][CANCEL] pedido {ped_id} cancelado")


async def accion_reponer_stock(conn):
    """OC con proveedor real. Alternancia 'ingreso'/'reposicion' para BI Q9."""
    row = await conn.fetchrow("""
        SELECT ns.almacen_id, ns.variante_id, vp.proveedor_id
        FROM niveles_stock ns
        JOIN variantes_producto vp ON vp.variante_id = ns.variante_id
        WHERE ns.cantidad_disponible <= ns.punto_reorden
          AND vp.proveedor_id IS NOT NULL
        ORDER BY ns.cantidad_disponible ASC LIMIT 1
    """)
    if not row:
        ns_id = await _random_id(conn, "niveles_stock", "nivel_stock_id")
        if ns_id:
            row = await conn.fetchrow("""
                SELECT ns.almacen_id, ns.variante_id, vp.proveedor_id
                FROM niveles_stock ns
                JOIN variantes_producto vp ON vp.variante_id = ns.variante_id
                WHERE ns.nivel_stock_id = $1
                  AND vp.proveedor_id IS NOT NULL
            """, ns_id)
    if not row:
        return

    alm_id  = row["almacen_id"]
    var_id  = row["variante_id"]
    prov_id = row["proveedor_id"]
    # [FIX-V5-10] cantidad con más variabilidad
    cant    = random.randint(15, 150)

    precio_costo = await conn.fetchval(
        "SELECT precio_costo FROM variantes_producto WHERE variante_id=$1", var_id)
    precio_costo = float(precio_costo or 0)
    subtotal_oc  = round(precio_costo * cant, 2)
    iva_oc       = round(subtotal_oc * 0.19, 2)
    total_oc     = round(subtotal_oc + iva_oc, 2)

    numero_oc = f"OC-{datetime.now().strftime('%Y%m')}-{_rand_str(6)}"
    oc_id = await conn.fetchval("""
        INSERT INTO ordenes_compra
            (proveedor_id, almacen_id, numero_oc, estado,
             subtotal, impuesto_total, total_oc)
        VALUES ($1, $2, $3, 'recibida_total', $4, $5, $6)
        RETURNING orden_compra_id
    """, prov_id, alm_id, numero_oc, subtotal_oc, iva_oc, total_oc)

    await conn.execute("""
        INSERT INTO detalle_ordenes_compra
            (orden_compra_id, variante_id, cantidad_solicitada, cantidad_recibida,
             precio_unitario_compra, total_linea)
        VALUES ($1, $2, $3, $3, $4, $5)
    """, oc_id, var_id, cant, precio_costo, subtotal_oc)

    await conn.execute("""
        UPDATE niveles_stock
        SET cantidad_fisica = cantidad_fisica + $1, actualizado_en = NOW()
        WHERE almacen_id = $2 AND variante_id = $3
    """, cant, alm_id, var_id)

    tipo_mov = random.choice(["ingreso", "reposicion"])
    mov_id = await conn.fetchval("""
        INSERT INTO movimientos_inventario
            (almacen_id, variante_id, tipo_movimiento, cantidad,
             tipo_referencia, referencia_id)
        VALUES ($1, $2, $3, $4, 'orden_compra', $5)
        RETURNING movimiento_inventario_id
    """, alm_id, var_id, tipo_mov, cant, oc_id)

    await oltp_emit_evento(conn, "inventario", mov_id, "stock_repuesto", {
        "movimiento_id":   mov_id,
        "orden_compra_id": oc_id,
        "proveedor_id":    prov_id,
        "almacen_id":      alm_id,
        "variante_id":     var_id,
        "cantidad":        cant,
        "tipo":            tipo_mov,
        "fecha":           datetime.now(timezone.utc).isoformat(),
    })
    oltp_stats["reposiciones"] += 1
    log.info(f"[OLTP][STOCK] variante {var_id} +{cant} ({tipo_mov}) via OC {numero_oc}")


async def accion_devolucion(conn):
    """
    [FIX-V5-03] Sin actualizado_en en envios (columna no existe).
    [D-04] Una sola devolución activa por pedido.
    """
    ped_id = await _random_id(conn, "pedidos", "pedido_id", "estado_actual = 'entregado'")
    if not ped_id:
        return

    row = await conn.fetchrow("""
        SELECT p.pedido_id, dp.detalle_pedido_id, dp.variante_id,
               dp.cantidad, dp.almacen_id, p.cliente_id
        FROM pedidos p
        JOIN detalle_pedidos dp ON dp.pedido_id = p.pedido_id
        WHERE p.pedido_id = $1
        LIMIT 1
    """, ped_id)
    if not row:
        return

    dev_exists = await conn.fetchval(
        "SELECT 1 FROM devoluciones WHERE pedido_id=$1 AND estado != 'rechazada'", ped_id)
    if dev_exists:
        return

    dev_id = await conn.fetchval("""
        INSERT INTO devoluciones (pedido_id, motivo, estado)
        VALUES ($1, $2, 'aprobada')
        RETURNING devolucion_id
    """, row["pedido_id"], random.choice(MOTIVOS_DEV))

    cant_dev = random.randint(1, max(1, row["cantidad"]))
    await conn.execute("""
        INSERT INTO detalle_devoluciones
            (devolucion_id, detalle_pedido_id, variante_id, cantidad_devuelta)
        VALUES ($1, $2, $3, $4)
    """, dev_id, row["detalle_pedido_id"], row["variante_id"], cant_dev)

    alm_id = row["almacen_id"] or random.randint(1, 3)
    await conn.execute("""
        UPDATE niveles_stock
        SET cantidad_fisica = cantidad_fisica + $1, actualizado_en = NOW()
        WHERE almacen_id = $2 AND variante_id = $3
    """, cant_dev, alm_id, row["variante_id"])

    await conn.execute("""
        INSERT INTO movimientos_inventario
            (almacen_id, variante_id, tipo_movimiento, cantidad,
             tipo_referencia, referencia_id)
        VALUES ($1, $2, 'devolucion', $3, 'devolucion', $4)
    """, alm_id, row["variante_id"], cant_dev, dev_id)

    # [FIX-V5-03] Sin actualizado_en — columna no existe en envios
    envio_row = await conn.fetchrow(
        "SELECT envio_id FROM envios WHERE pedido_id=$1 LIMIT 1", ped_id)
    if envio_row:
        await conn.execute("""
            UPDATE envios SET estado_envio='devuelto'
            WHERE envio_id=$1
        """, envio_row["envio_id"])
        await oltp_emit_evento(conn, "envio", envio_row["envio_id"], "envio_devuelto", {
            "envio_id":      envio_row["envio_id"],
            "pedido_id":     ped_id,
            "devolucion_id": dev_id,
        })

    await oltp_emit_evento(conn, "devolucion", dev_id, "devolucion_creada", {
        "devolucion_id":     dev_id,
        "pedido_id":         row["pedido_id"],
        "detalle_pedido_id": row["detalle_pedido_id"],
        "variante_id":       row["variante_id"],
        "cantidad_devuelta": cant_dev,
        "cliente_id":        row["cliente_id"],
        "creado_en":         datetime.now(timezone.utc).isoformat(),
    })
    oltp_stats["devoluciones"] += 1
    log.info(f"[OLTP][DEV] pedido {row['pedido_id']} — {cant_dev} unidades devueltas")


async def accion_carrito(conn):
    """
    [FIX-V5-05] Probabilidad aumentada a 15%.
    Flujo: carrito activo → convertido (70%) con pedido vinculado, o abandonado (30%).
    """
    cli_id = await _random_id(conn, "clientes", "cliente_id", "activo = TRUE")
    if not cli_id:
        return

    cart_id = await conn.fetchval("""
        INSERT INTO carritos (cliente_id, estado, codigo_moneda)
        VALUES ($1, 'activo', 'CLP')
        RETURNING carrito_id
    """, cli_id)

    var_ids = await _random_ids(conn, "variantes_producto", "variante_id", "activa = TRUE", n=5)
    valor_total = Decimal("0")

    if var_ids:
        variantes = await conn.fetch("""
            SELECT vp.variante_id, pp.precio_lista
            FROM variantes_producto vp
            JOIN precios_producto pp ON pp.variante_id = vp.variante_id
            WHERE vp.variante_id = ANY($1::bigint[]) AND vp.activa = TRUE
            LIMIT 4
        """, var_ids)

        for v in variantes:
            cant = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
            await conn.execute("""
                INSERT INTO detalle_carritos
                    (carrito_id, variante_id, cantidad, precio_unitario)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT DO NOTHING
            """, cart_id, v["variante_id"], cant, v["precio_lista"])
            valor_total += Decimal(str(v["precio_lista"])) * cant

    se_convierte = random.random() < PROB_CARRITO_CONVERSION

    if se_convierte:
        dir_row = await conn.fetchrow(
            "SELECT direccion_id FROM direcciones_cliente WHERE cliente_id=$1 AND es_predeterminada=TRUE LIMIT 1",
            cli_id)
        if not dir_row:
            dir_row = await conn.fetchrow(
                "SELECT direccion_id FROM direcciones_cliente WHERE cliente_id=$1 LIMIT 1", cli_id)

        if dir_row and var_ids:
            dir_id = dir_row["direccion_id"]
            items_data, subtotal, desc_tot, imp_tot, costo_envio, total_final = \
                await _build_items_data(conn, var_ids)

            if items_data:
                canal = random.choice(CANALES)
                ped_id, numero = await _insert_pedido(
                    conn, cli_id, dir_id, canal, items_data,
                    subtotal, desc_tot, imp_tot, costo_envio, total_final,
                    carrito_id=cart_id,
                )

                await conn.execute("""
                    UPDATE carritos SET estado='convertido', actualizado_en=NOW()
                    WHERE carrito_id=$1
                """, cart_id)

                await oltp_emit_evento(conn, "carrito", cart_id, "carrito_convertido", {
                    "carrito_id":  cart_id,
                    "cliente_id":  cli_id,
                    "pedido_id":   ped_id,
                    "valor_total": float(valor_total),
                    "creado_en":   datetime.now(timezone.utc).isoformat(),
                })

                await oltp_emit_evento(conn, "pedido", ped_id, "pedido_creado", {
                    "pedido_id":       ped_id,
                    "numero_pedido":   numero,
                    "cliente_id":      cli_id,
                    "total_final":     float(total_final),
                    "subtotal":        float(subtotal),
                    "impuesto_total":  float(imp_tot),
                    "descuento_total": float(desc_tot),
                    "costo_envio":     float(costo_envio),
                    "canal":           canal,
                    "num_items":       len(items_data),
                    "carrito_id":      cart_id,
                    "creado_en":       datetime.now(timezone.utc).isoformat(),
                })

                oltp_stats["carritos_convertidos"] += 1
                oltp_stats["pedidos"] += 1
                log.info(f"[OLTP][CARRITO] carrito {cart_id} CONVERTIDO → pedido #{numero}")
                return

    # Abandonado
    await conn.execute("""
        UPDATE carritos SET estado='abandonado', actualizado_en=NOW()
        WHERE carrito_id=$1
    """, cart_id)

    await oltp_emit_evento(conn, "carrito", cart_id, "carrito_abandonado", {
        "carrito_id":  cart_id,
        "cliente_id":  cli_id,
        "valor_total": float(valor_total),
        "creado_en":   datetime.now(timezone.utc).isoformat(),
    })
    oltp_stats["carritos_abandonados"] += 1
    log.info(f"[OLTP][CARRITO] carrito {cart_id} ABANDONADO (CLP ${valor_total:,.0f})")


async def accion_actualizar_envio(conn):
    """
    Marca envíos 'en_camino' como entregados.
    [FIX-V5-03] Sin actualizado_en en envios.
    """
    envio_id = await _random_id(conn, "envios", "envio_id", "estado_envio = 'en_camino'")
    if not envio_id:
        return

    row = await conn.fetchrow(
        "SELECT envio_id, pedido_id, estado_envio, enviado_en FROM envios WHERE envio_id=$1 FOR UPDATE SKIP LOCKED",
        envio_id)
    if not row or row["estado_envio"] != "en_camino":
        return

    # [FIX-V5-03] Sin actualizado_en
    await conn.execute(
        "UPDATE envios SET estado_envio='entregado', entregado_en=NOW() WHERE envio_id=$1",
        row["envio_id"])

    ped_estado = await conn.fetchval(
        "SELECT estado_actual FROM pedidos WHERE pedido_id=$1", row["pedido_id"])
    if ped_estado != "entregado":
        await conn.execute("""
            UPDATE pedidos
            SET estado_actual='entregado', estado_fulfillment='entregado',
                entregado_en=NOW(), actualizado_en=NOW()
            WHERE pedido_id=$1
        """, row["pedido_id"])
        await conn.execute("""
            INSERT INTO historial_estado_pedidos
                (pedido_id, estado_anterior, estado_nuevo, cambiado_por)
            VALUES ($1, $2, 'entregado', 'simulator')
        """, row["pedido_id"], ped_estado)

    await oltp_emit_evento(conn, "envio", row["envio_id"], "envio_entregado", {
        "envio_id":  row["envio_id"],
        "pedido_id": row["pedido_id"],
    })
    oltp_stats["envios"] += 1
    log.info(f"[OLTP][ENVIO] envío {row['envio_id']} entregado")


OLTP_ACCIONES = {
    "nuevo_pedido":     accion_nuevo_pedido,
    "avanzar_estado":   accion_avanzar_estado,
    "procesar_pago":    accion_procesar_pago,
    "cancelar_pedido":  accion_cancelar_pedido,
    "reponer_stock":    accion_reponer_stock,
    "devolucion":       accion_devolucion,
    "carrito":          accion_carrito,
    "actualizar_envio": accion_actualizar_envio,
}


def elegir_accion():
    return random.choices(list(PROB.keys()), weights=list(PROB.values()), k=1)[0]


async def oltp_get_adaptive_interval(conn):
    total = await conn.fetchval("SELECT COUNT(*) FROM pedidos")
    if total > OLTP_MAX_PEDIDOS:
        return -1
    if total > 80000:
        return random.uniform(15, 25)
    elif total > 50000:
        return random.uniform(10, 18)
    elif total > 20000:
        return random.uniform(8, 14)
    return random.uniform(OLTP_INTERVAL_MIN, OLTP_INTERVAL_MAX)


# =============================================================================
# MOTOR DE CICLO DE VIDA — [FIX-V5-01] NUEVO EN v5
#
# Responsable de avanzar pedidos deterministicamente por la cadena completa.
# Opera independientemente del ciclo OLTP aleatorio.
# Garantiza que TODAS las facts se poblen (envíos, devoluciones, etc.).
# =============================================================================

async def _cv_batch_avanzar(pool, estado_origen, estado_destino, batch=None, extra_check=""):
    """
    Avanza un batch de pedidos de estado_origen → estado_destino.
    Retorna el número de pedidos avanzados.
    """
    if batch is None:
        batch = CV_BATCH_PER_STATE

    avanzados = 0
    # Seleccionar los pedidos más antiguos en el estado origen (con edad mínima)
    async with pool.acquire() as conn:
        pedidos = await conn.fetch(f"""
            SELECT pedido_id, estado_actual
            FROM pedidos
            WHERE estado_actual = $1
              AND (NOW() - actualizado_en) >= INTERVAL '{CV_MIN_AGE_SECONDS} seconds'
              {extra_check}
            ORDER BY actualizado_en ASC
            LIMIT $2
        """, estado_origen, batch)

    for ped_row in pedidos:
        if not running:
            break
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # Re-verificar estado dentro de la transacción (SKIP LOCKED)
                    row = await conn.fetchrow("""
                        SELECT pedido_id, estado_actual
                        FROM pedidos WHERE pedido_id=$1 AND estado_actual=$2
                        FOR UPDATE SKIP LOCKED
                    """, ped_row["pedido_id"], estado_origen)
                    if not row:
                        continue
                    await _aplicar_transicion_estado(
                        conn, row["pedido_id"], estado_origen, estado_destino)
            avanzados += 1
            oltp_stats["cv_avances"] += 1
        except Exception as e:
            log.debug(f"[CV] pedido {ped_row['pedido_id']} ({estado_origen}→{estado_destino}): {e}")

    return avanzados


async def _cv_pagar_pedidos(pool):
    """
    Procesa el pago de pedidos en estado 'confirmado' o 'pendiente' con edad suficiente.
    Simula pago exitoso (95% éxito en ciclo_vida — los fallos aleatorios vienen del OLTP).
    """
    async with pool.acquire() as conn:
        pedidos = await conn.fetch(f"""
            SELECT pedido_id, total_final, cliente_id, estado_actual
            FROM pedidos
            WHERE estado_actual IN ('pendiente', 'confirmado')
              AND (NOW() - actualizado_en) >= INTERVAL '{CV_MIN_AGE_SECONDS} seconds'
              AND estado_pago != 'pagado'
            ORDER BY actualizado_en ASC
            LIMIT {CV_BATCH_PER_STATE}
        """)

    for ped_row in pedidos:
        if not running:
            break
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow("""
                        SELECT pedido_id, total_final, cliente_id, estado_actual
                        FROM pedidos WHERE pedido_id=$1
                          AND estado_actual IN ('pendiente','confirmado')
                          AND estado_pago != 'pagado'
                        FOR UPDATE SKIP LOCKED
                    """, ped_row["pedido_id"])
                    if not row:
                        continue

                    metodo    = random.choice(METODOS_PAGO)
                    proveedor = random.choice(PROV_PAGO)
                    monto     = float(row["total_final"])
                    exito     = random.random() > 0.05  # 95% éxito en ciclo_vida

                    estado_pago_str = "capturado" if exito else "fallido"
                    pago_id = await conn.fetchval("""
                        INSERT INTO pagos
                            (pedido_id, proveedor_pago, transaccion_externa_id,
                             metodo_pago, codigo_moneda, monto, estado)
                        VALUES ($1,$2,$3,$4,'CLP',$5,$6)
                        RETURNING pago_id
                    """, row["pedido_id"], proveedor, f"TXN-CV-{_rand_str(10)}",
                        metodo, monto, estado_pago_str)

                    for tipo_ev in ["iniciado", "autorizado"]:
                        await conn.execute("""
                            INSERT INTO eventos_pago (pago_id, tipo_evento, estado_evento, monto_evento)
                            VALUES ($1,$2,'ok',$3)
                        """, pago_id, tipo_ev, monto)

                    if exito:
                        await conn.execute("""
                            INSERT INTO eventos_pago (pago_id, tipo_evento, estado_evento, monto_evento)
                            VALUES ($1,'capturado','ok',$2)
                        """, pago_id, monto)
                        await conn.execute("""
                            UPDATE pedidos
                            SET estado_pago='pagado', estado_actual='pagado',
                                estado_fulfillment='pendiente', actualizado_en=NOW()
                            WHERE pedido_id=$1
                        """, row["pedido_id"])
                        await conn.execute("""
                            INSERT INTO historial_estado_pedidos
                                (pedido_id, estado_anterior, estado_nuevo, cambiado_por)
                            VALUES ($1,$2,'pagado','ciclo_vida')
                        """, row["pedido_id"], row["estado_actual"])

                        await oltp_emit_evento(conn, "pago", pago_id, "pago_capturado", {
                            "pago_id":    pago_id,
                            "pedido_id":  row["pedido_id"],
                            "monto":      monto,
                            "metodo":     metodo,
                            "proveedor":  proveedor,
                            "cliente_id": row["cliente_id"],
                            "creado_en":  datetime.now(timezone.utc).isoformat(),
                        })
                        await oltp_emit_evento(conn, "pedido", row["pedido_id"], "pedido_actualizado", {
                            "pedido_id":    row["pedido_id"],
                            "estado_anterior": row["estado_actual"],
                            "estado_nuevo":    "pagado",
                        })
                        oltp_stats["cv_avances"] += 1

        except Exception as e:
            log.debug(f"[CV][PAGO] pedido {ped_row['pedido_id']}: {e}")


async def ciclo_vida_tick(pool):
    """
    [FIX-V5-01] Un ciclo completo del motor de ciclo de vida.
    Avanza pedidos en cada estado intermedio hacia el siguiente.
    Orden de procesamiento: de estados más avanzados a menos avanzados
    para evitar que un pedido avance dos veces en el mismo tick.
    """
    # Avanzar desde estados más avanzados primero (evita doble-avance en el mismo tick)

    # empaquetado → enviado (crea envío)
    n = await _cv_batch_avanzar(pool, "empaquetado", "enviado")
    if n:
        log.info(f"[CV] empaquetado→enviado: {n} pedidos")

    # preparando → empaquetado
    n = await _cv_batch_avanzar(pool, "preparando", "empaquetado")
    if n:
        log.info(f"[CV] preparando→empaquetado: {n} pedidos")

    # pagado → preparando
    n = await _cv_batch_avanzar(pool, "pagado", "preparando")
    if n:
        log.info(f"[CV] pagado→preparando: {n} pedidos")

    # confirmado → pagar (a través de función especial)
    await _cv_pagar_pedidos(pool)

    # pendiente → confirmado
    n = await _cv_batch_avanzar(pool, "pendiente", "confirmado")
    if n:
        log.info(f"[CV] pendiente→confirmado: {n} pedidos")

    # enviado → entregado (con delay más largo para simular tránsito)
    n = await _cv_batch_avanzar(
        pool, "enviado", "entregado",
        extra_check=f"AND (NOW() - actualizado_en) >= INTERVAL '{max(CV_MIN_AGE_SECONDS * 2, 60)} seconds'"
    )
    if n:
        log.info(f"[CV] enviado→entregado: {n} pedidos")


async def motor_ciclo_vida(pool):
    """
    [FIX-V5-01] Motor dedicado al avance determinístico de pedidos.
    Garantiza que todos los pedidos recorran el ciclo completo y que
    fact_envios, fact_devoluciones y otras facts se pueblen correctamente.
    """
    log.info("[CV] Motor de ciclo de vida iniciado.")
    while running:
        try:
            await ciclo_vida_tick(pool)
        except Exception as e:
            log.error(f"[CV][ERR] ciclo_vida_tick: {e}")
        # [FIX-V5-10] Jitter en el intervalo para evitar thundering herd
        jitter = random.uniform(-1.0, 1.5)
        await asyncio.sleep(max(0.1, CV_INTERVAL + jitter))


# =============================================================================
# OLAP — DIMENSIONES (SCD Tipo 2)
# =============================================================================

async def _scd2_cerrar_version(conn, tabla, id_col, id_val):
    await conn.execute(f"""
        UPDATE warehouse.{tabla}
        SET valido_hasta = NOW(), es_actual = FALSE
        WHERE {id_col} = $1 AND es_actual = TRUE
    """, id_val)


async def olap_ensure_dim_cliente(conn, cliente_id):
    row = await conn.fetchrow("""
        SELECT c.cliente_id,
               c.correo,
               c.nombre || ' ' || c.apellido     AS nombre_completo,
               c.segmento,
               c.estado,
               COALESCE(d.codigo_pais, 'CL')     AS codigo_pais,
               COALESCE(d.ciudad, 'Santiago')     AS ciudad,
               COALESCE(d.provincia, 'Metropolitana') AS region
        FROM clientes c
        LEFT JOIN direcciones_cliente d
               ON d.cliente_id = c.cliente_id AND d.es_predeterminada = TRUE
        WHERE c.cliente_id = $1
    """, cliente_id)
    if not row:
        return None

    dim_actual = await conn.fetchrow("""
        SELECT cliente_sk, segmento, estado, ciudad, region
        FROM warehouse.dim_cliente
        WHERE cliente_id = $1 AND es_actual = TRUE
        FOR UPDATE
    """, cliente_id)

    if dim_actual:
        cambio = (
            dim_actual["segmento"] != row["segmento"] or
            dim_actual["estado"]   != row["estado"]   or
            dim_actual["ciudad"]   != row["ciudad"]   or
            dim_actual["region"]   != row["region"]
        )
        if not cambio:
            return dim_actual["cliente_sk"]
        await _scd2_cerrar_version(conn, "dim_cliente", "cliente_id", cliente_id)

    sk = await conn.fetchval("""
        INSERT INTO warehouse.dim_cliente
            (cliente_id, correo, nombre_completo, segmento, estado,
             codigo_pais, ciudad, region, valido_desde, valido_hasta, es_actual)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8, NOW(), NULL, TRUE)
        RETURNING cliente_sk
    """, row["cliente_id"], row["correo"], row["nombre_completo"],
        row["segmento"], row["estado"],
        row["codigo_pais"], row["ciudad"], row["region"])
    olap_stats["dim_upserts"] += 1
    return sk


async def olap_ensure_dim_producto(conn, producto_id):
    row = await conn.fetchrow("""
        SELECT p.producto_id,
               p.nombre                       AS nombre_producto,
               m.nombre                       AS nombre_marca,
               c.nombre                       AS nombre_categoria,
               COALESCE(cp.nombre, c.nombre)  AS categoria_padre,
               p.estado_producto
        FROM productos p
        JOIN marcas     m  ON m.marca_id     = p.marca_id
        JOIN categorias c  ON c.categoria_id = p.categoria_id
        LEFT JOIN categorias cp ON cp.categoria_id = c.categoria_padre_id
        WHERE p.producto_id = $1
    """, producto_id)
    if not row:
        return None

    dim_actual = await conn.fetchrow("""
        SELECT producto_sk, nombre_producto, estado_producto
        FROM warehouse.dim_producto
        WHERE producto_id = $1 AND es_actual = TRUE
        FOR UPDATE
    """, producto_id)

    if dim_actual:
        cambio = (
            dim_actual["nombre_producto"] != row["nombre_producto"] or
            dim_actual["estado_producto"] != row["estado_producto"]
        )
        if not cambio:
            return dim_actual["producto_sk"]
        await _scd2_cerrar_version(conn, "dim_producto", "producto_id", producto_id)

    sk = await conn.fetchval("""
        INSERT INTO warehouse.dim_producto
            (producto_id, nombre_producto, nombre_marca, nombre_categoria,
             categoria_padre, estado_producto, valido_desde, valido_hasta, es_actual)
        VALUES ($1,$2,$3,$4,$5,$6, NOW(), NULL, TRUE)
        RETURNING producto_sk
    """, row["producto_id"], row["nombre_producto"], row["nombre_marca"],
        row["nombre_categoria"], row["categoria_padre"], row["estado_producto"])
    olap_stats["dim_upserts"] += 1
    return sk


async def olap_ensure_dim_variante(conn, variante_id):
    row = await conn.fetchrow("""
        SELECT vp.variante_id, vp.producto_id, vp.sku, vp.color, vp.talla,
               COALESCE(pp.precio_lista, 0) AS precio_lista_actual,
               vp.precio_costo, vp.activa
        FROM variantes_producto vp
        LEFT JOIN precios_producto pp ON pp.variante_id = vp.variante_id
        WHERE vp.variante_id = $1
        ORDER BY pp.vigente_desde DESC NULLS LAST LIMIT 1
    """, variante_id)
    if not row:
        return None

    prod_sk = await olap_ensure_dim_producto(conn, row["producto_id"])

    dim_actual = await conn.fetchrow("""
        SELECT variante_sk, precio_lista_actual, activa
        FROM warehouse.dim_variante
        WHERE variante_id = $1 AND es_actual = TRUE
        FOR UPDATE
    """, variante_id)

    if dim_actual:
        cambio = (
            float(dim_actual["precio_lista_actual"] or 0) != float(row["precio_lista_actual"] or 0) or
            dim_actual["activa"] != row["activa"]
        )
        if not cambio:
            return dim_actual["variante_sk"]
        await _scd2_cerrar_version(conn, "dim_variante", "variante_id", variante_id)

    sk = await conn.fetchval("""
        INSERT INTO warehouse.dim_variante
            (variante_id, producto_sk, sku, color, talla,
             precio_lista_actual, precio_costo, activa,
             valido_desde, valido_hasta, es_actual)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8, NOW(), NULL, TRUE)
        RETURNING variante_sk
    """, row["variante_id"], prod_sk, row["sku"], row["color"], row["talla"],
        row["precio_lista_actual"], row["precio_costo"], row["activa"])
    olap_stats["dim_upserts"] += 1
    return sk


async def olap_ensure_dim_almacen(conn, almacen_id):
    existing = await conn.fetchval(
        "SELECT almacen_sk FROM warehouse.dim_almacen WHERE almacen_id = $1", almacen_id)
    if existing:
        return existing
    row = await conn.fetchrow(
        "SELECT almacen_id, codigo, nombre, ciudad, codigo_pais FROM almacenes WHERE almacen_id=$1",
        almacen_id)
    if not row:
        return None
    sk = await conn.fetchval("""
        INSERT INTO warehouse.dim_almacen (almacen_id, codigo, nombre, ciudad, codigo_pais)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (almacen_id) DO UPDATE
            SET nombre = EXCLUDED.nombre, ciudad = EXCLUDED.ciudad
        RETURNING almacen_sk
    """, row["almacen_id"], row["codigo"], row["nombre"], row["ciudad"], row["codigo_pais"])
    olap_stats["dim_upserts"] += 1
    return sk


async def olap_get_fecha_sk(conn, fecha):
    if isinstance(fecha, str):
        fecha = datetime.fromisoformat(fecha.replace("Z", "+00:00"))
    if hasattr(fecha, "date"):
        fecha = fecha.date()
    return await conn.fetchval(
        "SELECT fecha_sk FROM warehouse.dim_fecha WHERE fecha = $1", fecha)


# =============================================================================
# OLAP — HANDLERS DE FACTS
# =============================================================================

async def olap_process_pedido_creado(conn, data):
    """INSERT en fact_pedidos + fact_detalle_pedidos. Idempotente via UNIQUE."""
    pedido_id = data.get("pedido_id")
    if not pedido_id:
        return

    ped = await conn.fetchrow("SELECT * FROM pedidos WHERE pedido_id = $1", pedido_id)
    if not ped:
        return

    cliente_sk = await olap_ensure_dim_cliente(conn, ped["cliente_id"])
    fecha_sk   = await olap_get_fecha_sk(conn, ped["creado_en"])
    canal_sk   = await conn.fetchval(
        "SELECT canal_sk FROM warehouse.dim_canal WHERE canal = $1", ped["canal_venta"])
    estado_sk  = await conn.fetchval(
        "SELECT estado_pedido_sk FROM warehouse.dim_estado_pedido WHERE estado = $1",
        ped["estado_actual"])
    num_items  = await conn.fetchval(
        "SELECT COUNT(*) FROM detalle_pedidos WHERE pedido_id = $1", pedido_id)

    await conn.execute("""
        INSERT INTO warehouse.fact_pedidos (
            pedido_id, numero_pedido, cliente_sk, fecha_sk, canal_sk, estado_pedido_sk,
            subtotal, descuento_total, impuesto_total, costo_envio, total_final,
            num_items, estado_pago, estado_fulfillment, creado_en
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        ON CONFLICT (pedido_id) DO NOTHING
    """, pedido_id, ped["numero_pedido"], cliente_sk, fecha_sk, canal_sk, estado_sk,
        ped["subtotal"], ped["descuento_total"], ped["impuesto_total"],
        ped["costo_envio"], ped["total_final"], num_items,
        ped["estado_pago"], ped["estado_fulfillment"], ped["creado_en"])
    olap_stats["facts_inserted"] += 1

    detalles = await conn.fetch(
        "SELECT * FROM detalle_pedidos WHERE pedido_id = $1", pedido_id)
    for det in detalles:
        variante_sk = await olap_ensure_dim_variante(conn, det["variante_id"])
        prod_id     = await conn.fetchval(
            "SELECT producto_id FROM variantes_producto WHERE variante_id=$1",
            det["variante_id"])
        producto_sk = await olap_ensure_dim_producto(conn, prod_id) if prod_id else None

        await conn.execute("""
            INSERT INTO warehouse.fact_detalle_pedidos (
                detalle_pedido_id, pedido_id, variante_sk, producto_sk,
                cliente_sk, fecha_sk,
                cantidad, precio_unitario, descuento_linea, impuesto_linea,
                total_linea, creado_en
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (detalle_pedido_id) DO NOTHING
        """, det["detalle_pedido_id"], pedido_id, variante_sk, producto_sk,
            cliente_sk, fecha_sk, det["cantidad"], det["precio_unitario"],
            det["descuento_linea"], det["impuesto_linea"],
            det["total_linea"], det["creado_en"])
        olap_stats["facts_inserted"] += 1

    await conn.execute("""
        UPDATE warehouse.marcas_agua SET ultima_carga=NOW(), actualizado_en=NOW()
        WHERE tabla_fuente='pedidos'
    """)
    log.info(f"  [OLAP] fact_pedidos + {len(detalles)} detalles → pedido {pedido_id}")


async def olap_process_pedido_actualizado(conn, data):
    """
    Actualiza estado en fact_pedidos.
    [FIX-V5-09] Mapa completo de estado_fulfillment para TODOS los estados del flujo.
    """
    pedido_id    = data.get("pedido_id")
    estado_nuevo = data.get("estado_nuevo")
    if not pedido_id or not estado_nuevo:
        return

    estado_sk   = await conn.fetchval(
        "SELECT estado_pedido_sk FROM warehouse.dim_estado_pedido WHERE estado = $1",
        estado_nuevo)
    fulfillment = ESTADO_FULFILLMENT_MAP.get(estado_nuevo)

    # [FIX-V5-09] Actualizar tanto estado_pedido_sk como estado_fulfillment
    if fulfillment:
        updated = await conn.fetchval("""
            UPDATE warehouse.fact_pedidos
            SET estado_pedido_sk   = COALESCE($1, estado_pedido_sk),
                estado_fulfillment = $2
            WHERE pedido_id = $3
            RETURNING fact_pedido_sk
        """, estado_sk, fulfillment, pedido_id)
    else:
        updated = await conn.fetchval("""
            UPDATE warehouse.fact_pedidos
            SET estado_pedido_sk = COALESCE($1, estado_pedido_sk)
            WHERE pedido_id = $2
            RETURNING fact_pedido_sk
        """, estado_sk, pedido_id)

    if updated:
        olap_stats["facts_updated"] += 1


async def olap_process_pedido_cancelado(conn, data):
    """Marca cancelado en fact_pedidos."""
    pedido_id = data.get("pedido_id")
    if not pedido_id:
        return

    estado_sk = await conn.fetchval(
        "SELECT estado_pedido_sk FROM warehouse.dim_estado_pedido WHERE estado = 'cancelado'")

    updated = await conn.fetchval("""
        UPDATE warehouse.fact_pedidos
        SET estado_pedido_sk   = $1,
            estado_pago        = 'reembolsado',
            estado_fulfillment = 'pendiente'
        WHERE pedido_id = $2
        RETURNING fact_pedido_sk
    """, estado_sk, pedido_id)
    if updated:
        olap_stats["facts_updated"] += 1


async def olap_process_pago(conn, data, es_exitoso):
    """INSERT en fact_pagos. Idempotente via UNIQUE(pago_id)."""
    pago_id   = data.get("pago_id")
    pedido_id = data.get("pedido_id")
    if not pago_id:
        return

    pago = await conn.fetchrow("SELECT * FROM pagos WHERE pago_id = $1", pago_id)
    if not pago:
        return

    cliente_id = data.get("cliente_id") or await conn.fetchval(
        "SELECT cliente_id FROM pedidos WHERE pedido_id=$1", pedido_id)
    cliente_sk = await olap_ensure_dim_cliente(conn, cliente_id) if cliente_id else None
    fecha_sk   = await olap_get_fecha_sk(conn, pago["creado_en"])
    metodo_sk  = await conn.fetchval(
        "SELECT metodo_pago_sk FROM warehouse.dim_metodo_pago WHERE metodo_pago=$1",
        pago["metodo_pago"])

    await conn.execute("""
        INSERT INTO warehouse.fact_pagos (
            pago_id, pedido_id, cliente_sk, fecha_sk, metodo_pago_sk,
            monto, estado, proveedor_pago, creado_en
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (pago_id) DO NOTHING
    """, pago_id, pago["pedido_id"], cliente_sk, fecha_sk, metodo_sk,
        pago["monto"], pago["estado"], pago["proveedor_pago"], pago["creado_en"])
    olap_stats["facts_inserted"] += 1

    if es_exitoso and pago["pedido_id"]:
        await conn.execute("""
            UPDATE warehouse.fact_pedidos
            SET estado_pago = 'pagado'
            WHERE pedido_id = $1
        """, pago["pedido_id"])
        olap_stats["facts_updated"] += 1

    await conn.execute("""
        UPDATE warehouse.marcas_agua SET ultima_carga=NOW(), actualizado_en=NOW()
        WHERE tabla_fuente='pagos'
    """)
    log.info(f"  [OLAP] fact_pagos: pago {pago_id} ({'✓' if es_exitoso else '✗'})")


async def olap_process_stock(conn, data):
    """
    INSERT en fact_movimientos_inventario. Idempotente via UNIQUE.
    Acepta tipos: ingreso, reposicion, venta, reserva, devolucion.
    """
    mov_id = data.get("movimiento_id")
    if not mov_id:
        return

    mov = await conn.fetchrow(
        "SELECT * FROM movimientos_inventario WHERE movimiento_inventario_id=$1", mov_id)
    if not mov:
        return

    almacen_sk  = await olap_ensure_dim_almacen(conn, mov["almacen_id"])
    variante_sk = await olap_ensure_dim_variante(conn, mov["variante_id"])
    fecha_sk    = await olap_get_fecha_sk(conn, mov["fecha_movimiento"])

    await conn.execute("""
        INSERT INTO warehouse.fact_movimientos_inventario (
            movimiento_id, almacen_sk, variante_sk, fecha_sk,
            tipo_movimiento, cantidad, tipo_referencia, referencia_id, fecha_movimiento
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (movimiento_id) DO NOTHING
    """, mov["movimiento_inventario_id"], almacen_sk, variante_sk, fecha_sk,
        mov["tipo_movimiento"], mov["cantidad"],
        mov["tipo_referencia"], mov["referencia_id"], mov["fecha_movimiento"])
    olap_stats["facts_inserted"] += 1

    await conn.execute("""
        UPDATE warehouse.marcas_agua SET ultima_carga=NOW(), actualizado_en=NOW()
        WHERE tabla_fuente='movimientos_inventario'
    """)
    log.info(f"  [OLAP] fact_movimientos: mov {mov_id} ({mov['tipo_movimiento']})")


async def olap_process_envio(conn, data):
    """
    INSERT en fact_envios cuando envío es entregado.
    [FIX-V5-08] dias_entrega: calculado desde timestamps reales; si NULL, imputa valor.
    """
    envio_id = data.get("envio_id")
    if not envio_id:
        return

    envio = await conn.fetchrow("SELECT * FROM envios WHERE envio_id = $1", envio_id)
    if not envio:
        return

    almacen_sk       = await olap_ensure_dim_almacen(conn, envio["almacen_id"])
    fecha_envio_sk   = await olap_get_fecha_sk(conn, envio["enviado_en"]) \
        if envio["enviado_en"] else None
    fecha_entrega_sk = await olap_get_fecha_sk(conn, envio["entregado_en"]) \
        if envio["entregado_en"] else None

    # [FIX-V5-08] Calcular días reales; si faltan timestamps, imputar distribución realista
    dias = None
    if envio["enviado_en"] and envio["entregado_en"]:
        d1   = envio["enviado_en"].date()   if hasattr(envio["enviado_en"],   "date") else envio["enviado_en"]
        d2   = envio["entregado_en"].date() if hasattr(envio["entregado_en"], "date") else envio["entregado_en"]
        dias = (d2 - d1).days
        if dias < 0:
            dias = 0  # timestamps fuera de orden: imputar 0
    else:
        # Imputar valor realista si los timestamps no están disponibles
        dias = _dias_entrega_simulado()

    await conn.execute("""
        INSERT INTO warehouse.fact_envios (
            envio_id, pedido_id, almacen_sk, fecha_envio_sk, fecha_entrega_sk,
            estado_envio, transportista, dias_entrega
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (envio_id) DO UPDATE
            SET estado_envio     = EXCLUDED.estado_envio,
                fecha_entrega_sk = COALESCE(EXCLUDED.fecha_entrega_sk, warehouse.fact_envios.fecha_entrega_sk),
                dias_entrega     = COALESCE(EXCLUDED.dias_entrega,     warehouse.fact_envios.dias_entrega)
    """, envio_id, envio["pedido_id"], almacen_sk,
        fecha_envio_sk, fecha_entrega_sk,
        envio["estado_envio"], envio["transportista"], dias)
    olap_stats["facts_inserted"] += 1

    await conn.execute("""
        UPDATE warehouse.marcas_agua SET ultima_carga=NOW(), actualizado_en=NOW()
        WHERE tabla_fuente='envios'
    """)
    log.info(f"  [OLAP] fact_envios: envío {envio_id} (dias={dias})")


async def olap_process_envio_devuelto(conn, data):
    """
    Actualiza fact_envios cuando un envío pasa a 'devuelto'.
    Si no existe el registro aún (caso de devolución sin entrega previa), lo inserta.
    """
    envio_id = data.get("envio_id")
    if not envio_id:
        return

    envio = await conn.fetchrow("SELECT * FROM envios WHERE envio_id = $1", envio_id)
    if not envio:
        return

    updated = await conn.fetchval("""
        UPDATE warehouse.fact_envios
        SET estado_envio = 'devuelto'
        WHERE envio_id = $1
        RETURNING envio_id
    """, envio_id)

    if not updated:
        almacen_sk       = await olap_ensure_dim_almacen(conn, envio["almacen_id"])
        fecha_envio_sk   = await olap_get_fecha_sk(conn, envio["enviado_en"]) \
            if envio["enviado_en"] else None
        fecha_entrega_sk = await olap_get_fecha_sk(conn, envio["entregado_en"]) \
            if envio["entregado_en"] else None

        dias = None
        if envio["enviado_en"] and envio["entregado_en"]:
            d1   = envio["enviado_en"].date()   if hasattr(envio["enviado_en"],   "date") else envio["enviado_en"]
            d2   = envio["entregado_en"].date() if hasattr(envio["entregado_en"], "date") else envio["entregado_en"]
            dias = max(0, (d2 - d1).days)
        else:
            dias = _dias_entrega_simulado()

        await conn.execute("""
            INSERT INTO warehouse.fact_envios (
                envio_id, pedido_id, almacen_sk, fecha_envio_sk, fecha_entrega_sk,
                estado_envio, transportista, dias_entrega
            ) VALUES ($1,$2,$3,$4,$5,'devuelto',$6,$7)
            ON CONFLICT (envio_id) DO UPDATE SET estado_envio = 'devuelto'
        """, envio_id, envio["pedido_id"], almacen_sk,
            fecha_envio_sk, fecha_entrega_sk,
            envio["transportista"], dias)
        olap_stats["facts_inserted"] += 1
    else:
        olap_stats["facts_updated"] += 1

    log.info(f"  [OLAP] fact_envios: envío {envio_id} → devuelto")


async def olap_process_devolucion(conn, data):
    """INSERT en fact_devoluciones."""
    devolucion_id = data.get("devolucion_id")
    if not devolucion_id:
        return

    dev = await conn.fetchrow(
        "SELECT * FROM devoluciones WHERE devolucion_id = $1", devolucion_id)
    if not dev:
        return

    ped = await conn.fetchrow(
        "SELECT cliente_id, creado_en FROM pedidos WHERE pedido_id = $1", dev["pedido_id"])

    cliente_sk = await olap_ensure_dim_cliente(conn, ped["cliente_id"]) if ped else None
    fecha_sk   = await olap_get_fecha_sk(conn, dev["creado_en"])

    det = await conn.fetchrow("""
        SELECT dd.variante_id, dd.cantidad_devuelta
        FROM detalle_devoluciones dd
        WHERE dd.devolucion_id = $1 LIMIT 1
    """, devolucion_id)

    variante_sk = None
    producto_sk = None
    cantidad    = 0
    if det:
        variante_sk = await olap_ensure_dim_variante(conn, det["variante_id"])
        prod_id     = await conn.fetchval(
            "SELECT producto_id FROM variantes_producto WHERE variante_id=$1",
            det["variante_id"])
        if prod_id:
            producto_sk = await olap_ensure_dim_producto(conn, prod_id)
        cantidad = det["cantidad_devuelta"]

    await conn.execute("""
        INSERT INTO warehouse.fact_devoluciones (
            devolucion_id, pedido_id, variante_sk, producto_sk, cliente_sk, fecha_sk,
            motivo, estado, cantidad_devuelta
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (devolucion_id) DO NOTHING
    """, devolucion_id, dev["pedido_id"], variante_sk, producto_sk, cliente_sk, fecha_sk,
        dev["motivo"], dev["estado"], cantidad)
    olap_stats["facts_inserted"] += 1

    await conn.execute("""
        UPDATE warehouse.marcas_agua SET ultima_carga=NOW(), actualizado_en=NOW()
        WHERE tabla_fuente='devoluciones'
    """)
    log.info(f"  [OLAP] fact_devoluciones: devolución {devolucion_id}")


async def olap_process_carrito(conn, data, convertido=False):
    """INSERT en fact_carritos. Maneja abandonado y convertido (upsert)."""
    carrito_id = data.get("carrito_id")
    if not carrito_id:
        return

    carrito = await conn.fetchrow("SELECT * FROM carritos WHERE carrito_id = $1", carrito_id)
    if not carrito:
        return

    cliente_sk = await olap_ensure_dim_cliente(conn, carrito["cliente_id"]) \
        if carrito["cliente_id"] else None
    fecha_sk = await olap_get_fecha_sk(conn, carrito["creado_en"])

    num_items = await conn.fetchval(
        "SELECT COUNT(*) FROM detalle_carritos WHERE carrito_id = $1", carrito_id)
    valor = await conn.fetchval("""
        SELECT COALESCE(SUM(cantidad * precio_unitario), 0)
        FROM detalle_carritos WHERE carrito_id = $1
    """, carrito_id)

    estado_final = carrito["estado"]

    await conn.execute("""
        INSERT INTO warehouse.fact_carritos (
            carrito_id, cliente_sk, fecha_sk, estado, num_items, valor_total, convertido
        ) VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (carrito_id) DO UPDATE
            SET estado     = EXCLUDED.estado,
                convertido = EXCLUDED.convertido
    """, carrito_id, cliente_sk, fecha_sk, estado_final,
        num_items, valor, estado_final == "convertido")
    olap_stats["facts_inserted"] += 1
    log.info(f"  [OLAP] fact_carritos: carrito {carrito_id} ({estado_final})")


async def olap_process_carrito_convertido(conn, data):
    """Handler específico para 'carrito_convertido'. Delega a olap_process_carrito."""
    await olap_process_carrito(conn, data, convertido=True)


# DLQ helper
async def _dlq_put(conn, evento_id, tipo_evento, payload, motivo, error=None):
    """Envía evento a Dead-Letter Queue. Idempotente via ON CONFLICT."""
    payload_str = json.dumps(payload) if isinstance(payload, dict) else (payload or "{}")
    await conn.execute("""
        INSERT INTO warehouse.dlq_eventos
            (evento_id, tipo_evento, payload, motivo, error_detalle)
        VALUES ($1, $2, $3::jsonb, $4, $5)
        ON CONFLICT (evento_id) DO NOTHING
    """, evento_id, tipo_evento, payload_str, motivo, error)
    olap_stats["dlq"] += 1
    log.warning(f"  [OLAP][DLQ] evento {evento_id} ({tipo_evento}): {motivo}")


OLAP_HANDLERS = {
    # Pedidos
    "pedido_creado":      olap_process_pedido_creado,
    "pedido_actualizado": olap_process_pedido_actualizado,
    "pedido_cancelado":   olap_process_pedido_cancelado,
    # Pagos
    "pago_capturado":     lambda c, d: olap_process_pago(c, d, True),
    "pago_fallido":       lambda c, d: olap_process_pago(c, d, False),
    # Inventario — todos los tipos del CHECK constraint
    "stock_repuesto":     olap_process_stock,   # tipo: ingreso / reposicion
    "stock_reservado":    olap_process_stock,   # tipo: reserva
    "stock_vendido":      olap_process_stock,   # tipo: venta
    # Envíos — ciclo completo
    "envio_entregado":    olap_process_envio,
    "envio_devuelto":     olap_process_envio_devuelto,
    # Devoluciones
    "devolucion_creada":  olap_process_devolucion,
    # Carritos
    "carrito_abandonado": olap_process_carrito,
    "carrito_convertido": olap_process_carrito_convertido,
}


async def olap_process_event(conn, evento_id, tipo_evento, payload):
    """Procesa un evento. Eventos sin handler → DLQ."""
    handler = OLAP_HANDLERS.get(tipo_evento)
    data    = payload if isinstance(payload, dict) else (json.loads(payload) if payload else {})

    if handler is None:
        await _dlq_put(conn, evento_id, tipo_evento, data, "sin_handler")
        await conn.execute("""
            UPDATE eventos_negocio
            SET procesado_olap = TRUE, intentos_olap = intentos_olap + 1
            WHERE evento_negocio_id = $1
        """, evento_id)
        return

    await handler(conn, data)
    await conn.execute("""
        UPDATE eventos_negocio
        SET procesado_olap = TRUE, intentos_olap = intentos_olap + 1, error_olap = NULL
        WHERE evento_negocio_id = $1
    """, evento_id)
    olap_stats["procesados"] += 1


async def olap_batch_catchup(pool):
    """Polling: procesa eventos pendientes con savepoints por evento."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            eventos = await conn.fetch("""
                SELECT evento_negocio_id, tipo_evento, payload, intentos_olap
                FROM eventos_negocio
                WHERE procesado_olap = FALSE
                  AND intentos_olap  < $1
                ORDER BY ocurrido_en ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            """, MAX_OLAP_RETRIES, OLAP_BATCH_SIZE)

            for ev in eventos:
                eid      = ev["evento_negocio_id"]
                tipo     = ev["tipo_evento"]
                payload  = ev["payload"]
                intentos = ev["intentos_olap"]

                try:
                    async with conn.transaction(isolation="read_committed"):
                        await olap_process_event(conn, eid, tipo, payload)
                except Exception as e:
                    err_msg = str(e)[:500]
                    log.error(f"  [OLAP][ERR] evento {eid} ({tipo}): {err_msg}")
                    olap_stats["errores"] += 1

                    if intentos + 1 >= MAX_OLAP_RETRIES:
                        async with conn.transaction(isolation="read_committed"):
                            await _dlq_put(conn, eid, tipo, payload,
                                           "error_max_reintentos", err_msg)
                            await conn.execute("""
                                UPDATE eventos_negocio
                                SET procesado_olap = TRUE,
                                    intentos_olap  = intentos_olap + 1,
                                    error_olap     = $2
                                WHERE evento_negocio_id = $1
                            """, eid, err_msg)
                    else:
                        async with conn.transaction(isolation="read_committed"):
                            await conn.execute("""
                                UPDATE eventos_negocio
                                SET intentos_olap = intentos_olap + 1,
                                    error_olap    = $2
                                WHERE evento_negocio_id = $1
                            """, eid, err_msg)


async def olap_listen_notify(pool):
    """Procesador en tiempo real via LISTEN/NOTIFY. Re-suscripción automática."""
    while running:
        try:
            async with pool.acquire() as conn:
                await conn.add_listener("eventos_negocio",
                    lambda con, pid, channel, payload_str: asyncio.ensure_future(
                        _handle_notify(pool, payload_str)))
                await conn.execute("LISTEN eventos_negocio")
                log.info("[OLAP][LISTEN] Escuchando canal 'eventos_negocio'")
                while running:
                    await asyncio.sleep(1)
        except Exception as e:
            log.error(f"[OLAP][LISTEN] Conexión caída: {e}. Reconectando en 5s...")
            await asyncio.sleep(5)


async def _handle_notify(pool, payload_str):
    """Procesa un evento recibido via NOTIFY."""
    try:
        data      = json.loads(payload_str)
        evento_id = data.get("id")
        tipo      = data.get("tipo_evento")
        if not evento_id:
            return
        async with pool.acquire() as conn:
            async with conn.transaction():
                pendiente = await conn.fetchval("""
                    SELECT 1 FROM eventos_negocio
                    WHERE evento_negocio_id = $1 AND procesado_olap = FALSE
                    FOR UPDATE SKIP LOCKED
                """, evento_id)
                if pendiente:
                    payload = data.get("payload", {})
                    await olap_process_event(conn, evento_id, tipo, payload)
    except Exception as e:
        log.error(f"[OLAP][NOTIFY] Error procesando notificación: {e}")


# =============================================================================
# MOTORES PRINCIPALES
# =============================================================================

async def motor_oltp(pool):
    log.info("[OLTP] Motor iniciado.")
    while running:
        async with pool.acquire() as conn:
            interval = await oltp_get_adaptive_interval(conn)
        if interval < 0:
            log.info("[OLTP] Límite de pedidos alcanzado. Motor pausado.")
            await asyncio.sleep(30)
            continue

        accion = elegir_accion()
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await OLTP_ACCIONES[accion](conn)
            oltp_stats["ciclos"] += 1
        except Exception as e:
            oltp_stats["errores"] += 1
            log.error(f"[OLTP][ERR] {accion}: {e}")

        # [FIX-V5-10] Jitter en el intervalo
        jitter = random.uniform(-0.5, 0.5)
        await asyncio.sleep(max(0.1, interval + jitter))


async def motor_olap(pool):
    log.info("[OLAP] Motor iniciado.")
    last_health = time.time()

    asyncio.ensure_future(olap_listen_notify(pool))

    while running:
        try:
            await olap_batch_catchup(pool)
        except Exception as e:
            log.error(f"[OLAP][ERR] batch_catchup: {e}")

        if time.time() - last_health >= OLAP_HEALTH_EVERY:
            await _log_health(pool)
            last_health = time.time()

        await asyncio.sleep(OLAP_POLL_INTERVAL)


async def _log_health(pool):
    async with pool.acquire() as conn:
        pendientes = await conn.fetchval(
            "SELECT COUNT(*) FROM eventos_negocio WHERE procesado_olap = FALSE")
        dlq_total  = await conn.fetchval("SELECT COUNT(*) FROM warehouse.dlq_eventos")
        fp_total   = await conn.fetchval("SELECT COUNT(*) FROM warehouse.fact_pedidos")
        fe_total   = await conn.fetchval("SELECT COUNT(*) FROM warehouse.fact_envios")
        fc_total   = await conn.fetchval("SELECT COUNT(*) FROM warehouse.fact_carritos")
        fc_conv    = await conn.fetchval(
            "SELECT COUNT(*) FROM warehouse.fact_carritos WHERE convertido = TRUE")

        # Distribución de estados para diagnóstico
        estados = await conn.fetch("""
            SELECT estado_actual, COUNT(*) as cnt
            FROM pedidos
            WHERE estado_actual NOT IN ('entregado','cancelado','reembolsado')
            GROUP BY 1 ORDER BY 1
        """)
        watermark = await conn.fetchval(
            "SELECT ultima_carga FROM warehouse.marcas_agua WHERE tabla_fuente='pedidos'")

    conv_rate = (fc_conv / fc_total * 100) if fc_total else 0
    estados_str = " | ".join(f"{r['estado_actual']}:{r['cnt']}" for r in estados)

    log.info(
        f"[HEALTH] OLTP→ pedidos={oltp_stats['pedidos']} pagos={oltp_stats['pagos']} "
        f"carr_conv={oltp_stats['carritos_convertidos']} "
        f"carr_aband={oltp_stats['carritos_abandonados']} "
        f"cv_avances={oltp_stats['cv_avances']} "
        f"errores={oltp_stats['errores']} | "
        f"OLAP→ procesados={olap_stats['procesados']} facts={olap_stats['facts_inserted']} "
        f"updates={olap_stats['facts_updated']} dlq={olap_stats['dlq']} | "
        f"DB→ pendientes={pendientes} dlq_total={dlq_total} "
        f"fact_pedidos={fp_total} fact_envios={fe_total} "
        f"carritos={fc_total} tasa_conv={conv_rate:.1f}% wm={watermark} | "
        f"BACKLOG→ {estados_str}"
    )


# =============================================================================
# SETUP / VERIFICACIÓN
# =============================================================================

async def verify_schema(conn):
    """
    Verifica que todas las tablas requeridas existan.
    [FIX-V5-04] Detecta carrito_id en pedidos y configura flag global.
    """
    global _SCHEMA_TIENE_CARRITO_ID

    tablas_oltp = [
        "clientes", "pedidos", "detalle_pedidos", "pagos", "eventos_negocio",
        "almacenes", "niveles_stock", "movimientos_inventario",
        "ordenes_compra", "detalle_ordenes_compra",
        "envios", "detalle_envios",
        "devoluciones", "detalle_devoluciones",
        "promociones_aplicadas_pedido",
    ]
    tablas_wh = [
        "dim_fecha", "dim_cliente", "dim_producto", "dim_variante",
        "dim_almacen", "dim_metodo_pago", "dim_estado_pedido", "dim_canal",
        "fact_pedidos", "fact_detalle_pedidos", "fact_pagos",
        "fact_movimientos_inventario", "fact_envios", "fact_carritos",
        "fact_devoluciones", "dlq_eventos", "marcas_agua",
    ]

    faltantes = []
    for t in tablas_oltp:
        exists = await conn.fetchval(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name=$1", t)
        if not exists:
            faltantes.append(f"public.{t}")

    for t in tablas_wh:
        exists = await conn.fetchval(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='warehouse' AND table_name=$1", t)
        if not exists:
            faltantes.append(f"warehouse.{t}")

    if faltantes:
        log.error(f"Tablas faltantes: {faltantes}")
        log.error("Ejecuta los scripts SQL en orden: 01_oltp.sql → 02_warehouse.sql → ...")
        return False

    # [FIX-V5-04] Detectar carrito_id en pedidos exactamente UNA vez
    carrito_col = await conn.fetchval("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='pedidos'
          AND column_name='carrito_id'
    """)
    _SCHEMA_TIENE_CARRITO_ID = bool(carrito_col)
    if _SCHEMA_TIENE_CARRITO_ID:
        log.info("[SETUP] pedidos.carrito_id detectada — los pedidos desde carrito incluirán vínculo.")
    else:
        log.info(
            "[SETUP] pedidos.carrito_id NO encontrada — se omitirá vínculo carrito→pedido. "
            "Para habilitarlo: ALTER TABLE pedidos ADD COLUMN carrito_id BIGINT REFERENCES carritos(carrito_id);"
        )

    # Verificar que dim_fecha tenga registros suficientes
    fecha_count = await conn.fetchval("SELECT COUNT(*) FROM warehouse.dim_fecha")
    if fecha_count < 365:
        log.warning(f"[SETUP] dim_fecha solo tiene {fecha_count} filas. Verifica 02_warehouse.sql.")

    # Verificar marcas_agua para todas las entidades necesarias
    wm_faltantes = await conn.fetch("""
        SELECT t.tabla FROM
            (VALUES ('pedidos'),('pagos'),('envios'),('devoluciones'),('movimientos_inventario')) t(tabla)
        WHERE NOT EXISTS (
            SELECT 1 FROM warehouse.marcas_agua WHERE tabla_fuente = t.tabla
        )
    """)
    if wm_faltantes:
        falt_str = ", ".join(r["tabla"] for r in wm_faltantes)
        log.warning(f"[SETUP] marcas_agua faltantes para: {falt_str}. Insertando...")
        for r in wm_faltantes:
            await conn.execute(
                "INSERT INTO warehouse.marcas_agua (tabla_fuente) VALUES ($1) ON CONFLICT DO NOTHING",
                r["tabla"])

    log.info("[SETUP] Schema verificado: todas las tablas presentes.")
    return True


# =============================================================================
# ENTRY POINT
# =============================================================================

async def main():
    import argparse

    global OLTP_INTERVAL_MIN, OLTP_INTERVAL_MAX, OLTP_MAX_PEDIDOS
    global PROB_CARRITO_CONVERSION, CV_INTERVAL, CV_MIN_AGE_SECONDS

    parser = argparse.ArgumentParser(description="Plataforma de datos ecommerce v5")
    parser.add_argument("--solo-oltp",        action="store_true",
                        help="Solo motor OLTP + ciclo de vida (sin OLAP)")
    parser.add_argument("--solo-olap",        action="store_true",
                        help="Solo procesador OLAP (sin OLTP)")
    parser.add_argument("--interval",         nargs=2, type=float, metavar=("MIN", "MAX"),
                        help="Intervalo OLTP en segundos")
    parser.add_argument("--max-pedidos",      type=int,
                        help="Máximo número de pedidos en OLTP")
    parser.add_argument("--prob-conversion",  type=float,
                        help="Probabilidad de conversión de carrito (0-1, default: 0.70)")
    parser.add_argument("--cv-interval",      type=float,
                        help=f"Intervalo motor ciclo de vida en segundos (default: {CV_INTERVAL})")
    parser.add_argument("--cv-min-age",       type=int,
                        help=f"Edad mínima en segundos para avanzar un pedido (default: {CV_MIN_AGE_SECONDS})")
    args = parser.parse_args()

    if args.interval:
        OLTP_INTERVAL_MIN, OLTP_INTERVAL_MAX = args.interval
    if args.max_pedidos:
        OLTP_MAX_PEDIDOS = args.max_pedidos
    if args.prob_conversion is not None:
        PROB_CARRITO_CONVERSION = max(0.0, min(1.0, args.prob_conversion))
    if args.cv_interval is not None:
        CV_INTERVAL = args.cv_interval
    if args.cv_min_age is not None:
        CV_MIN_AGE_SECONDS = args.cv_min_age

    log.info("Conectando a PostgreSQL...")
    try:
        # Pools separados: OLTP/CV y OLAP no comparten conexiones
        oltp_pool = await asyncpg.create_pool(**DB_CONFIG, min_size=3, max_size=8)
        olap_pool = await asyncpg.create_pool(**DB_CONFIG, min_size=2, max_size=5)
    except Exception as e:
        log.error(f"No se pudo conectar a PostgreSQL: {e}")
        sys.exit(1)

    async with oltp_pool.acquire() as conn:
        ok = await verify_schema(conn)
    if not ok:
        sys.exit(1)

    log.info("=" * 70)
    log.info("Plataforma de datos ecommerce v5 iniciada")
    log.info(f"  OLTP interval: {OLTP_INTERVAL_MIN}–{OLTP_INTERVAL_MAX}s")
    log.info(f"  OLAP poll: {OLAP_POLL_INTERVAL}s | batch: {OLAP_BATCH_SIZE}")
    log.info(f"  Max reintentos OLAP: {MAX_OLAP_RETRIES}")
    log.info(f"  Ciclo de vida: cada {CV_INTERVAL}s | edad mínima: {CV_MIN_AGE_SECONDS}s")
    log.info(f"  Prob. conversión carrito: {PROB_CARRITO_CONVERSION:.0%}")
    log.info(f"  Schema carrito_id en pedidos: {_SCHEMA_TIENE_CARRITO_ID}")
    log.info("  Distribución de acciones OLTP:")
    for accion, prob in PROB.items():
        log.info(f"    {accion:<20} {prob:.0%}")
    log.info("=" * 70)

    tareas = []
    if not args.solo_olap:
        tareas.append(asyncio.ensure_future(motor_oltp(oltp_pool)))
        # [FIX-V5-01] Motor de ciclo de vida siempre corre junto al OLTP
        tareas.append(asyncio.ensure_future(motor_ciclo_vida(oltp_pool)))
    if not args.solo_oltp:
        tareas.append(asyncio.ensure_future(motor_olap(olap_pool)))

    if not tareas:
        log.error("Debes elegir al menos un motor (--solo-oltp, --solo-olap, o ninguno).")
        sys.exit(1)

    await asyncio.gather(*tareas)

    log.info("Cerrando pools de conexión...")
    await oltp_pool.close()
    await olap_pool.close()
    log.info("Plataforma detenida correctamente.")


if __name__ == "__main__":
    asyncio.run(main())
