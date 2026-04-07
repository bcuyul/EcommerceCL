-- =============================================================
-- QUERIES DE PORTAFOLIO — Ecommerce CL v3
-- Para usar en Apache Superset: SQL Lab o como Dataset de Charts
-- Conectarse con usuario: pbi_analytical (acceso completo warehouse)
-- =============================================================

-- =============================================================
-- 1. VENTAS DIARIAS — Tendencia temporal
-- Gráfico sugerido: Line Chart (eje X = fecha, eje Y = ventas_netas)
-- =============================================================

SELECT
    d.fecha,
    d.dia_nombre,
    d.es_fin_semana,
    COUNT(DISTINCT f.pedido_id)     AS total_pedidos,
    SUM(f.total_final)              AS ventas_netas,
    AVG(f.total_final)              AS ticket_promedio,
    SUM(f.descuento_total)          AS descuentos_otorgados
FROM warehouse.fact_pedidos f
JOIN warehouse.dim_fecha d ON f.fecha_sk = d.fecha_sk
WHERE f.estado_pago = 'pagado'
GROUP BY 1, 2, 3
ORDER BY 1;


-- =============================================================
-- 2. TOP 10 PRODUCTOS MÁS VENDIDOS — Por ingresos
-- Gráfico sugerido: Bar Chart horizontal
-- =============================================================

SELECT
    dp.nombre_producto,
    dp.nombre_marca,
    dp.nombre_categoria,
    SUM(fdp.cantidad)               AS unidades_vendidas,
    SUM(fdp.total_linea)            AS ingresos_totales,
    AVG(fdp.precio_unitario)        AS precio_promedio,
    COUNT(DISTINCT fdp.pedido_id)   AS pedidos_distintos
FROM warehouse.fact_detalle_pedidos fdp
JOIN warehouse.dim_producto dp ON fdp.producto_sk = dp.producto_sk
WHERE dp.es_actual = TRUE
GROUP BY 1, 2, 3
ORDER BY ingresos_totales DESC
LIMIT 10;


-- =============================================================
-- 3. TASA DE CONVERSIÓN DE CARRITOS — Por mes
-- Gráfico sugerido: Bar + Line combo (barras = volumen, línea = tasa %)
-- =============================================================

SELECT
    d.anio,
    d.mes,
    d.mes_nombre,
    COUNT(*)                                                        AS total_carritos,
    COUNT(*) FILTER (WHERE fc.convertido = TRUE)                   AS convertidos,
    COUNT(*) FILTER (WHERE fc.convertido = FALSE)                  AS abandonados,
    ROUND(
        COUNT(*) FILTER (WHERE fc.convertido = TRUE)::numeric
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                               AS tasa_conversion_pct
FROM warehouse.fact_carritos fc
JOIN warehouse.dim_fecha d ON fc.fecha_sk = d.fecha_sk
GROUP BY 1, 2, 3
ORDER BY 1, 2;


-- =============================================================
-- 4. PERFORMANCE DE TRANSPORTISTAS
-- Gráfico sugerido: Table con color condicional o Bar Chart
-- =============================================================

SELECT
    fe.transportista,
    COUNT(*)                                                AS total_envios,
    ROUND(AVG(fe.dias_entrega), 1)                          AS promedio_dias_entrega,
    MIN(fe.dias_entrega)                                    AS minimo_dias,
    MAX(fe.dias_entrega)                                    AS maximo_dias,
    COUNT(*) FILTER (WHERE fe.estado_envio = 'entregado')   AS entregas_exitosas,
    COUNT(*) FILTER (WHERE fe.estado_envio = 'devuelto')    AS devoluciones,
    ROUND(
        COUNT(*) FILTER (WHERE fe.estado_envio = 'entregado')::numeric
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                       AS tasa_exito_pct,
    da.ciudad                                               AS ciudad_almacen
FROM warehouse.fact_envios fe
JOIN warehouse.dim_almacen da ON fe.almacen_sk = da.almacen_sk
WHERE fe.transportista IS NOT NULL
GROUP BY 1, 9
ORDER BY promedio_dias_entrega;


-- =============================================================
-- 5. ANÁLISIS DE DEVOLUCIONES — Por motivo y producto
-- Gráfico sugerido: Pie Chart (motivos) + Table (productos con más devoluciones)
-- =============================================================

SELECT
    fd.motivo,
    dp.nombre_categoria,
    COUNT(*)                    AS total_devoluciones,
    SUM(fd.cantidad_devuelta)   AS unidades_devueltas,
    COUNT(DISTINCT fd.pedido_id) AS pedidos_afectados
FROM warehouse.fact_devoluciones fd
JOIN warehouse.dim_producto dp ON fd.producto_sk = dp.producto_sk
WHERE dp.es_actual = TRUE
GROUP BY 1, 2
ORDER BY total_devoluciones DESC;


-- =============================================================
-- 6. TICKET PROMEDIO POR CANAL DE VENTA
-- Gráfico sugerido: Bar Chart o KPI Cards
-- =============================================================

SELECT
    dc.canal,
    COUNT(DISTINCT f.pedido_id)             AS total_pedidos,
    SUM(f.total_final)                      AS ventas_totales,
    ROUND(AVG(f.total_final), 0)            AS ticket_promedio_clp,
    ROUND(AVG(f.num_items), 1)              AS items_promedio_por_pedido,
    ROUND(AVG(f.descuento_total), 0)        AS descuento_promedio_clp
FROM warehouse.fact_pedidos f
JOIN warehouse.dim_canal dc ON f.canal_sk = dc.canal_sk
WHERE f.estado_pago = 'pagado'
GROUP BY 1
ORDER BY ventas_totales DESC;


-- =============================================================
-- 7. MÉTODOS DE PAGO — Distribución y montos
-- Gráfico sugerido: Pie Chart o Donut
-- =============================================================

SELECT
    dmp.metodo_pago,
    dmp.categoria,
    COUNT(*)                AS total_transacciones,
    SUM(fp.monto)           AS monto_total_clp,
    ROUND(AVG(fp.monto), 0) AS monto_promedio_clp,
    ROUND(
        COUNT(*)::numeric / SUM(COUNT(*)) OVER () * 100, 1
    )                       AS porcentaje_uso
FROM warehouse.fact_pagos fp
JOIN warehouse.dim_metodo_pago dmp ON fp.metodo_pago_sk = dmp.metodo_pago_sk
WHERE fp.estado = 'capturado'
GROUP BY 1, 2
ORDER BY total_transacciones DESC;


-- =============================================================
-- 8. SEGMENTACIÓN DE CLIENTES — Por segmento y ventas acumuladas
-- Gráfico sugerido: Bar Chart o Treemap
-- =============================================================

SELECT
    dc.segmento,
    COUNT(DISTINCT dc.cliente_id)       AS total_clientes,
    COUNT(DISTINCT f.pedido_id)         AS total_pedidos,
    SUM(f.total_final)                  AS ventas_totales,
    ROUND(AVG(f.total_final), 0)        AS ticket_promedio,
    ROUND(
        SUM(f.total_final)
        / NULLIF(COUNT(DISTINCT dc.cliente_id), 0), 0
    )                                   AS ltv_promedio_clp
FROM warehouse.dim_cliente dc
JOIN warehouse.fact_pedidos f ON f.cliente_sk = dc.cliente_sk
WHERE dc.es_actual = TRUE
  AND f.estado_pago = 'pagado'
GROUP BY 1
ORDER BY ventas_totales DESC;


-- =============================================================
-- 9. ROTACIÓN DE INVENTARIO — Entradas vs salidas por almacén
-- Gráfico sugerido: Grouped Bar o Heatmap por almacén + mes
-- =============================================================

SELECT
    da.nombre                                                           AS almacen,
    da.ciudad,
    d.mes_nombre,
    d.anio,
    SUM(CASE WHEN fmi.tipo_movimiento IN ('ingreso','reposicion')
             THEN fmi.cantidad ELSE 0 END)                             AS entradas,
    SUM(CASE WHEN fmi.tipo_movimiento IN ('venta','reserva')
             THEN ABS(fmi.cantidad) ELSE 0 END)                        AS salidas,
    SUM(CASE WHEN fmi.tipo_movimiento = 'devolucion'
             THEN fmi.cantidad ELSE 0 END)                             AS devoluciones_stock
FROM warehouse.fact_movimientos_inventario fmi
JOIN warehouse.dim_almacen da ON fmi.almacen_sk = da.almacen_sk
JOIN warehouse.dim_fecha    d  ON fmi.fecha_sk   = d.fecha_sk
GROUP BY 1, 2, 3, 4
ORDER BY da.nombre, d.anio, d.mes_nombre;


-- =============================================================
-- 10. RETENCIÓN Y FRECUENCIA DE COMPRA — Análisis de cohort simple
-- Gráfico sugerido: Table con formato condicional
-- Requiere usuario pbi_analytical (acceso a fact_pedidos completo)
-- =============================================================

WITH primer_pedido AS (
    SELECT
        f.cliente_sk,
        MIN(d.fecha)                AS fecha_primera_compra,
        DATE_TRUNC('month', MIN(d.fecha)) AS mes_cohort
    FROM warehouse.fact_pedidos f
    JOIN warehouse.dim_fecha d ON f.fecha_sk = d.fecha_sk
    WHERE f.estado_pago = 'pagado'
    GROUP BY 1
),
pedidos_por_cliente AS (
    SELECT
        f.cliente_sk,
        COUNT(DISTINCT f.pedido_id)     AS total_pedidos,
        SUM(f.total_final)              AS valor_total,
        MAX(d.fecha)                    AS ultima_compra
    FROM warehouse.fact_pedidos f
    JOIN warehouse.dim_fecha d ON f.fecha_sk = d.fecha_sk
    WHERE f.estado_pago = 'pagado'
    GROUP BY 1
)
SELECT
    TO_CHAR(pp.mes_cohort, 'YYYY-MM')       AS mes_cohort,
    COUNT(DISTINCT pp.cliente_sk)            AS clientes_nuevos,
    ROUND(AVG(pc.total_pedidos), 1)          AS promedio_pedidos,
    ROUND(AVG(pc.valor_total), 0)            AS ltv_promedio_clp,
    COUNT(*) FILTER (WHERE pc.total_pedidos > 1)  AS clientes_recurrentes,
    ROUND(
        COUNT(*) FILTER (WHERE pc.total_pedidos > 1)::numeric
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                        AS tasa_recurrencia_pct
FROM primer_pedido pp
JOIN pedidos_por_cliente pc ON pp.cliente_sk = pc.cliente_sk
GROUP BY 1
ORDER BY 1;
