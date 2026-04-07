-- =====================================================
-- 05_views.sql
-- Vistas analíticas + seguridad
-- =====================================================

SET search_path TO warehouse, public;

-- ==========================================
-- VISTA: VENTAS GEOGRÁFICAS
-- ==========================================

CREATE OR REPLACE VIEW warehouse.vw_ventas_geograficas AS
SELECT
    d.fecha,
    f.region,
    SUM(f.total) AS total_ventas,
    COUNT(*) AS total_pedidos
FROM warehouse.fact_pedidos f
JOIN warehouse.dim_fecha d ON f.fecha_key = d.fecha_key
GROUP BY 1,2;

-- ==========================================
-- VISTA: CLIENTES ACTUALES
-- ==========================================

CREATE OR REPLACE VIEW warehouse.vw_clientes_actuales AS
SELECT
    cliente_id,
    segmento,
    fecha_inicio,
    fecha_fin
FROM warehouse.dim_cliente
WHERE es_actual = TRUE;

-- ==========================================
-- VISTA: CLIENTES HISTÓRICO
-- ==========================================

CREATE OR REPLACE VIEW warehouse.vw_clientes_historico AS
SELECT *
FROM warehouse.dim_cliente;

-- ==========================================
-- VISTA: PERFORMANCE TRANSPORTISTAS
-- ==========================================

CREATE OR REPLACE VIEW warehouse.vw_performance_transportistas AS
SELECT
    transportista,
    COUNT(*) AS total_envios,
    AVG(tiempo_entrega) AS promedio_entrega
FROM warehouse.fact_envios
GROUP BY 1;

-- ==========================================
-- VISTA: DEVOLUCIONES
-- ==========================================

CREATE OR REPLACE VIEW warehouse.vw_resumen_devoluciones AS
SELECT
    motivo,
    COUNT(*) AS total_devoluciones
FROM warehouse.fact_devoluciones
GROUP BY 1;

-- ==========================================
-- VISTA: STOCK CRÍTICO
-- ==========================================

CREATE OR REPLACE VIEW warehouse.vw_stock_critico AS
SELECT
    producto_id,
    stock_actual,
    stock_minimo
FROM warehouse.dim_producto
WHERE stock_actual < stock_minimo;

-- ==========================================
-- VISTA: DLQ
-- ==========================================

CREATE OR REPLACE VIEW warehouse.vw_dlq_monitor AS
SELECT *
FROM warehouse.dlq_eventos;

-- =====================================================
-- 🔥 FIX CLAVE: OWNER DE VISTAS
-- =====================================================

ALTER VIEW warehouse.vw_ventas_geograficas OWNER TO ecommerce;
ALTER VIEW warehouse.vw_clientes_actuales OWNER TO ecommerce;
ALTER VIEW warehouse.vw_clientes_historico OWNER TO ecommerce;
ALTER VIEW warehouse.vw_performance_transportistas OWNER TO ecommerce;
ALTER VIEW warehouse.vw_resumen_devoluciones OWNER TO ecommerce;
ALTER VIEW warehouse.vw_stock_critico OWNER TO ecommerce;
ALTER VIEW warehouse.vw_dlq_monitor OWNER TO ecommerce;

-- =====================================================
-- GRANTS
-- =====================================================

-- ecommerce (full)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA warehouse TO ecommerce;

-- pbi_executive (solo vistas)
GRANT SELECT ON warehouse.vw_ventas_geograficas TO pbi_executive;
GRANT SELECT ON warehouse.vw_clientes_actuales TO pbi_executive;
GRANT SELECT ON warehouse.vw_clientes_historico TO pbi_executive;
GRANT SELECT ON warehouse.vw_performance_transportistas TO pbi_executive;
GRANT SELECT ON warehouse.vw_resumen_devoluciones TO pbi_executive;
GRANT SELECT ON warehouse.vw_stock_critico TO pbi_executive;
GRANT SELECT ON warehouse.vw_dlq_monitor TO pbi_executive;

-- pbi_analytical (todo warehouse)
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse TO pbi_analytical;

-- =====================================================
-- DEFAULT PRIVILEGES (futuras vistas/tablas)
-- =====================================================

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA warehouse
GRANT SELECT ON TABLES TO pbi_executive;

-- =====================================================
-- FIN
-- =====================================================