-- =====================================================
-- 04_rbac.sql
-- Roles y permisos
-- =====================================================

-- ==========================================
-- SCHEMAS
-- ==========================================

GRANT USAGE ON SCHEMA public TO ecommerce;
GRANT USAGE ON SCHEMA warehouse TO ecommerce;
GRANT USAGE ON SCHEMA internal TO ecommerce;

GRANT USAGE ON SCHEMA warehouse TO pbi_analytical;
GRANT USAGE ON SCHEMA warehouse TO pbi_executive;

-- ==========================================
-- ECOMMERCE (ROL APP - FULL ACCESS)
-- ==========================================

-- PUBLIC
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ecommerce;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ecommerce;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO ecommerce;

-- WAREHOUSE
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA warehouse TO ecommerce;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA warehouse TO ecommerce;

-- IMPORTANTE: asegurar acceso a vistas
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA warehouse TO ecommerce;

-- DEFAULT PRIVILEGES (para futuras tablas/vistas)
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
GRANT ALL ON TABLES TO ecommerce;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA warehouse
GRANT ALL ON TABLES TO ecommerce;

-- ==========================================
-- INTERNAL (SEGURIDAD)
-- ==========================================

REVOKE ALL ON internal.usuarios FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON internal.usuarios TO ecommerce;

-- ==========================================
-- PBI ANALYTICAL (LECTURA COMPLETA)
-- ==========================================

GRANT SELECT ON ALL TABLES IN SCHEMA warehouse TO pbi_analytical;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA warehouse
GRANT SELECT ON TABLES TO pbi_analytical;

-- ==========================================
-- PBI EXECUTIVE (SOLO VISTAS)
-- ==========================================

-- Se asignan en 05_views.sql

-- ==========================================
-- FIN
-- ==========================================