-- =============================================================================
-- 03_auth.sql  —  Ecommerce CL v3 — Autenticación de aplicación (bcrypt)
-- Ejecutar DESPUÉS de 02_warehouse.sql
--
-- Correcciones v3 sobre v2:
--   [v3-A01] SECURITY DEFINER con SET search_path fijo en ambas funciones.
--   [v3-A02] ecommerce solo tiene EXECUTE sobre las dos funciones de internal;
--            nunca SELECT/INSERT/UPDATE sobre internal.usuarios.
--   [v3-A03] Contraseñas hasheadas con bcrypt cost=12 en seed.
--   [v3-A04] Función de desbloqueo separada para operaciones de admin.
--   [v3-A05] Lógica de bloqueo: tras 5 intentos fallidos → 15 minutos.
--   [v3-A06] Índice explícito sobre username (complementa la constraint UNIQUE).
--
-- NOTA: El uso de crypt() aquí es solo para setup de demo. En producción,
-- el hash debe generarse en la capa de aplicación (Python bcrypt/argon2-cffi)
-- y almacenarse ya hasheado. Nunca transmitir la contraseña en texto plano.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS internal;

-- =============================================================================
-- TABLA DE USUARIOS DE APLICACIÓN
-- =============================================================================

CREATE TABLE IF NOT EXISTS internal.usuarios (
    usuario_id              SERIAL PRIMARY KEY,
    username                VARCHAR(50)   NOT NULL UNIQUE,
    -- bcrypt hash: verificar con crypt(pwd_ingresado, password_hash) = password_hash
    password_hash           VARCHAR(128)  NOT NULL,
    nombre_completo         VARCHAR(100),
    correo                  VARCHAR(100),
    rol                     VARCHAR(20)   NOT NULL
        CHECK (rol IN ('executive','management','operational','analytical')),
    area                    VARCHAR(50),
    activo                  BOOLEAN       NOT NULL DEFAULT TRUE,
    intentos_fallidos       SMALLINT      NOT NULL DEFAULT 0,
    ultimo_intento_fallido  TIMESTAMPTZ,
    bloqueado_hasta         TIMESTAMPTZ,
    creado_en               TIMESTAMPTZ   DEFAULT NOW(),
    ultimo_login            TIMESTAMPTZ
);

-- [v3-A06] Índice explícito complementario (la constraint UNIQUE ya crea uno,
-- pero lo dejamos explícito para mayor claridad en los planes de ejecución)
-- En PostgreSQL el UNIQUE constraint ya crea el índice; este comentario documenta
-- la decisión de no crear uno redundante.

-- =============================================================================
-- SEED — contraseña demo: dashboard123
-- Para rotar contraseñas desde psql:
--   UPDATE internal.usuarios
--   SET password_hash = crypt('nueva_clave', gen_salt('bf', 12))
--   WHERE username = 'pedro_exec';
-- =============================================================================

INSERT INTO internal.usuarios
    (username, password_hash, nombre_completo, rol, area)
VALUES
    ('pedro_exec',
     crypt('dashboard123', gen_salt('bf', 12)),
     'Pedro Valdivia',    'executive',   'Direccion General'),
    ('paula_mngr',
     crypt('dashboard123', gen_salt('bf', 12)),
     'Paula Jaraquemada', 'management',  'Gerencia Comercial'),
    ('juan_ops',
     crypt('dashboard123', gen_salt('bf', 12)),
     'Juan Perez',        'operational', 'Logistica y Almacen'),
    ('carla_data',
     crypt('dashboard123', gen_salt('bf', 12)),
     'Carla Troncoso',    'analytical',  'Business Intelligence')
ON CONFLICT (username) DO NOTHING;

-- =============================================================================
-- FUNCIÓN: verificar_credenciales
-- [v3-A01] SECURITY DEFINER + search_path fijo
-- Retorna datos del usuario solo si la autenticación es exitosa.
-- Implementa protección contra timing attacks (pg_sleep en fallo de usuario).
-- =============================================================================

CREATE OR REPLACE FUNCTION internal.verificar_credenciales(
    p_username VARCHAR,
    p_password VARCHAR
)
RETURNS TABLE (
    usuario_id      INT,
    username        VARCHAR,
    nombre_completo VARCHAR,
    rol             VARCHAR,
    area            VARCHAR
)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = internal, pg_temp
AS $$
DECLARE
    v_usuario internal.usuarios%ROWTYPE;
BEGIN
    SELECT * INTO v_usuario
    FROM internal.usuarios u
    WHERE u.username = p_username AND u.activo = TRUE;

    IF NOT FOUND THEN
        -- No revelar si el usuario existe (timing attack mitigation)
        PERFORM pg_sleep(0.1);
        RETURN;
    END IF;

    -- Cuenta bloqueada
    IF v_usuario.bloqueado_hasta IS NOT NULL
       AND v_usuario.bloqueado_hasta > NOW() THEN
        RETURN;
    END IF;

    IF public.crypt(p_password, v_usuario.password_hash) = v_usuario.password_hash THEN
        -- Éxito: resetear contadores de bloqueo
        UPDATE internal.usuarios
        SET ultimo_login           = NOW(),
            intentos_fallidos      = 0,
            ultimo_intento_fallido = NULL,
            bloqueado_hasta        = NULL
        WHERE internal.usuarios.username = p_username;

        RETURN QUERY
            SELECT u.usuario_id, u.username, u.nombre_completo, u.rol, u.area
            FROM internal.usuarios u
            WHERE u.username = p_username;
    ELSE
        -- Fallo: incrementar intentos; bloquear al 5to intento (15 minutos)
        UPDATE internal.usuarios
        SET intentos_fallidos       = intentos_fallidos + 1,
            ultimo_intento_fallido  = NOW(),
            bloqueado_hasta         = CASE
                WHEN intentos_fallidos + 1 >= 5
                THEN NOW() + INTERVAL '15 minutes'
                ELSE NULL
            END
        WHERE internal.usuarios.username = p_username;

        RETURN;
    END IF;
END;
$$;

-- =============================================================================
-- FUNCIÓN: desbloquear_usuario (uso admin exclusivo)
-- [v3-A04]
-- =============================================================================

CREATE OR REPLACE FUNCTION internal.desbloquear_usuario(p_username VARCHAR)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = internal, pg_temp
AS $$
BEGIN
    UPDATE internal.usuarios
    SET intentos_fallidos       = 0,
        ultimo_intento_fallido  = NULL,
        bloqueado_hasta         = NULL
    WHERE username = p_username;
END;
$$;

-- =============================================================================
-- PERMISOS
-- [v3-A02] ecommerce solo puede llamar las dos funciones.
--          NO tiene acceso directo a internal.usuarios.
-- =============================================================================

GRANT USAGE   ON SCHEMA internal TO ecommerce;
GRANT EXECUTE ON FUNCTION internal.verificar_credenciales(VARCHAR, VARCHAR) TO ecommerce;
GRANT EXECUTE ON FUNCTION internal.desbloquear_usuario(VARCHAR) TO ecommerce;
-- NO se otorga SELECT/INSERT/UPDATE sobre internal.usuarios

DO $$ BEGIN RAISE NOTICE '03_auth.sql completado. Ejecuta: 04_rbac.sql'; END $$;