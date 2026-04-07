-- =============================================================================
-- 02_warehouse.sql  —  Ecommerce CL v2 — Star Schema Kimball + SCD2 + DLQ
-- Ejecutar DESPUÉS de 01_oltp.sql
--
-- Decisiones de modelado documentadas:
--   [M-01] fact_envios: un registro por envío (no por pedido). Permite análisis
--          de performance por transportista, tiempo de entrega y SLA.
--   [M-02] fact_devoluciones: analítica de tasa, motivos e impacto financiero.
--   [M-03] SCD Tipo 2 en dim_cliente, dim_producto, dim_variante.
--          Índice único parcial WHERE es_actual=TRUE garantiza unicidad de versión activa.
--   [M-04] Idempotencia real: UNIQUE sobre clave de negocio en todas las facts.
--          Los INSERT usan ON CONFLICT DO NOTHING → pipeline es re-ejecutable.
--   [M-05] dim_almacen es Type 1 (atributos estables). Upsert simple sin historial.
--   [M-06] DLQ (dlq_eventos): nunca se descartan eventos. Los sin handler o con
--          error tras MAX_RETRIES quedan aquí para auditoría/recuperación manual.
--   [M-07] marcas_agua: actualizada en cada ciclo OLAP para soporte de catchup.
--
-- Correcciones v3 sobre v2:
--   [v3-W01] dim_cliente: valido_hasta NULL para versión activa está documentado
--            (NULL = vigente); CHECK constraint confirma coherencia.
--   [v3-W02] fact_pedidos: columnas estado_pago/estado_fulfillment actualizables
--            para reflejar cambios de estado sin duplicar filas.
--   [v3-W03] Índices adicionales en DLQ para monitoreo operacional.
-- =============================================================================

\c ecommerce_cl
SET client_encoding = 'UTF8';
SET timezone = 'America/Santiago';

CREATE SCHEMA IF NOT EXISTS warehouse;

-- =============================================================================
-- DIMENSIONES
-- =============================================================================

-- dim_fecha: calendario 2020–2030 pre-cargado
CREATE TABLE warehouse.dim_fecha (
    fecha_sk        SERIAL PRIMARY KEY,
    fecha           DATE         NOT NULL UNIQUE,
    anio            SMALLINT,
    trimestre       SMALLINT,
    mes             SMALLINT,
    semana          SMALLINT,
    dia_mes         SMALLINT,
    dia_semana      SMALLINT,
    dia_nombre      VARCHAR(20),
    mes_nombre      VARCHAR(20),
    es_fin_semana   BOOLEAN,
    es_feriado      BOOLEAN DEFAULT FALSE
);

INSERT INTO warehouse.dim_fecha
    (fecha, anio, trimestre, mes, semana, dia_mes, dia_semana, dia_nombre, mes_nombre, es_fin_semana)
SELECT
    d::date,
    EXTRACT(year    FROM d)::smallint,
    EXTRACT(quarter FROM d)::smallint,
    EXTRACT(month   FROM d)::smallint,
    EXTRACT(week    FROM d)::smallint,
    EXTRACT(day     FROM d)::smallint,
    EXTRACT(dow     FROM d)::smallint,
    trim(to_char(d, 'Day')),
    trim(to_char(d, 'Month')),
    EXTRACT(dow FROM d) IN (0, 6)
FROM generate_series('2020-01-01'::date, '2030-12-31'::date, '1 day') g(d)
ON CONFLICT DO NOTHING;

-- dim_cliente: SCD Tipo 2
-- [M-03] Índice único parcial garantiza como máximo una versión activa por cliente.
-- [v3-W01] valido_hasta IS NULL para filas activas; NOT NULL para históricas.
CREATE TABLE warehouse.dim_cliente (
    cliente_sk      SERIAL PRIMARY KEY,
    cliente_id      BIGINT       NOT NULL,
    correo          VARCHAR(255),
    nombre_completo VARCHAR(200),
    segmento        VARCHAR(30),
    estado          VARCHAR(20),
    codigo_pais     CHAR(2),
    ciudad          VARCHAR(100),
    region          VARCHAR(100),
    valido_desde    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- NULL = versión vigente; NOT NULL = versión cerrada
    valido_hasta    TIMESTAMPTZ,
    es_actual       BOOLEAN      NOT NULL DEFAULT TRUE,
    -- Coherencia SCD2: si es_actual=FALSE, valido_hasta debe tener valor
    CONSTRAINT chk_scd2_cliente_cierre CHECK (
        (es_actual = TRUE  AND valido_hasta IS NULL) OR
        (es_actual = FALSE AND valido_hasta IS NOT NULL)
    )
);
-- [M-03] Garantía de unicidad: solo una versión activa por cliente_id
CREATE UNIQUE INDEX uq_dim_cliente_actual
    ON warehouse.dim_cliente(cliente_id) WHERE es_actual = TRUE;

-- dim_producto: SCD Tipo 2
CREATE TABLE warehouse.dim_producto (
    producto_sk     SERIAL PRIMARY KEY,
    producto_id     BIGINT       NOT NULL,
    nombre_producto VARCHAR(255),
    nombre_marca    VARCHAR(100),
    nombre_categoria VARCHAR(100),
    categoria_padre VARCHAR(100),
    estado_producto VARCHAR(20),
    valido_desde    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    valido_hasta    TIMESTAMPTZ,
    es_actual       BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT chk_scd2_producto_cierre CHECK (
        (es_actual = TRUE  AND valido_hasta IS NULL) OR
        (es_actual = FALSE AND valido_hasta IS NOT NULL)
    )
);
CREATE UNIQUE INDEX uq_dim_producto_actual
    ON warehouse.dim_producto(producto_id) WHERE es_actual = TRUE;

-- dim_variante: SCD Tipo 2
CREATE TABLE warehouse.dim_variante (
    variante_sk         SERIAL PRIMARY KEY,
    variante_id         BIGINT       NOT NULL,
    producto_sk         INT REFERENCES warehouse.dim_producto(producto_sk),
    sku                 VARCHAR(100),
    color               VARCHAR(50),
    talla               VARCHAR(20),
    precio_lista_actual NUMERIC(12,2),
    precio_costo        NUMERIC(12,2),
    activa              BOOLEAN,
    valido_desde        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    valido_hasta        TIMESTAMPTZ,
    es_actual           BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT chk_scd2_variante_cierre CHECK (
        (es_actual = TRUE  AND valido_hasta IS NULL) OR
        (es_actual = FALSE AND valido_hasta IS NOT NULL)
    )
);
CREATE UNIQUE INDEX uq_dim_variante_actual
    ON warehouse.dim_variante(variante_id) WHERE es_actual = TRUE;

-- dim_almacen: Type 1 — atributos estables, sin historial [M-05]
CREATE TABLE warehouse.dim_almacen (
    almacen_sk      SERIAL PRIMARY KEY,
    almacen_id      BIGINT       NOT NULL UNIQUE,
    codigo          VARCHAR(20),
    nombre          VARCHAR(100),
    ciudad          VARCHAR(100),
    codigo_pais     CHAR(2)
);

-- dim_metodo_pago
CREATE TABLE warehouse.dim_metodo_pago (
    metodo_pago_sk  SERIAL PRIMARY KEY,
    metodo_pago     VARCHAR(30)  UNIQUE,
    categoria       VARCHAR(30)
);

INSERT INTO warehouse.dim_metodo_pago (metodo_pago, categoria) VALUES
    ('tarjeta_credito',  'tarjeta'),
    ('tarjeta_debito',   'tarjeta'),
    ('transferencia',    'bancario'),
    ('efectivo',         'efectivo'),
    ('billetera_virtual','digital'),
    ('cuotas',           'digital'),
    ('criptomoneda',     'digital')
ON CONFLICT DO NOTHING;

-- dim_estado_pedido
CREATE TABLE warehouse.dim_estado_pedido (
    estado_pedido_sk SERIAL PRIMARY KEY,
    estado           VARCHAR(30)  UNIQUE,
    es_terminal      BOOLEAN      DEFAULT FALSE,
    es_positivo      BOOLEAN      DEFAULT TRUE
);

INSERT INTO warehouse.dim_estado_pedido (estado, es_terminal, es_positivo) VALUES
    ('pendiente',    FALSE, TRUE),
    ('confirmado',   FALSE, TRUE),
    ('pagado',       FALSE, TRUE),
    ('preparando',   FALSE, TRUE),
    ('empaquetado',  FALSE, TRUE),
    ('enviado',      FALSE, TRUE),
    ('entregado',    TRUE,  TRUE),
    ('cancelado',    TRUE,  FALSE),
    ('reembolsado',  TRUE,  FALSE)
ON CONFLICT DO NOTHING;

-- dim_canal
CREATE TABLE warehouse.dim_canal (
    canal_sk        SERIAL PRIMARY KEY,
    canal           VARCHAR(30)  UNIQUE
);

INSERT INTO warehouse.dim_canal (canal) VALUES
    ('web'), ('mobile'), ('marketplace'), ('call_center'), ('api')
ON CONFLICT DO NOTHING;

-- dim_promocion (Type 1 — rara vez cambia de forma material)
CREATE TABLE warehouse.dim_promocion (
    promocion_sk    SERIAL PRIMARY KEY,
    promocion_id    BIGINT       UNIQUE,
    nombre          VARCHAR(150),
    tipo            VARCHAR(30)
);

-- =============================================================================
-- FACT TABLES
-- [M-04] Todas las facts tienen UNIQUE sobre su clave de negocio para idempotencia.
--        Los INSERT usan ON CONFLICT DO NOTHING → pipeline re-ejecutable sin duplicados.
-- =============================================================================

-- fact_pedidos: una fila por pedido.
-- [v3-W02] estado_pago/estado_fulfillment son actualizables (UPDATE on conflict o
--          UPDATE directa) sin duplicar la fila.
CREATE TABLE warehouse.fact_pedidos (
    fact_pedido_sk      BIGSERIAL PRIMARY KEY,
    pedido_id           BIGINT       NOT NULL UNIQUE,          -- [M-04]
    numero_pedido       VARCHAR(30),
    cliente_sk          INT REFERENCES warehouse.dim_cliente(cliente_sk),
    fecha_sk            INT REFERENCES warehouse.dim_fecha(fecha_sk),
    canal_sk            INT REFERENCES warehouse.dim_canal(canal_sk),
    estado_pedido_sk    INT REFERENCES warehouse.dim_estado_pedido(estado_pedido_sk),
    promocion_sk        INT REFERENCES warehouse.dim_promocion(promocion_sk),
    subtotal            NUMERIC(12,2),
    descuento_total     NUMERIC(12,2),
    impuesto_total      NUMERIC(12,2),
    costo_envio         NUMERIC(12,2),
    total_final         NUMERIC(12,2),
    num_items           INT,
    estado_pago         VARCHAR(20),
    estado_fulfillment  VARCHAR(20),
    creado_en           TIMESTAMPTZ,
    cargado_en          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE warehouse.fact_detalle_pedidos (
    fact_detalle_sk     BIGSERIAL PRIMARY KEY,
    detalle_pedido_id   BIGINT       NOT NULL UNIQUE,          -- [M-04]
    pedido_id           BIGINT       NOT NULL,
    variante_sk         INT REFERENCES warehouse.dim_variante(variante_sk),
    producto_sk         INT REFERENCES warehouse.dim_producto(producto_sk),
    cliente_sk          INT REFERENCES warehouse.dim_cliente(cliente_sk),
    fecha_sk            INT REFERENCES warehouse.dim_fecha(fecha_sk),
    cantidad            INT,
    precio_unitario     NUMERIC(12,2),
    descuento_linea     NUMERIC(12,2),
    impuesto_linea      NUMERIC(12,2),
    total_linea         NUMERIC(12,2),
    creado_en           TIMESTAMPTZ,
    cargado_en          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE warehouse.fact_pagos (
    fact_pago_sk        BIGSERIAL PRIMARY KEY,
    pago_id             BIGINT       NOT NULL UNIQUE,          -- [M-04]
    pedido_id           BIGINT       NOT NULL,
    cliente_sk          INT REFERENCES warehouse.dim_cliente(cliente_sk),
    fecha_sk            INT REFERENCES warehouse.dim_fecha(fecha_sk),
    metodo_pago_sk      INT REFERENCES warehouse.dim_metodo_pago(metodo_pago_sk),
    monto               NUMERIC(12,2),
    estado              VARCHAR(20),
    proveedor_pago      VARCHAR(50),
    creado_en           TIMESTAMPTZ,
    cargado_en          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE warehouse.fact_movimientos_inventario (
    fact_mov_sk         BIGSERIAL PRIMARY KEY,
    movimiento_id       BIGINT       NOT NULL UNIQUE,          -- [M-04]
    almacen_sk          INT REFERENCES warehouse.dim_almacen(almacen_sk),
    variante_sk         INT REFERENCES warehouse.dim_variante(variante_sk),
    fecha_sk            INT REFERENCES warehouse.dim_fecha(fecha_sk),
    tipo_movimiento     VARCHAR(30),
    cantidad            INT,
    tipo_referencia     VARCHAR(30),
    referencia_id       BIGINT,
    fecha_movimiento    TIMESTAMPTZ,
    cargado_en          TIMESTAMPTZ  DEFAULT NOW()
);

-- [M-01] fact_envios: un registro por envío
CREATE TABLE warehouse.fact_envios (
    fact_envio_sk       BIGSERIAL PRIMARY KEY,
    envio_id            BIGINT       NOT NULL UNIQUE,          -- [M-04]
    pedido_id           BIGINT       NOT NULL,
    almacen_sk          INT REFERENCES warehouse.dim_almacen(almacen_sk),
    fecha_envio_sk      INT REFERENCES warehouse.dim_fecha(fecha_sk),
    fecha_entrega_sk    INT REFERENCES warehouse.dim_fecha(fecha_sk),
    estado_envio        VARCHAR(30),
    transportista       VARCHAR(100),
    dias_entrega        INT,
    cargado_en          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE warehouse.fact_carritos (
    fact_carrito_sk     BIGSERIAL PRIMARY KEY,
    carrito_id          BIGINT       NOT NULL UNIQUE,          -- [M-04]
    cliente_sk          INT REFERENCES warehouse.dim_cliente(cliente_sk),
    fecha_sk            INT REFERENCES warehouse.dim_fecha(fecha_sk),
    estado              VARCHAR(20),
    num_items           INT,
    valor_total         NUMERIC(12,2),
    convertido          BOOLEAN,
    cargado_en          TIMESTAMPTZ  DEFAULT NOW()
);

-- [M-02] fact_devoluciones: analítica de tasa y motivos
CREATE TABLE warehouse.fact_devoluciones (
    fact_devolucion_sk  BIGSERIAL PRIMARY KEY,
    devolucion_id       BIGINT       NOT NULL UNIQUE,          -- [M-04]
    pedido_id           BIGINT       NOT NULL,
    variante_sk         INT REFERENCES warehouse.dim_variante(variante_sk),
    producto_sk         INT REFERENCES warehouse.dim_producto(producto_sk),
    cliente_sk          INT REFERENCES warehouse.dim_cliente(cliente_sk),
    fecha_sk            INT REFERENCES warehouse.dim_fecha(fecha_sk),
    motivo              VARCHAR(50),
    estado              VARCHAR(20),
    cantidad_devuelta   INT,
    cargado_en          TIMESTAMPTZ  DEFAULT NOW()
);

-- [M-06] DLQ: eventos sin handler o que fallaron tras MAX_RETRIES.
-- Nunca se descartan silenciosamente; permiten auditoría y recuperación manual.
CREATE TABLE warehouse.dlq_eventos (
    dlq_id              BIGSERIAL PRIMARY KEY,
    evento_id           BIGINT       NOT NULL UNIQUE,
    tipo_evento         VARCHAR(60),
    payload             JSONB,
    -- 'sin_handler' | 'error_max_reintentos'
    motivo              VARCHAR(50)  NOT NULL
        CHECK (motivo IN ('sin_handler','error_max_reintentos')),
    error_detalle       TEXT,
    creado_en           TIMESTAMPTZ  DEFAULT NOW()
);

-- [M-07] marcas_agua: usada activamente por el procesador OLAP para catchup
CREATE TABLE warehouse.marcas_agua (
    tabla_fuente    VARCHAR(100) PRIMARY KEY,
    ultima_carga    TIMESTAMPTZ  DEFAULT '2020-01-01',
    actualizado_en  TIMESTAMPTZ  DEFAULT NOW()
);

INSERT INTO warehouse.marcas_agua (tabla_fuente) VALUES
    ('pedidos'), ('pagos'), ('clientes'), ('productos'), ('variantes'),
    ('envios'), ('devoluciones'), ('movimientos_inventario')
ON CONFLICT DO NOTHING;

-- =============================================================================
-- ÍNDICES — optimizados para DirectQuery desde Superset
-- =============================================================================

CREATE INDEX idx_fp_cliente_sk      ON warehouse.fact_pedidos(cliente_sk);
CREATE INDEX idx_fp_fecha_sk        ON warehouse.fact_pedidos(fecha_sk);
CREATE INDEX idx_fp_canal_sk        ON warehouse.fact_pedidos(canal_sk);
CREATE INDEX idx_fp_estado_sk       ON warehouse.fact_pedidos(estado_pedido_sk);
CREATE INDEX idx_fp_pedido_id       ON warehouse.fact_pedidos(pedido_id);

CREATE INDEX idx_fdp_pedido_id      ON warehouse.fact_detalle_pedidos(pedido_id);
CREATE INDEX idx_fdp_variante_sk    ON warehouse.fact_detalle_pedidos(variante_sk);
CREATE INDEX idx_fdp_producto_sk    ON warehouse.fact_detalle_pedidos(producto_sk);
CREATE INDEX idx_fdp_cliente_sk     ON warehouse.fact_detalle_pedidos(cliente_sk);
CREATE INDEX idx_fdp_fecha_sk       ON warehouse.fact_detalle_pedidos(fecha_sk);

CREATE INDEX idx_fpago_pedido_id    ON warehouse.fact_pagos(pedido_id);
CREATE INDEX idx_fpago_fecha_sk     ON warehouse.fact_pagos(fecha_sk);
CREATE INDEX idx_fpago_metodo_sk    ON warehouse.fact_pagos(metodo_pago_sk);

CREATE INDEX idx_fmov_almacen_sk    ON warehouse.fact_movimientos_inventario(almacen_sk);
CREATE INDEX idx_fmov_variante_sk   ON warehouse.fact_movimientos_inventario(variante_sk);
CREATE INDEX idx_fmov_fecha_sk      ON warehouse.fact_movimientos_inventario(fecha_sk);

CREATE INDEX idx_fenv_pedido_id     ON warehouse.fact_envios(pedido_id);
CREATE INDEX idx_fenv_almacen_sk    ON warehouse.fact_envios(almacen_sk);
CREATE INDEX idx_fenv_fecha_env_sk  ON warehouse.fact_envios(fecha_envio_sk);

CREATE INDEX idx_fdev_pedido_id     ON warehouse.fact_devoluciones(pedido_id);
CREATE INDEX idx_fdev_cliente_sk    ON warehouse.fact_devoluciones(cliente_sk);
CREATE INDEX idx_fdev_fecha_sk      ON warehouse.fact_devoluciones(fecha_sk);

-- [v3-W03] Índices DLQ para monitoreo operacional
CREATE INDEX idx_dlq_tipo_evento    ON warehouse.dlq_eventos(tipo_evento);
CREATE INDEX idx_dlq_creado_en      ON warehouse.dlq_eventos(creado_en DESC);

DO $$ BEGIN RAISE NOTICE '02_warehouse.sql completado. Ejecuta: 03_auth.sql'; END $$;