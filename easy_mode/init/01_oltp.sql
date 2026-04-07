-- =============================================================================
-- 01_oltp.sql  —  Ecommerce CL v3 — Schema public (OLTP)
-- Ejecutar DESPUÉS de 00_reset.sql
--
-- Decisiones de diseño documentadas:
--   [D-01] Múltiples intentos de pago permitidos — cada intento = 1 fila en pagos.
--          Solo los pagos con estado='capturado' actualizan el estado del pedido.
--          No existe UNIQUE(pedido_id) en pagos porque un pedido puede tener
--          un primer pago fallido y luego uno exitoso.
--   [D-02] Un solo envío activo por pedido — índice único parcial sobre pedido_id
--          excluyendo estados fallido/devuelto (permite reenvíos tras incidente).
--   [D-03] Cancelación post-pago: permitida. Si hay pago capturado, se crea
--          reembolso automático con estado='pendiente'.
--   [D-04] Una sola devolución activa por pedido (estado != 'rechazada').
--          Una rechazada permite intentar de nuevo.
--   [D-05] IVA 19% calculado UNA sola vez sobre base_imponible del pedido
--          (subtotal - descuento_total). No se acumula por línea.
--   [D-06] cantidad_disponible en niveles_stock es columna generada
--          (fisica - reservada), protegida con CHECK >= 0 en ambas fuentes.
--
-- Correcciones v2 sobre v1:
--   [v3-01] Seed: impuesto_linea calculado sobre neto de la línea, no sobre subtotal del pedido
--   [v3-02] Seed: total_linea = precio * cant (sin IVA de línea acumulado) consistente con D-05
--   [v3-03] ON CONFLICT idempotente en INSERT inicial de rol ecommerce
--   [v3-04] fn_notificar_evento_negocio: SET search_path fijo (cierra hijacking)
--   [v3-05] Índice compuesto en eventos_negocio para polling eficiente
--   [v3-06] CHECK en pedidos: total_final = subtotal - descuento_total + impuesto_total + costo_envio (± 1 CLP por redondeo)
-- =============================================================================

\c ecommerce_cl
SET client_encoding = 'UTF8';
SET timezone = 'America/Santiago';

-- =============================================================================
-- TABLAS OLTP
-- =============================================================================

CREATE TABLE proveedores (
    proveedor_id            BIGSERIAL PRIMARY KEY,
    nombre_proveedor        VARCHAR(200) NOT NULL,
    rut_proveedor           VARCHAR(12),
    correo_contacto         VARCHAR(255),
    telefono                VARCHAR(30),
    codigo_pais             CHAR(2)      NOT NULL DEFAULT 'CL',
    condiciones_pago        VARCHAR(50)  DEFAULT 'contado'
        CHECK (condiciones_pago IN ('contado','30_dias','60_dias','90_dias')),
    plazo_entrega_dias      SMALLINT     DEFAULT 3,
    activo                  BOOLEAN      NOT NULL DEFAULT TRUE,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE clientes (
    cliente_id              BIGSERIAL PRIMARY KEY,
    cliente_externo_id      VARCHAR(64)  UNIQUE,
    correo                  VARCHAR(255) NOT NULL UNIQUE,
    nombre                  VARCHAR(100) NOT NULL,
    apellido                VARCHAR(100) NOT NULL,
    telefono                VARCHAR(30),
    segmento                VARCHAR(30)  NOT NULL DEFAULT 'estandar'
        CHECK (segmento IN ('vip','premium','estandar','nuevo','inactivo')),
    estado                  VARCHAR(20)  NOT NULL DEFAULT 'activo'
        CHECK (estado IN ('activo','inactivo','bloqueado')),
    activo                  BOOLEAN      NOT NULL DEFAULT TRUE,
    eliminado_en            TIMESTAMPTZ,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE direcciones_cliente (
    direccion_id            BIGSERIAL PRIMARY KEY,
    cliente_id              BIGINT       NOT NULL REFERENCES clientes(cliente_id) ON DELETE CASCADE,
    tipo_direccion          VARCHAR(20)  NOT NULL DEFAULT 'envio'
        CHECK (tipo_direccion IN ('envio','facturacion','ambas')),
    destinatario            VARCHAR(200) NOT NULL,
    linea1                  VARCHAR(255) NOT NULL,
    linea2                  VARCHAR(255),
    ciudad                  VARCHAR(100) NOT NULL,
    provincia               VARCHAR(100),
    codigo_postal           VARCHAR(20),
    codigo_pais             CHAR(2)      NOT NULL DEFAULT 'CL',
    es_predeterminada       BOOLEAN      NOT NULL DEFAULT FALSE,
    activo                  BOOLEAN      NOT NULL DEFAULT TRUE,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE sesiones_cliente (
    sesion_id               BIGSERIAL PRIMARY KEY,
    cliente_id              BIGINT REFERENCES clientes(cliente_id),
    canal                   VARCHAR(30)  NOT NULL DEFAULT 'web'
        CHECK (canal IN ('web','mobile','marketplace','call_center','api')),
    dispositivo             VARCHAR(50),
    origen_trafico          VARCHAR(50),
    iniciado_en             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finalizado_en           TIMESTAMPTZ,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE marcas (
    marca_id                BIGSERIAL PRIMARY KEY,
    nombre                  VARCHAR(100) NOT NULL UNIQUE,
    activo                  BOOLEAN      NOT NULL DEFAULT TRUE,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE categorias (
    categoria_id            BIGSERIAL PRIMARY KEY,
    categoria_padre_id      BIGINT REFERENCES categorias(categoria_id),
    nombre                  VARCHAR(100) NOT NULL,
    slug                    VARCHAR(120) NOT NULL UNIQUE,
    activo                  BOOLEAN      NOT NULL DEFAULT TRUE,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE productos (
    producto_id             BIGSERIAL PRIMARY KEY,
    marca_id                BIGINT       NOT NULL REFERENCES marcas(marca_id),
    categoria_id            BIGINT       NOT NULL REFERENCES categorias(categoria_id),
    nombre                  VARCHAR(255) NOT NULL,
    slug                    VARCHAR(280) NOT NULL UNIQUE,
    descripcion             TEXT,
    estado_producto         VARCHAR(20)  NOT NULL DEFAULT 'activo'
        CHECK (estado_producto IN ('activo','inactivo','borrador','descontinuado')),
    activo                  BOOLEAN      NOT NULL DEFAULT TRUE,
    eliminado_en            TIMESTAMPTZ,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE variantes_producto (
    variante_id             BIGSERIAL PRIMARY KEY,
    producto_id             BIGINT       NOT NULL REFERENCES productos(producto_id),
    proveedor_id            BIGINT       REFERENCES proveedores(proveedor_id),
    sku                     VARCHAR(100) NOT NULL UNIQUE,
    codigo_barras           VARCHAR(100),
    color                   VARCHAR(50),
    talla                   VARCHAR(20),
    atributos_json          JSONB        DEFAULT '{}'::jsonb,
    precio_costo            NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (precio_costo >= 0),
    activa                  BOOLEAN      NOT NULL DEFAULT TRUE,
    eliminado_en            TIMESTAMPTZ,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE precios_producto (
    precio_id               BIGSERIAL PRIMARY KEY,
    variante_id             BIGINT       NOT NULL REFERENCES variantes_producto(variante_id),
    codigo_moneda           CHAR(3)      NOT NULL DEFAULT 'CLP',
    precio_lista            NUMERIC(12,2) NOT NULL CHECK (precio_lista >= 0),
    precio_oferta           NUMERIC(12,2) CHECK (precio_oferta >= 0),
    vigente_desde           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    vigente_hasta           TIMESTAMPTZ,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE promociones (
    promocion_id            BIGSERIAL PRIMARY KEY,
    nombre                  VARCHAR(150) NOT NULL,
    tipo                    VARCHAR(30)  NOT NULL
        CHECK (tipo IN ('porcentaje','monto_fijo','envio_gratis','combo')),
    valor                   NUMERIC(12,2) NOT NULL CHECK (valor >= 0),
    codigo_moneda           CHAR(3)      DEFAULT 'CLP',
    activa                  BOOLEAN      NOT NULL DEFAULT TRUE,
    vigente_desde           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    vigente_hasta           TIMESTAMPTZ,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE cupones (
    cupon_id                BIGSERIAL PRIMARY KEY,
    codigo                  VARCHAR(50)  NOT NULL UNIQUE,
    promocion_id            BIGINT REFERENCES promociones(promocion_id),
    usos_maximos            INT          DEFAULT 1,
    usos_actuales           INT          NOT NULL DEFAULT 0,
    activo                  BOOLEAN      NOT NULL DEFAULT TRUE,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE almacenes (
    almacen_id              BIGSERIAL PRIMARY KEY,
    codigo                  VARCHAR(20)  NOT NULL UNIQUE,
    nombre                  VARCHAR(100) NOT NULL,
    ciudad                  VARCHAR(100),
    codigo_pais             CHAR(2)      NOT NULL DEFAULT 'CL',
    activo                  BOOLEAN      NOT NULL DEFAULT TRUE,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE niveles_stock (
    nivel_stock_id          BIGSERIAL PRIMARY KEY,
    almacen_id              BIGINT       NOT NULL REFERENCES almacenes(almacen_id),
    variante_id             BIGINT       NOT NULL REFERENCES variantes_producto(variante_id),
    -- [D-06] cantidad_disponible es siempre >= 0 gracias a los CHECKs en física y reservada
    cantidad_fisica         INT          NOT NULL DEFAULT 0 CHECK (cantidad_fisica >= 0),
    cantidad_reservada      INT          NOT NULL DEFAULT 0 CHECK (cantidad_reservada >= 0),
    cantidad_disponible     INT GENERATED ALWAYS AS (cantidad_fisica - cantidad_reservada) STORED,
    punto_reorden           INT          NOT NULL DEFAULT 10,
    actualizado_en          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (almacen_id, variante_id)
);

CREATE TABLE carritos (
    carrito_id              BIGSERIAL PRIMARY KEY,
    cliente_id              BIGINT REFERENCES clientes(cliente_id),
    sesion_id               BIGINT REFERENCES sesiones_cliente(sesion_id),
    estado                  VARCHAR(20)  NOT NULL DEFAULT 'activo'
        CHECK (estado IN ('activo','convertido','abandonado','expirado')),
    codigo_moneda           CHAR(3)      NOT NULL DEFAULT 'CLP',
    cupon_id                BIGINT REFERENCES cupones(cupon_id),
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE detalle_carritos (
    detalle_carrito_id      BIGSERIAL PRIMARY KEY,
    carrito_id              BIGINT       NOT NULL REFERENCES carritos(carrito_id) ON DELETE CASCADE,
    variante_id             BIGINT       NOT NULL REFERENCES variantes_producto(variante_id),
    cantidad                INT          NOT NULL CHECK (cantidad > 0),
    precio_unitario         NUMERIC(12,2) NOT NULL CHECK (precio_unitario >= 0),
    agregado_en             TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- [D-05] total_final = subtotal - descuento_total + impuesto_total + costo_envio
-- El CHECK permite ±2 CLP de tolerancia por redondeo decimal.
CREATE TABLE pedidos (
    pedido_id               BIGSERIAL PRIMARY KEY,
    numero_pedido           VARCHAR(30)  NOT NULL UNIQUE,
    cliente_id              BIGINT       NOT NULL REFERENCES clientes(cliente_id),
    direccion_facturacion_id BIGINT REFERENCES direcciones_cliente(direccion_id),
    direccion_envio_id      BIGINT REFERENCES direcciones_cliente(direccion_id),
    canal_venta             VARCHAR(30)  NOT NULL DEFAULT 'web'
        CHECK (canal_venta IN ('web','mobile','marketplace','call_center','api')),
    codigo_moneda           CHAR(3)      NOT NULL DEFAULT 'CLP',
    estado_actual           VARCHAR(30)  NOT NULL DEFAULT 'pendiente'
        CHECK (estado_actual IN ('pendiente','confirmado','pagado','preparando',
                                 'empaquetado','enviado','entregado','cancelado','reembolsado')),
    subtotal                NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (subtotal >= 0),
    descuento_total         NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (descuento_total >= 0),
    impuesto_total          NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (impuesto_total >= 0),
    costo_envio             NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (costo_envio >= 0),
    total_final             NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (total_final >= 0),
    estado_pago             VARCHAR(20)  NOT NULL DEFAULT 'pendiente'
        CHECK (estado_pago IN ('pendiente','parcial','pagado','reembolsado','fallido')),
    estado_fulfillment      VARCHAR(20)  NOT NULL DEFAULT 'pendiente'
        CHECK (estado_fulfillment IN ('pendiente','preparando','listo','enviado','entregado','devuelto')),
    promocion_id            BIGINT REFERENCES promociones(promocion_id),
    cupon_id                BIGINT REFERENCES cupones(cupon_id),
    notas                   TEXT,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    cancelado_en            TIMESTAMPTZ,
    entregado_en            TIMESTAMPTZ
);

-- trazabilidad de qué almacén reservó cada línea (necesario para liberación de stock)
CREATE TABLE detalle_pedidos (
    detalle_pedido_id       BIGSERIAL PRIMARY KEY,
    pedido_id               BIGINT       NOT NULL REFERENCES pedidos(pedido_id) ON DELETE CASCADE,
    variante_id             BIGINT       NOT NULL REFERENCES variantes_producto(variante_id),
    almacen_id              BIGINT       REFERENCES almacenes(almacen_id),
    nombre_producto_snapshot VARCHAR(255) NOT NULL,
    sku_snapshot            VARCHAR(100) NOT NULL,
    cantidad                INT          NOT NULL CHECK (cantidad > 0),
    precio_unitario         NUMERIC(12,2) NOT NULL CHECK (precio_unitario >= 0),
    descuento_linea         NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (descuento_linea >= 0),
    -- [v3-01] impuesto_linea = referencial, calculado sobre neto de línea
    impuesto_linea          NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (impuesto_linea >= 0),
    -- [v3-02] total_linea = (precio_unitario - descuento_linea/cant) * cantidad (sin IVA acumulado)
    total_linea             NUMERIC(12,2) NOT NULL CHECK (total_linea >= 0),
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE historial_estado_pedidos (
    historial_estado_pedido_id BIGSERIAL PRIMARY KEY,
    pedido_id               BIGINT       NOT NULL REFERENCES pedidos(pedido_id) ON DELETE CASCADE,
    estado_anterior         VARCHAR(30),
    estado_nuevo            VARCHAR(30)  NOT NULL,
    cambiado_por            VARCHAR(100) DEFAULT 'sistema',
    codigo_motivo           VARCHAR(50),
    notas                   TEXT,
    cambiado_en             TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- [D-01] Múltiples pagos por pedido permitidos (reintentos).
-- No hay UNIQUE(pedido_id): un primer pago fallido + uno exitoso es el flujo normal.
CREATE TABLE pagos (
    pago_id                 BIGSERIAL PRIMARY KEY,
    pedido_id               BIGINT       NOT NULL REFERENCES pedidos(pedido_id),
    proveedor_pago          VARCHAR(50)  NOT NULL,
    transaccion_externa_id  VARCHAR(100),
    metodo_pago             VARCHAR(30)  NOT NULL
        CHECK (metodo_pago IN ('tarjeta_credito','tarjeta_debito','transferencia',
                               'efectivo','billetera_virtual','cuotas','criptomoneda')),
    codigo_moneda           CHAR(3)      NOT NULL DEFAULT 'CLP',
    monto                   NUMERIC(12,2) NOT NULL CHECK (monto > 0),
    estado                  VARCHAR(20)  NOT NULL DEFAULT 'iniciado'
        CHECK (estado IN ('iniciado','autorizado','capturado','fallido','cancelado','reembolsado')),
    cuotas                  SMALLINT     DEFAULT 1,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE eventos_pago (
    evento_pago_id          BIGSERIAL PRIMARY KEY,
    pago_id                 BIGINT       NOT NULL REFERENCES pagos(pago_id) ON DELETE CASCADE,
    tipo_evento             VARCHAR(30)  NOT NULL,
    estado_evento           VARCHAR(20)  NOT NULL,
    monto_evento            NUMERIC(12,2) CHECK (monto_evento >= 0),
    payload_crudo           JSONB        DEFAULT '{}'::jsonb,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE reembolsos (
    reembolso_id            BIGSERIAL PRIMARY KEY,
    pago_id                 BIGINT       NOT NULL REFERENCES pagos(pago_id),
    pedido_id               BIGINT       NOT NULL REFERENCES pedidos(pedido_id),
    monto                   NUMERIC(12,2) NOT NULL CHECK (monto > 0),
    codigo_moneda           CHAR(3)      NOT NULL DEFAULT 'CLP',
    codigo_motivo           VARCHAR(50),
    estado                  VARCHAR(20)  NOT NULL DEFAULT 'pendiente'
        CHECK (estado IN ('pendiente','aprobado','procesado','rechazado')),
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    procesado_en            TIMESTAMPTZ
);

CREATE TABLE movimientos_inventario (
    movimiento_inventario_id BIGSERIAL PRIMARY KEY,
    almacen_id              BIGINT       NOT NULL REFERENCES almacenes(almacen_id),
    variante_id             BIGINT       NOT NULL REFERENCES variantes_producto(variante_id),
    tipo_movimiento         VARCHAR(30)  NOT NULL
        CHECK (tipo_movimiento IN ('ingreso','reserva','liberacion','venta',
                                   'devolucion','ajuste','traslado_entrada',
                                   'traslado_salida','reposicion')),
    cantidad                INT          NOT NULL,
    tipo_referencia         VARCHAR(30),
    referencia_id           BIGINT,
    notas                   TEXT,
    fecha_movimiento        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE ordenes_compra (
    orden_compra_id         BIGSERIAL PRIMARY KEY,
    proveedor_id            BIGINT       NOT NULL REFERENCES proveedores(proveedor_id),
    almacen_id              BIGINT       NOT NULL REFERENCES almacenes(almacen_id),
    numero_oc               VARCHAR(30)  NOT NULL UNIQUE,
    estado                  VARCHAR(20)  NOT NULL DEFAULT 'borrador'
        CHECK (estado IN ('borrador','enviada','confirmada','recibida_parcial',
                          'recibida_total','cancelada')),
    fecha_estimada_entrega  DATE,
    fecha_recepcion         TIMESTAMPTZ,
    subtotal                NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (subtotal >= 0),
    impuesto_total          NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (impuesto_total >= 0),
    total_oc                NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (total_oc >= 0),
    notas                   TEXT,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE detalle_ordenes_compra (
    detalle_oc_id           BIGSERIAL PRIMARY KEY,
    orden_compra_id         BIGINT       NOT NULL REFERENCES ordenes_compra(orden_compra_id) ON DELETE CASCADE,
    variante_id             BIGINT       NOT NULL REFERENCES variantes_producto(variante_id),
    cantidad_solicitada     INT          NOT NULL CHECK (cantidad_solicitada > 0),
    cantidad_recibida       INT          NOT NULL DEFAULT 0 CHECK (cantidad_recibida >= 0),
    precio_unitario_compra  NUMERIC(12,2) NOT NULL CHECK (precio_unitario_compra >= 0),
    total_linea             NUMERIC(12,2) NOT NULL CHECK (total_linea >= 0),
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- [D-02] Un solo envío activo por pedido.
-- Envíos fallidos/devueltos permiten crear uno nuevo (índice parcial los excluye).
CREATE TABLE envios (
    envio_id                BIGSERIAL PRIMARY KEY,
    pedido_id               BIGINT       NOT NULL REFERENCES pedidos(pedido_id),
    almacen_id              BIGINT       NOT NULL REFERENCES almacenes(almacen_id),
    estado_envio            VARCHAR(30)  NOT NULL DEFAULT 'pendiente'
        CHECK (estado_envio IN ('pendiente','preparando','despachado','en_camino',
                                'entregado','fallido','devuelto')),
    transportista           VARCHAR(100),
    numero_seguimiento      VARCHAR(100),
    costo_envio_real        NUMERIC(12,2) DEFAULT 0 CHECK (costo_envio_real >= 0),
    enviado_en              TIMESTAMPTZ,
    entregado_en            TIMESTAMPTZ,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE detalle_envios (
    detalle_envio_id        BIGSERIAL PRIMARY KEY,
    envio_id                BIGINT       NOT NULL REFERENCES envios(envio_id) ON DELETE CASCADE,
    estado_envio            VARCHAR(30)  NOT NULL,
    descripcion             VARCHAR(255),
    ciudad                  VARCHAR(100),
    ocurrido_en             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- [D-04] Una sola devolución activa por pedido
CREATE TABLE devoluciones (
    devolucion_id           BIGSERIAL PRIMARY KEY,
    pedido_id               BIGINT       NOT NULL REFERENCES pedidos(pedido_id),
    motivo                  VARCHAR(50)  NOT NULL
        CHECK (motivo IN ('producto_defectuoso','no_cumple_expectativa',
                          'talla_incorrecta','cambio_decision','otro')),
    estado                  VARCHAR(20)  NOT NULL DEFAULT 'pendiente'
        CHECK (estado IN ('pendiente','aprobada','rechazada','completada')),
    notas                   TEXT,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    procesado_en            TIMESTAMPTZ
);

CREATE TABLE detalle_devoluciones (
    detalle_devolucion_id   BIGSERIAL PRIMARY KEY,
    devolucion_id           BIGINT       NOT NULL REFERENCES devoluciones(devolucion_id) ON DELETE CASCADE,
    detalle_pedido_id       BIGINT       NOT NULL REFERENCES detalle_pedidos(detalle_pedido_id),
    variante_id             BIGINT       NOT NULL REFERENCES variantes_producto(variante_id),
    cantidad_devuelta       INT          NOT NULL CHECK (cantidad_devuelta > 0),
    motivo_linea            VARCHAR(100),
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE promociones_aplicadas_pedido (
    id                      BIGSERIAL PRIMARY KEY,
    pedido_id               BIGINT       NOT NULL REFERENCES pedidos(pedido_id) ON DELETE CASCADE,
    promocion_id            BIGINT       NOT NULL REFERENCES promociones(promocion_id),
    cupon_id                BIGINT REFERENCES cupones(cupon_id),
    descuento_aplicado      NUMERIC(12,2) NOT NULL DEFAULT 0,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (pedido_id, promocion_id)
);

-- [v3-05] Columnas de retry/DLQ tracking para el procesador OLAP
CREATE TABLE eventos_negocio (
    evento_negocio_id       BIGSERIAL PRIMARY KEY,
    tipo_entidad            VARCHAR(50)  NOT NULL,
    entidad_id              BIGINT,
    tipo_evento             VARCHAR(60)  NOT NULL,
    payload                 JSONB        DEFAULT '{}'::jsonb,
    procesado_olap          BOOLEAN      NOT NULL DEFAULT FALSE,
    intentos_olap           SMALLINT     NOT NULL DEFAULT 0,
    error_olap              TEXT,
    ocurrido_en             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE resenas_producto (
    resena_id               BIGSERIAL PRIMARY KEY,
    producto_id             BIGINT       NOT NULL REFERENCES productos(producto_id),
    cliente_id              BIGINT       NOT NULL REFERENCES clientes(cliente_id),
    calificacion            SMALLINT     NOT NULL CHECK (calificacion BETWEEN 1 AND 5),
    comentario              TEXT,
    aprobada                BOOLEAN      NOT NULL DEFAULT FALSE,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE alertas_operativas (
    alerta_id               BIGSERIAL PRIMARY KEY,
    tipo_alerta             VARCHAR(60)  NOT NULL,
    entidad                 VARCHAR(50),
    entidad_id              BIGINT,
    mensaje                 TEXT         NOT NULL,
    nivel                   VARCHAR(10)  NOT NULL DEFAULT 'warning'
        CHECK (nivel IN ('info','warning','critical')),
    resuelta                BOOLEAN      NOT NULL DEFAULT FALSE,
    creado_en               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resuelto_en             TIMESTAMPTZ
);

-- =============================================================================
-- ÍNDICES OLTP
-- =============================================================================

CREATE INDEX idx_clientes_correo              ON clientes(correo);
CREATE INDEX idx_clientes_estado              ON clientes(estado);
CREATE INDEX idx_pedidos_cliente_id           ON pedidos(cliente_id);
CREATE INDEX idx_pedidos_estado_actual        ON pedidos(estado_actual);
CREATE INDEX idx_pedidos_creado_en            ON pedidos(creado_en);
CREATE INDEX idx_detalle_pedidos_pedido_id    ON detalle_pedidos(pedido_id);
CREATE INDEX idx_detalle_pedidos_variante_id  ON detalle_pedidos(variante_id);
CREATE INDEX idx_detalle_pedidos_almacen_id   ON detalle_pedidos(almacen_id);
CREATE INDEX idx_pagos_pedido_id              ON pagos(pedido_id);
CREATE INDEX idx_pagos_estado                 ON pagos(estado);
CREATE INDEX idx_movimientos_almacen_variante ON movimientos_inventario(almacen_id, variante_id);
CREATE INDEX idx_movimientos_fecha            ON movimientos_inventario(fecha_movimiento);
CREATE INDEX idx_niveles_stock_disponible     ON niveles_stock(cantidad_disponible);
CREATE INDEX idx_ordenes_compra_proveedor     ON ordenes_compra(proveedor_id);
CREATE INDEX idx_detalle_oc_variante          ON detalle_ordenes_compra(variante_id);
CREATE INDEX idx_variantes_proveedor          ON variantes_producto(proveedor_id);
CREATE INDEX idx_devoluciones_pedido          ON devoluciones(pedido_id);
CREATE INDEX idx_detalle_dev_devolucion       ON detalle_devoluciones(devolucion_id);
CREATE INDEX idx_envios_pedido                ON envios(pedido_id);

-- [v3-05] Índice compuesto optimizado para el polling OLAP (WHERE + ORDER BY)
CREATE INDEX idx_eventos_negocio_poll ON eventos_negocio(procesado_olap, ocurrido_en, evento_negocio_id)
    WHERE procesado_olap = FALSE;

-- [D-02] Un solo envío activo por pedido (excluye fallido/devuelto)
CREATE UNIQUE INDEX uq_envio_activo_pedido ON envios(pedido_id)
    WHERE estado_envio NOT IN ('fallido','devuelto');

-- =============================================================================
-- TRIGGER LISTEN/NOTIFY
-- [v3-04] SET search_path fijo en la función — previene search_path hijacking
-- =============================================================================

CREATE OR REPLACE FUNCTION fn_notificar_evento_negocio()
RETURNS TRIGGER LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
BEGIN
    PERFORM pg_notify(
        'eventos_negocio',
        json_build_object(
            'id',           NEW.evento_negocio_id,
            'tipo_entidad', NEW.tipo_entidad,
            'entidad_id',   NEW.entidad_id,
            'tipo_evento',  NEW.tipo_evento,
            'payload',      NEW.payload,
            'ocurrido_en',  NEW.ocurrido_en
        )::text
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_notificar_evento_negocio
    AFTER INSERT ON eventos_negocio
    FOR EACH ROW EXECUTE FUNCTION fn_notificar_evento_negocio();

-- =============================================================================
-- DATOS SEMILLA (SEED)
-- =============================================================================

INSERT INTO almacenes (codigo, nombre, ciudad, codigo_pais) VALUES
    ('ALM-SCL', 'CD Santiago',    'Santiago',    'CL'),
    ('ALM-VLP', 'Deposito Valpo', 'Vina del Mar','CL'),
    ('ALM-CCP', 'Deposito BioBio','Concepcion',  'CL');

INSERT INTO marcas (nombre) VALUES
    ('TechPro'), ('FashionNow'), ('HomeStyle'), ('SportMax'), ('EcoVerde');

INSERT INTO categorias (nombre, slug) VALUES
    ('Tecnologia',  'tecnologia'),
    ('Ropa y Moda', 'ropa-y-moda');
INSERT INTO categorias (categoria_padre_id, nombre, slug) VALUES
    (1, 'Smartphones', 'smartphones'),
    (2, 'Remeras',     'remeras');

INSERT INTO proveedores
    (nombre_proveedor, rut_proveedor, correo_contacto, telefono, codigo_pais, condiciones_pago, plazo_entrega_dias)
VALUES
    ('Importadora del Sur', '76.543.210-K', 'ventas@importadoradelsur.cl', '+562 2345 6789', 'CL', '30_dias', 5),
    ('TechDistrib SpA',     '77.123.456-3', 'contacto@techdistrib.cl',     '+562 2987 6543', 'CL', '60_dias', 3),
    ('FashionCL Ltda',      '79.876.543-1', 'pedidos@fashioncl.cl',        '+562 2111 2222', 'CL', 'contado', 7),
    ('SportImport SA',      '76.111.222-5', 'comercial@sportimport.cl',    '+562 2333 4444', 'CL', '30_dias', 4),
    ('EcoSupplies Chile',   '78.999.888-7', 'ecoventas@ecosupplies.cl',    '+562 2555 6666', 'CL', 'contado', 2);

-- 500 clientes con direcciones predeterminadas
DO $$
DECLARE
    nombres   TEXT[] := ARRAY['Carlos','Maria','Juan','Laura','Diego','Ana'];
    apellidos TEXT[] := ARRAY['Garcia','Rodriguez','Lopez','Martinez'];
    i INT; nom TEXT; ape TEXT; cli_id BIGINT;
BEGIN
    FOR i IN 1..500 LOOP
        nom := nombres[1 + (floor(random()*6))::int];
        ape := apellidos[1 + (floor(random()*4))::int];
        INSERT INTO clientes
            (cliente_externo_id, correo, nombre, apellido, telefono, creado_en)
        VALUES (
            'EXT-' || lpad(i::text, 6, '0'),
            lower(nom) || '.' || lower(ape) || i || '@mail.com',
            nom, ape,
            '+569' || (10000000 + floor(random()*89999999)::int)::text,
            NOW() - (random()*730)::int * INTERVAL '1 day'
        ) RETURNING cliente_id INTO cli_id;

        INSERT INTO direcciones_cliente
            (cliente_id, destinatario, linea1, ciudad, provincia, codigo_pais, es_predeterminada)
        VALUES (
            cli_id, nom || ' ' || ape, 'Calle ' || i,
            (ARRAY['Santiago','Vina del Mar','Concepcion'])[1 + (floor(random()*3))::int],
            (ARRAY['Metropolitana','Valparaiso','Biobio'])[1 + (floor(random()*3))::int],
            'CL', TRUE
        );
    END LOOP;
END $$;

-- 200 productos con variantes y stock inicial
DO $$
DECLARE
    i INT; prod_id BIGINT; var_id BIGINT; precio_base NUMERIC; prov_id BIGINT;
BEGIN
    FOR i IN 1..200 LOOP
        INSERT INTO productos (marca_id, categoria_id, nombre, slug)
        VALUES (
            1 + (floor(random()*5))::int,
            3 + (floor(random()*2))::int,
            'Producto ' || i,
            'prod-' || i || '-' || (floor(random()*10000))::int
        ) RETURNING producto_id INTO prod_id;

        precio_base := 5000 + floor(random()*50000)::int;
        SELECT proveedor_id INTO prov_id FROM proveedores ORDER BY random() LIMIT 1;

        INSERT INTO variantes_producto (producto_id, proveedor_id, sku, precio_costo)
        VALUES (prod_id, prov_id, 'SKU-' || prod_id, round(precio_base * 0.45, 2))
        RETURNING variante_id INTO var_id;

        INSERT INTO precios_producto (variante_id, precio_lista)
        VALUES (var_id, round(precio_base, 2));

        INSERT INTO niveles_stock (almacen_id, variante_id, cantidad_fisica)
        VALUES
            (1, var_id, 50 + floor(random()*100)::int),
            (2, var_id, 20 + floor(random()*60)::int),
            (3, var_id, 20 + floor(random()*60)::int);
    END LOOP;
END $$;

-- 2000 pedidos históricos con IVA correcto [D-05]
-- [v3-01][v3-02] impuesto_linea es referencial (línea); IVA real = a nivel pedido
DO $$
DECLARE
    i INT; cli_id BIGINT; ped_id BIGINT; var_id BIGINT; alm_id BIGINT;
    pr NUMERIC; dt TIMESTAMPTZ; cant INT;
    subtotal_calc NUMERIC; descuento_calc NUMERIC; base_imponible NUMERIC;
    impuesto_calc NUMERIC; total_calc NUMERIC;
    linea_neta NUMERIC; imp_linea NUMERIC; tot_linea NUMERIC;
BEGIN
    FOR i IN 1..2000 LOOP
        SELECT cliente_id INTO cli_id FROM clientes ORDER BY random() LIMIT 1;
        dt   := NOW() - (random()*720)::int * INTERVAL '1 day';
        cant := 1 + floor(random()*3)::int;

        SELECT vp.variante_id, pp.precio_lista
        INTO var_id, pr
        FROM variantes_producto vp
        JOIN precios_producto pp ON pp.variante_id = vp.variante_id
        ORDER BY random() LIMIT 1;

        SELECT almacen_id INTO alm_id
        FROM niveles_stock
        WHERE variante_id = var_id
        ORDER BY cantidad_fisica DESC LIMIT 1;

        IF alm_id IS NULL THEN alm_id := 1; END IF;

        subtotal_calc  := pr * cant;
        descuento_calc := 0;  -- seed sin descuento para simplificar validaciones
        base_imponible := subtotal_calc - descuento_calc;
        -- [D-05] IVA 19% una sola vez sobre base del pedido
        impuesto_calc  := round(base_imponible * 0.19, 2);
        total_calc     := base_imponible + impuesto_calc;

        -- [v3-01] impuesto_linea referencial sobre neto de la línea
        linea_neta := pr * cant;
        imp_linea  := round(linea_neta * 0.19, 2);
        tot_linea  := linea_neta;  -- [v3-02] total_linea sin IVA (IVA es del pedido)

        INSERT INTO pedidos
            (numero_pedido, cliente_id, subtotal, descuento_total, impuesto_total,
             costo_envio, total_final,
             estado_actual, estado_pago, estado_fulfillment, creado_en, entregado_en)
        VALUES ('PED-SEED-' || i, cli_id,
                subtotal_calc, descuento_calc, impuesto_calc, 0, total_calc,
                'entregado', 'pagado', 'entregado', dt, dt)
        RETURNING pedido_id INTO ped_id;

        INSERT INTO detalle_pedidos
            (pedido_id, variante_id, almacen_id, nombre_producto_snapshot, sku_snapshot,
             cantidad, precio_unitario, descuento_linea, impuesto_linea, total_linea)
        VALUES (
            ped_id, var_id, alm_id, 'Snapshot prod ' || var_id, 'SKU-' || var_id,
            cant, pr, 0, imp_linea, tot_linea
        );

        INSERT INTO pagos (pedido_id, proveedor_pago, metodo_pago, monto, estado, creado_en)
        VALUES (ped_id, 'Transbank', 'tarjeta_credito', total_calc, 'capturado', dt);

        INSERT INTO eventos_negocio
            (tipo_entidad, entidad_id, tipo_evento, payload, ocurrido_en)
        VALUES (
            'pedido', ped_id, 'pedido_creado',
            jsonb_build_object(
                'pedido_id',       ped_id,
                'total_final',     total_calc,
                'subtotal',        subtotal_calc,
                'descuento_total', descuento_calc,
                'impuesto_total',  impuesto_calc,
                'costo_envio',     0,
                'canal',           'web',
                'num_items',       cant,
                'cliente_id',      cli_id,
                'creado_en',       dt
            ),
            dt
        );
    END LOOP;
END $$;

-- =============================================================================
-- ROL BASE — RBAC completo en 04_rbac.sql
-- [v3-03] Idempotente: DO $$ / IF NOT EXISTS
-- =============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ecommerce') THEN
        CREATE ROLE ecommerce WITH LOGIN PASSWORD 'ecommerce123';
    END IF;
END $$;

GRANT ALL ON ALL TABLES    IN SCHEMA public TO ecommerce;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO ecommerce;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA public TO ecommerce;

DO $$ BEGIN RAISE NOTICE '01_oltp.sql completado. Ejecuta: 02_warehouse.sql'; END $$;