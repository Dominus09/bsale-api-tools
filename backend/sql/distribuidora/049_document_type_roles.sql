-- Semántica ERP de los tipos de documento Bsale, por empresa.
--
-- Identidad: (company_id, document_type_id), con document_type_id = id técnico de Bsale
-- (``bsale.document_types.bsale_id``). El mismo id puede significar otra cosa en otra
-- empresa: no hay mapping global.
--
-- Esta tabla NO guarda el nombre del tipo: el nombre visible se lee siempre de
-- ``bsale.document_types.name`` tal como lo entrega Bsale. ``expected_code_sii`` es el
-- código SII esperado del tipo (validación contra ``bsale.document_types.code_sii``) y no
-- es un document_type_id: code_sii 33 = factura (document_type_id 6).
--
-- Seed con ON CONFLICT DO NOTHING: el runner reaplica este archivo en cada corrida y no
-- debe pisar cambios posteriores (p. ej. activar una variante legacy).

CREATE TABLE IF NOT EXISTS distribuidora.document_type_roles (
    company_id        BIGINT      NOT NULL,
    document_type_id  BIGINT      NOT NULL,
    role              TEXT        NOT NULL,
    active            BOOLEAN     NOT NULL DEFAULT TRUE,
    expected_code_sii INT,
    notes             TEXT,
    source            TEXT        NOT NULL DEFAULT 'seed:049',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_distribuidora_document_type_roles
        PRIMARY KEY (company_id, document_type_id),
    CONSTRAINT ck_distribuidora_document_type_roles_role CHECK (role IN (
        'BOLETA', 'FACTURA', 'GUIA_DESPACHO', 'NOTA_CREDITO', 'COTIZACION', 'ORDEN_COMPRA'
    ))
);
-- +go

COMMENT ON TABLE distribuidora.document_type_roles IS
    'Rol ERP por (company_id, document_type_id Bsale). Sin nombre: usar bsale.document_types.name.';
-- +go
COMMENT ON COLUMN distribuidora.document_type_roles.expected_code_sii IS
    'Código SII esperado (validación contra bsale.document_types.code_sii); no es un document_type_id.';
-- +go

-- Company 3 (La Quillotana): tipos operativos activos.
INSERT INTO distribuidora.document_type_roles
    (company_id, document_type_id, role, active, expected_code_sii)
VALUES
    (3,  1, 'BOLETA',        TRUE, 39),
    (3,  6, 'FACTURA',       TRUE, 33),
    (3,  8, 'GUIA_DESPACHO', TRUE, 52),
    (3,  9, 'NOTA_CREDITO',  TRUE, 61),
    (3, 26, 'COTIZACION',    TRUE, NULL),
    (3, 33, 'ORDEN_COMPRA',  TRUE, NULL)
ON CONFLICT (company_id, document_type_id) DO NOTHING;
-- +go

-- Company 3: variantes legacy reconocidas pero fuera del alcance operacional (inactivas).
-- El tipo 25 (vale de venta) queda sin rol hasta una decisión de negocio explícita.
INSERT INTO distribuidora.document_type_roles
    (company_id, document_type_id, role, active, expected_code_sii, notes)
VALUES
    (3, 10, 'BOLETA',        FALSE, 35,   'variante legacy; inactiva hasta decisión de negocio'),
    (3, 15, 'FACTURA',       FALSE, 34,   'variante legacy; inactiva hasta decisión de negocio'),
    (3, 27, 'BOLETA',        FALSE, 41,   'variante legacy; inactiva hasta decisión de negocio'),
    (3, 30, 'ORDEN_COMPRA',  FALSE, NULL, 'variante legacy; inactiva hasta decisión de negocio'),
    (3, 32, 'NOTA_CREDITO',  FALSE, NULL, 'variante legacy; inactiva hasta decisión de negocio')
ON CONFLICT (company_id, document_type_id) DO NOTHING;
-- +go
