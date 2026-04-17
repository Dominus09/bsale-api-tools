-- =============================================================================
-- Sistema de gestión de rutas de vendedores (PostgreSQL, schema bsale)
-- Incluye: rutas diarias, visitas, validación geo, soporte offline e idempotencia
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bsale;

SET search_path TO bsale, public;

-- -----------------------------------------------------------------------------
-- Función: actualizar updated_at en cada UPDATE
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bsale.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END;
$$;

COMMENT ON FUNCTION bsale.set_updated_at() IS
  'Asigna updated_at automáticamente en operaciones UPDATE (auditoría y sincronización).';

-- =============================================================================
-- TABLA: rutas_dia
-- Representa la ruta planificada/ejecutada de un vendedor para un día calendario.
-- =============================================================================
CREATE TABLE bsale.rutas_dia (
  id bigserial PRIMARY KEY,

  fecha date NOT NULL,
  vendedor varchar(255) NOT NULL,

  estado varchar(32) NOT NULL DEFAULT 'en_progreso',
  CONSTRAINT chk_rutas_dia_estado CHECK (
    estado IN ('en_progreso', 'completada', 'incompleta')
  ),

  hora_inicio timestamptz NULL,
  hora_fin timestamptz NULL,

  total_clientes integer NOT NULL DEFAULT 0,
  clientes_visitados integer NOT NULL DEFAULT 0,
  clientes_pendientes integer NOT NULL DEFAULT 0,
  porcentaje_cumplimiento numeric(5, 2) NULL,

  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),

  CONSTRAINT chk_rutas_dia_clientes_no_neg CHECK (
    total_clientes >= 0
    AND clientes_visitados >= 0
    AND clientes_pendientes >= 0
  ),
  CONSTRAINT chk_rutas_dia_porcentaje CHECK (
    porcentaje_cumplimiento IS NULL
    OR (porcentaje_cumplimiento >= 0 AND porcentaje_cumplimiento <= 100)
  )
);

COMMENT ON TABLE bsale.rutas_dia IS
  'Ruta diaria de un vendedor: estado de avance, métricas de cumplimiento y ventana horaria.';

COMMENT ON COLUMN bsale.rutas_dia.id IS 'Identificador interno de la ruta del día.';
COMMENT ON COLUMN bsale.rutas_dia.fecha IS 'Día calendario de la ruta (zona horaria de negocio / planificación).';
COMMENT ON COLUMN bsale.rutas_dia.vendedor IS 'Identificador o código del vendedor en el sistema origen.';
COMMENT ON COLUMN bsale.rutas_dia.estado IS 'Ciclo de vida: en_progreso, completada, incompleta.';
COMMENT ON COLUMN bsale.rutas_dia.hora_inicio IS 'Marca temporal real de inicio de ejecución de la ruta.';
COMMENT ON COLUMN bsale.rutas_dia.hora_fin IS 'Marca temporal real de cierre de la ruta.';
COMMENT ON COLUMN bsale.rutas_dia.total_clientes IS 'Cantidad de clientes programados en la ruta.';
COMMENT ON COLUMN bsale.rutas_dia.clientes_visitados IS 'Visitas efectivas registradas (visitado).';
COMMENT ON COLUMN bsale.rutas_dia.clientes_pendientes IS 'Clientes aún no cerrados como visitados.';
COMMENT ON COLUMN bsale.rutas_dia.porcentaje_cumplimiento IS 'Indicador agregado de cumplimiento (0–100).';
COMMENT ON COLUMN bsale.rutas_dia.created_at IS 'Auditoría: creación del registro en servidor.';
COMMENT ON COLUMN bsale.rutas_dia.updated_at IS 'Auditoría: última modificación (también útil para sync incremental).';

CREATE INDEX idx_rutas_dia_fecha ON bsale.rutas_dia (fecha);
CREATE INDEX idx_rutas_dia_vendedor ON bsale.rutas_dia (vendedor);
CREATE INDEX idx_rutas_dia_vendedor_fecha ON bsale.rutas_dia (vendedor, fecha);

CREATE TRIGGER trg_rutas_dia_set_updated_at
BEFORE UPDATE ON bsale.rutas_dia
FOR EACH ROW
EXECUTE PROCEDURE bsale.set_updated_at();

-- =============================================================================
-- TABLA: visitas
-- Detalle por cliente dentro de una ruta: geo, validación, offline y sync.
-- =============================================================================
CREATE TABLE bsale.visitas (
  id bigserial PRIMARY KEY,

  ruta_id bigint NOT NULL,
  CONSTRAINT fk_visitas_ruta
    FOREIGN KEY (ruta_id)
    REFERENCES bsale.rutas_dia (id)
    ON DELETE CASCADE,

  cliente_id varchar(128) NOT NULL,

  nombre_fantasia text NULL,
  direccion text NULL,
  comuna text NULL,
  rut_clean varchar(64) NULL,

  orden_ruta integer NOT NULL,

  estado varchar(32) NOT NULL DEFAULT 'pendiente',
  CONSTRAINT chk_visitas_estado CHECK (
    estado IN ('pendiente', 'visitado', 'incidencia')
  ),

  tipo_incidencia varchar(64) NULL,
  CONSTRAINT chk_visitas_tipo_incidencia CHECK (
    tipo_incidencia IS NULL
    OR tipo_incidencia IN (
      'local cerrado',
      'sin stock',
      'no compra',
      'fuera de ruta',
      'otros',
      'atencion telefonica'
    )
  ),

  con_compra boolean NOT NULL DEFAULT false,
  observacion text NULL,
  foto_url text NULL,

  lat_cliente numeric(10, 7) NULL,
  lon_cliente numeric(10, 7) NULL,

  lat_visita numeric(10, 7) NULL,
  lon_visita numeric(10, 7) NULL,

  distancia_metros numeric(12, 2) NULL,

  validacion_estado varchar(40) NOT NULL DEFAULT 'pendiente_validacion',
  CONSTRAINT chk_visitas_validacion_estado CHECK (
    validacion_estado IN (
      'validado',
      'fuera_rango',
      'sin_gps',
      'pendiente_validacion',
      'offline'
    )
  ),

  fecha_hora_visita timestamptz NULL,

  sync_status varchar(32) NOT NULL DEFAULT 'pending_sync',
  CONSTRAINT chk_visitas_sync_status CHECK (
    sync_status IN ('synced', 'pending_sync')
  ),

  -- Idempotencia: clave generada en dispositivo (UUID/string) para evitar duplicados al reintentar sync
  local_action_id varchar(128) NOT NULL,
  CONSTRAINT uq_visitas_local_action_id UNIQUE (local_action_id),

  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),

  CONSTRAINT chk_visitas_orden_positivo CHECK (orden_ruta > 0),
  CONSTRAINT chk_visitas_distancia_no_neg CHECK (
    distancia_metros IS NULL OR distancia_metros >= 0
  )
);

COMMENT ON TABLE bsale.visitas IS
  'Visita planificada/realizada: evidencias, geolocalización, resultado de validación y estado de sincronización.';

COMMENT ON COLUMN bsale.visitas.id IS 'Identificador interno de la visita en el servidor.';
COMMENT ON COLUMN bsale.visitas.ruta_id IS 'Ruta diaria a la que pertenece la visita.';
COMMENT ON COLUMN bsale.visitas.cliente_id IS 'Identificador del cliente (varchar para integrar IDs externos o mixtos).';
COMMENT ON COLUMN bsale.visitas.orden_ruta IS 'Secuencia sugerida de visita dentro del día.';
COMMENT ON COLUMN bsale.visitas.estado IS 'pendiente, visitado o incidencia.';
COMMENT ON COLUMN bsale.visitas.tipo_incidencia IS 'Motivo cuando estado = incidencia (valores controlados).';
COMMENT ON COLUMN bsale.visitas.con_compra IS 'Indica si hubo compra en la visita.';
COMMENT ON COLUMN bsale.visitas.observacion IS 'Notas libres del vendedor.';
COMMENT ON COLUMN bsale.visitas.foto_url IS 'URL o clave de objeto de evidencia fotográfica.';
COMMENT ON COLUMN bsale.visitas.lat_cliente IS 'Latitud de referencia del cliente (catálogo / geocodificación).';
COMMENT ON COLUMN bsale.visitas.lon_cliente IS 'Longitud de referencia del cliente.';
COMMENT ON COLUMN bsale.visitas.lat_visita IS 'Latitud capturada en el momento de la visita.';
COMMENT ON COLUMN bsale.visitas.lon_visita IS 'Longitud capturada en el momento de la visita.';
COMMENT ON COLUMN bsale.visitas.distancia_metros IS 'Distancia calculada entre referencia del cliente y punto de visita.';
COMMENT ON COLUMN bsale.visitas.validacion_estado IS 'Resultado de reglas de validación geográfica y disponibilidad de GPS.';
COMMENT ON COLUMN bsale.visitas.fecha_hora_visita IS 'Momento registrado de la visita (puede venir del dispositivo offline).';
COMMENT ON COLUMN bsale.visitas.sync_status IS 'Estado de replicación servidor/dispositivo.';
COMMENT ON COLUMN bsale.visitas.local_action_id IS
  'Identificador idempotente generado offline; UNIQUE evita duplicar la misma acción al sincronizar.';
COMMENT ON COLUMN bsale.visitas.created_at IS 'Auditoría: creación del registro.';
COMMENT ON COLUMN bsale.visitas.updated_at IS 'Auditoría: última modificación.';

CREATE INDEX idx_visitas_ruta_id ON bsale.visitas (ruta_id);
CREATE INDEX idx_visitas_cliente_id ON bsale.visitas (cliente_id);
CREATE INDEX idx_visitas_sync_pending ON bsale.visitas (sync_status) WHERE sync_status = 'pending_sync';

CREATE TRIGGER trg_visitas_set_updated_at
BEFORE UPDATE ON bsale.visitas
FOR EACH ROW
EXECUTE PROCEDURE bsale.set_updated_at();
