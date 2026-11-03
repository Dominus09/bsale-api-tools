-- PROPUESTA — NO EJECUTAR automáticamente.
-- BD objetivo: analytics (única).
-- Otorga al OWNER canónico acceso explícito ERP+PWA, todos los perfiles
-- operativos, todas las empresas y todas las oficinas.
--
-- Alcance:
--   - Solo bsale.users.id = 1 (Carlos Felipe / carlosfelipe.romero@…).
--   - No cambia email, password_hash, role, active ni token_version.
--   - No toca a otros usuarios.
--   - No activa flags AUTH_* globales.
--
-- Prerrequisitos: 061 (user_companies), 062 (auth_events), 064–066 (multiapp).
-- Coolify: AUTH_OWNER_USER_IDS debe incluir 1 (sin cambiar role en PG).

BEGIN;

-- ─── Prechequeos (fallan ⇒ ROLLBACK) ───────────────────────────────────────
DO $$
DECLARE
  v_id integer;
  v_email text;
  v_nombre text;
  v_role text;
  v_active boolean;
  v_hash text;
  v_tv integer;
  v_companies integer;
  v_offices integer;
BEGIN
  SELECT u.id, lower(trim(u.email)), u.nombre_usuario, u.role, u.active,
         u.password_hash, COALESCE(u.token_version, 0)
    INTO v_id, v_email, v_nombre, v_role, v_active, v_hash, v_tv
  FROM bsale.users u
  WHERE u.id = 1
  FOR UPDATE;

  IF v_id IS NULL THEN
    RAISE EXCEPTION 'OWNER bootstrap abortado: bsale.users.id=1 no existe';
  END IF;
  IF v_email IS DISTINCT FROM 'carlosfelipe.romero@laquillotana.cl' THEN
    RAISE EXCEPTION
      'OWNER bootstrap abortado: email inesperado para id=1 (%). Esperado carlosfelipe.romero@laquillotana.cl',
      v_email;
  END IF;
  IF COALESCE(v_nombre, '') IS DISTINCT FROM 'Carlos Felipe' THEN
    RAISE EXCEPTION
      'OWNER bootstrap abortado: nombre_usuario inesperado para id=1 (%). Esperado Carlos Felipe',
      v_nombre;
  END IF;
  IF v_active IS NOT TRUE THEN
    RAISE EXCEPTION 'OWNER bootstrap abortado: id=1 no está activo';
  END IF;

  SELECT COUNT(*) INTO v_companies FROM bsale.companies;
  SELECT COUNT(*) INTO v_offices FROM bsale.offices;
  IF v_companies < 1 OR v_offices < 1 THEN
    RAISE EXCEPTION
      'OWNER bootstrap abortado: catálogo vacío (companies=%, offices=%)',
      v_companies, v_offices;
  END IF;

  -- Snapshot inmutable de identidad (para postchequeo en la misma TX).
  CREATE TEMP TABLE IF NOT EXISTS _owner_bootstrap_snapshot (
    user_id integer PRIMARY KEY,
    email text NOT NULL,
    role text NOT NULL,
    active boolean NOT NULL,
    password_hash text NOT NULL,
    token_version integer NOT NULL,
    companies_expected integer NOT NULL,
    offices_expected integer NOT NULL
  ) ON COMMIT DROP;

  DELETE FROM _owner_bootstrap_snapshot;
  INSERT INTO _owner_bootstrap_snapshot
  VALUES (v_id, v_email, v_role, v_active, v_hash, v_tv, v_companies, v_offices);

  RAISE NOTICE
    'OWNER bootstrap precheck OK id=% email=% role=% companies=% offices=%',
    v_id, v_email, v_role, v_companies, v_offices;
END $$;

-- ─── Apps: erp + pwa (ambas activas; evita perder ERP legacy) ─────────────
INSERT INTO app.user_apps (user_id, app_code, active)
VALUES
  (1, 'erp', TRUE),
  (1, 'pwa', TRUE)
ON CONFLICT (user_id, app_code) DO UPDATE
  SET active = EXCLUDED.active;

-- Cualquier otra app residual del OWNER (no debería existir) se elimina.
DELETE FROM app.user_apps
WHERE user_id = 1
  AND app_code NOT IN ('erp', 'pwa');

-- ─── Perfiles PWA (catálogo 065, incl. supervisor) ────────────────────────
INSERT INTO app.user_operational_profiles (user_id, profile_code, active)
VALUES
  (1, 'vendedor', TRUE),
  (1, 'chofer', TRUE),
  (1, 'bodega', TRUE),
  (1, 'cajero', TRUE),
  (1, 'reponedor', TRUE),
  (1, 'jefe_sucursal', TRUE),
  (1, 'supervisor', TRUE)
ON CONFLICT (user_id, profile_code) DO UPDATE
  SET active = EXCLUDED.active;

DELETE FROM app.user_operational_profiles
WHERE user_id = 1
  AND profile_code NOT IN (
    'vendedor', 'chofer', 'bodega', 'cajero',
    'reponedor', 'jefe_sucursal', 'supervisor'
  );

-- ─── Empresas: todas las de bsale.companies ───────────────────────────────
DELETE FROM app.user_companies WHERE user_id = 1;
INSERT INTO app.user_companies (user_id, company_id)
SELECT 1, c.company_id
FROM bsale.companies c
ORDER BY c.company_id;

-- ─── Oficinas: todas (company_id, bsale_id) ───────────────────────────────
DELETE FROM app.user_offices WHERE user_id = 1;
INSERT INTO app.user_offices (user_id, company_id, office_id)
SELECT 1, o.company_id, o.bsale_id
FROM bsale.offices o
ORDER BY o.company_id, o.bsale_id;

-- ─── Auditoría (directa; no depende de AUTH_AUDIT_EVENTS) ─────────────────
INSERT INTO app.auth_events (
  event_type, user_id, email, success, actor_user_id, detail
) VALUES
(
  'apps_change',
  1,
  'carlosfelipe.romero@laquillotana.cl',
  TRUE,
  1,
  jsonb_build_object(
    'source', 'sql_068_owner_full_ecosystem_access',
    'apps', jsonb_build_array('erp', 'pwa'),
    'note', 'OWNER bootstrap: apps explícitas erp+pwa'
  )
),
(
  'profiles_change',
  1,
  'carlosfelipe.romero@laquillotana.cl',
  TRUE,
  1,
  jsonb_build_object(
    'source', 'sql_068_owner_full_ecosystem_access',
    'profiles', jsonb_build_array(
      'vendedor', 'chofer', 'bodega', 'cajero',
      'reponedor', 'jefe_sucursal', 'supervisor'
    )
  )
),
(
  'companies_change',
  1,
  'carlosfelipe.romero@laquillotana.cl',
  TRUE,
  1,
  jsonb_build_object(
    'source', 'sql_068_owner_full_ecosystem_access',
    'mode', 'all_bsale_companies'
  )
),
(
  'offices_change',
  1,
  'carlosfelipe.romero@laquillotana.cl',
  TRUE,
  1,
  jsonb_build_object(
    'source', 'sql_068_owner_full_ecosystem_access',
    'mode', 'all_bsale_offices'
  )
);

-- ─── Postchequeos ─────────────────────────────────────────────────────────
DO $$
DECLARE
  s RECORD;
  v_email text;
  v_role text;
  v_active boolean;
  v_hash text;
  v_tv integer;
  n_apps integer;
  n_erp integer;
  n_pwa integer;
  n_profiles integer;
  n_companies integer;
  n_offices integer;
  n_audit integer;
BEGIN
  SELECT * INTO s FROM _owner_bootstrap_snapshot WHERE user_id = 1;
  IF s.user_id IS NULL THEN
    RAISE EXCEPTION 'OWNER bootstrap postcheck: snapshot ausente';
  END IF;

  SELECT lower(trim(email)), role, active, password_hash, COALESCE(token_version, 0)
    INTO v_email, v_role, v_active, v_hash, v_tv
  FROM bsale.users WHERE id = 1;

  IF v_email IS DISTINCT FROM s.email
     OR v_role IS DISTINCT FROM s.role
     OR v_active IS DISTINCT FROM s.active
     OR v_hash IS DISTINCT FROM s.password_hash
     OR v_tv IS DISTINCT FROM s.token_version THEN
    RAISE EXCEPTION
      'OWNER bootstrap abortado: identidad/credenciales de id=1 mutaron (email/role/active/hash/tv)';
  END IF;

  SELECT COUNT(*) INTO n_apps FROM app.user_apps WHERE user_id = 1 AND active IS TRUE;
  SELECT COUNT(*) INTO n_erp
    FROM app.user_apps WHERE user_id = 1 AND app_code = 'erp' AND active IS TRUE;
  SELECT COUNT(*) INTO n_pwa
    FROM app.user_apps WHERE user_id = 1 AND app_code = 'pwa' AND active IS TRUE;
  SELECT COUNT(*) INTO n_profiles
    FROM app.user_operational_profiles WHERE user_id = 1 AND active IS TRUE;
  SELECT COUNT(*) INTO n_companies FROM app.user_companies WHERE user_id = 1;
  SELECT COUNT(*) INTO n_offices FROM app.user_offices WHERE user_id = 1;
  SELECT COUNT(*) INTO n_audit
    FROM app.auth_events
    WHERE user_id = 1
      AND detail->>'source' = 'sql_068_owner_full_ecosystem_access'
      AND created_at >= now() - interval '1 minute';

  IF n_apps <> 2 OR n_erp <> 1 OR n_pwa <> 1 THEN
    RAISE EXCEPTION
      'OWNER bootstrap abortado: apps inválidas (apps=%, erp=%, pwa=%)',
      n_apps, n_erp, n_pwa;
  END IF;
  IF n_profiles <> 7 THEN
    RAISE EXCEPTION 'OWNER bootstrap abortado: perfiles=% (esperado 7)', n_profiles;
  END IF;
  IF n_companies <> s.companies_expected THEN
    RAISE EXCEPTION
      'OWNER bootstrap abortado: companies=% (esperado %)',
      n_companies, s.companies_expected;
  END IF;
  IF n_offices <> s.offices_expected THEN
    RAISE EXCEPTION
      'OWNER bootstrap abortado: offices=% (esperado %)',
      n_offices, s.offices_expected;
  END IF;
  IF n_audit < 4 THEN
    RAISE EXCEPTION 'OWNER bootstrap abortado: auditoría incompleta (events=%)', n_audit;
  END IF;

  -- Garantía: ningún otro user_id tocado en esta TX vía conteos de filas
  -- residuales no aplica; el script solo escribe WHERE user_id = 1 / INSERT 1.

  RAISE NOTICE
    'OWNER bootstrap OK id=1 apps=% profiles=% companies=% offices=% audit=%',
    n_apps, n_profiles, n_companies, n_offices, n_audit;
END $$;

COMMIT;

-- ─── Verificación manual post-COMMIT (solo lectura) ───────────────────────
-- SELECT id, email, role, active, nombre_usuario FROM bsale.users WHERE id = 1;
-- SELECT * FROM app.user_apps WHERE user_id = 1 ORDER BY app_code;
-- SELECT * FROM app.user_operational_profiles WHERE user_id = 1 ORDER BY profile_code;
-- SELECT * FROM app.user_companies WHERE user_id = 1 ORDER BY company_id;
-- SELECT company_id, office_id FROM app.user_offices WHERE user_id = 1 ORDER BY 1, 2;
-- SELECT event_type, detail, created_at FROM app.auth_events
--  WHERE detail->>'source' = 'sql_068_owner_full_ecosystem_access'
--  ORDER BY id DESC LIMIT 10;
