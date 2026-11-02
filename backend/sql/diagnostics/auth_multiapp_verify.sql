-- READ-ONLY — verificación pre/post migraciones multiapp 064-066.
-- No escribe. Ejecutar con el rol de la API si se desea comprobar privilegios.

SELECT current_database() AS db,
       current_user AS db_user;

SELECT '060_token_version' AS check_id,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='bsale' AND table_name='users' AND column_name='token_version'
       ) AS ok
UNION ALL
SELECT '061_user_companies', to_regclass('app.user_companies') IS NOT NULL
UNION ALL
SELECT '062_auth_events', to_regclass('app.auth_events') IS NOT NULL
UNION ALL
SELECT '063_password_tokens', to_regclass('app.auth_password_tokens') IS NOT NULL
UNION ALL
SELECT '064_user_apps', to_regclass('app.user_apps') IS NOT NULL
UNION ALL
SELECT '065_profiles', to_regclass('app.user_operational_profiles') IS NOT NULL
UNION ALL
SELECT '066_offices', to_regclass('app.user_offices') IS NOT NULL;

-- Conteos (tras 064-066 deben ser 0 hasta backfill 1B)
SELECT
  (SELECT count(*) FROM bsale.users) AS users,
  (SELECT count(*) FROM app.user_companies) AS user_companies,
  (SELECT count(*) FROM app.user_apps) AS user_apps,
  (SELECT count(*) FROM app.user_operational_profiles) AS profiles,
  (SELECT count(*) FROM app.user_offices) AS offices;
