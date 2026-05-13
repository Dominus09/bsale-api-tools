-- Set staff password_hash (bcrypt). Generate hash with:
--   python -m backend.maintenance.gen_staff_password_hash "NEW_PASSWORD" --email user@example.com
UPDATE bsale.users
SET password_hash = '<PASTE_BCRYPT_HASH_HERE>'
WHERE lower(trim(email)) = lower(trim('carlosfelipe.romero@laquillotana.cl'))
  AND active = true;
