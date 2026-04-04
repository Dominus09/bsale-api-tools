-- Set staff password_hash (bcrypt). Generate hash with:
--   python backend/scripts/gen_staff_password_hash.py "NEW_PASSWORD" --email user@example.com
UPDATE bsale.users
SET password_hash = '<PASTE_BCRYPT_HASH_HERE>'
WHERE lower(trim(email)) = lower(trim('carlosfelipe.romero@laquillotana.cl'))
  AND active = true;
