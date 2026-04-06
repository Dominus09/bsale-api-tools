-- Coordenadas derivadas del campo facebook (formato "lat,lon") en sync_clients.py
ALTER TABLE bsale.clients
  ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION;
