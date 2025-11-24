CREATE TABLE IF NOT EXISTS public.tb_sensor_evento (
  id SERIAL PRIMARY KEY,
  device_id TEXT,
  timestamp_ts TIMESTAMP,
  temperature DOUBLE PRECISION,
  humidity DOUBLE PRECISION,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  status TEXT,
  battery DOUBLE PRECISION,
  alert BOOLEAN,
  dt_carga TIMESTAMP DEFAULT now()
);
