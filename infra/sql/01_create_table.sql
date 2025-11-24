CREATE TABLE IF NOT EXISTS public.sensor_events (
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
