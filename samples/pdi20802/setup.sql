DROP TABLE IF EXISTS public.pdi20802_success CASCADE;
DROP TABLE IF EXISTS public.pdi20802_unsupported CASCADE;
DROP TABLE IF EXISTS public.pdi20802_narrow CASCADE;
DROP TABLE IF EXISTS public.pdi20802_large CASCADE;
DROP TABLE IF EXISTS public.pdi20802_missing CASCADE;
DROP USER IF EXISTS pdi20802_reader CASCADE;

CREATE TABLE public.pdi20802_success (
  id INTEGER,
  payload VARCHAR(64)
);

CREATE TABLE public.pdi20802_unsupported (
  id INTEGER,
  payload INTERVAL YEAR TO MONTH
);

CREATE TABLE public.pdi20802_narrow (
  id INTEGER,
  payload VARCHAR(4)
);

CREATE TABLE public.pdi20802_large (
  id INTEGER,
  payload VARCHAR(512)
);

-- Passwordless login is intentional for this disposable local-only probe account.
CREATE USER pdi20802_reader;
GRANT USAGE ON SCHEMA public TO pdi20802_reader;
GRANT SELECT ON TABLE public.pdi20802_success TO pdi20802_reader;