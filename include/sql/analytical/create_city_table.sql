
CREATE TABLE IF NOT EXISTS public.city (
    city_id integer NOT NULL,
    city character varying(50) NOT NULL,
    country_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);