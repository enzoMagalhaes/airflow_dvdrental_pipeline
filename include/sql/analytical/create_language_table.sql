CREATE TABLE IF NOT EXISTS public.language (
    language_id integer NOT NULL,
    name character(20) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);
