CREATE TABLE IF NOT EXISTS public.category (
    category_id integer NOT NULL,
    name character varying(25) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);
