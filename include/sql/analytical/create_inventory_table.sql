CREATE TABLE IF NOT EXISTS public.inventory (
    inventory_id integer NOT NULL,
    film_id smallint NOT NULL,
    store_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);