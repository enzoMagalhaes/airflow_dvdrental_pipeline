CREATE TABLE IF NOT EXISTS public.customer (
    customer_id integer NOT NULL,
    store_id smallint NOT NULL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    email character varying(50),
    address_id smallint NOT NULL,
    activebool boolean DEFAULT true NOT NULL,
    create_date date DEFAULT ('now' :: text) :: date NOT NULL,
    last_update timestamp without time zone DEFAULT now(),
    active integer
);