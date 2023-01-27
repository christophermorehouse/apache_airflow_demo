BEGIN;

SET client_encoding = 'LATIN1';

CREATE SCHEMA prod;

-- Table: prod.mla_microwaves
--DROP TABLE IF EXISTS prod.mla_microwaves CASCADE;
CREATE TABLE IF NOT EXISTS prod.mla_microwaves
(
    id text COLLATE pg_catalog."default" NOT NULL,
    site_id text COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    price numeric,
    sold_quantity integer,
    thumbnail text COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    CONSTRAINT vulnerabilities_id_pkey PRIMARY KEY (id)
)
TABLESPACE pg_default;

COMMIT;