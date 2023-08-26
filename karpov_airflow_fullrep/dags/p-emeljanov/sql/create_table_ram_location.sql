--создать таблицу
CREATE TABLE if not exists public.p_emeljanov_ram_location (
    id serial,
    "name" varchar,
    "type" varchar,
    dimension varchar,
    resident_cnt int,
    PRIMARY KEY (id)
);