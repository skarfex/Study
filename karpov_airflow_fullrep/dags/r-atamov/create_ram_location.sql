create table IF NOT EXISTS public.r_atamov_ram_location(
    id serial4 not null,
    name varchar(255),
    type varchar(255),
    dimension varchar(255),
    resident_cnt int,
    constraint pk_key PRIMARY KEY (id)
)