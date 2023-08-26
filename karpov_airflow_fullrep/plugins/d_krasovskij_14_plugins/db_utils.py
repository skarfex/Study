
from typing import List, Any


def prepare_values(values: List[tuple], in_braces=True):
    quotate = lambda x: f"'{x}'"
    breacketed = lambda x: f"({x})"

    def check_quote(val): 
        return quotate(val) if "'" not in val else quotate(val.replace("'", "\'"))
    
    def process_val(val: Any):
        if isinstance(val, int):
            return str(val)
        # elif isinstance(val, str):
        #     return check_quote(val)
        else:
            return check_quote(val)

    val_list = []
    for record in values:
        rec_list = []
        for val in record.values():
            rec_list.append(process_val(val))

        val_list.append(breacketed(', '.join(rec_list)))
    
    if in_braces:
        return breacketed(', '.join(val_list))
    
    return ',\n'.join(val_list)


def generate_upsert_statement(tablename:str, values_list_str:str) -> str:
    """Use transaction with TEMP table for upsert"""
    
    temptable = f"{tablename}_stage"

    return f"""
BEGIN;

CREATE TEMP TABLE {temptable} (
	LIKE d_krasovskij_14_ram_location
) ON COMMIT DROP DISTRIBUTED BY (id)
;

INSERT INTO {temptable} (id, name, type, dimension, resident_cnt)
    VALUES
    {values_list_str}
;

INSERT INTO {tablename} (id, name, type, dimension, resident_cnt)
SELECT 
    id, 
    name, 
    type, 
    dimension, 
    resident_cnt
FROM  {temptable} AS tmp
WHERE NOT EXISTS (
	SELECT id 
	FROM {tablename}
	WHERE id = tmp.id
)
;

UPDATE {tablename} as up SET
	id           = tmp.id, 
	name         = tmp.name, 
	type         = tmp.type, 
	dimension    = tmp.dimension, 
	resident_cnt = tmp.resident_cnt 
FROM {temptable} AS tmp 
WHERE up.id = tmp.id
;

COMMIT;
    """

create_table_sql = lambda tablename: f"""
CREATE TABLE IF NOT EXISTS public.{tablename} (
	id integer PRIMARY KEY NOT NULL,
	name text NOT NULL,
	type text,
	dimension text,
	resident_cnt integer NOT NULL
 )
 DISTRIBUTED BY (id);
 """


check_table_sql = lambda tablename: f"""
SELECT count(*) > 0
FROM pg_class c
WHERE c.relname = 'public.{tablename}' 
AND c.relkind = 'r';
"""


# def generate_upsert_statement(tablename, values_list_str): 
#     """Not working. INSERT ON CONFLICT not supported for elder then 9.5 PG versions"""

#     return f"""
# INSERT INTO {tablename} (id, name, type, dimension, resident_cnt)
# 	VALUES {values_list_str}
# 	ON CONFLICT (id, resident_cnt) 
#  	DO UPDATE SET (id, name, type, dimension, resident_cnt) 
#                 = (EXCLUDED.id, EXCLUDED.name, EXCLUDED.type, EXCLUDED.dimension, EXCLUDED.resident_cnt)
# ;
#     """

# def generate_upsert_statement(tablename:str, values_list_str:str) -> str:
#     """Legacy UPSERT with CTE realization. THIS NOT WORKING IN GREENPLUM (writable CTEs)"""

#     return f"""
# WITH EXCLUDED (id, name, type, dimension, resident_cnt) AS
# (
#     VALUES 
#     {values_list_str}
# ),
# upsert as (
#     UPDATE {tablename} old
#     SET
#         id           = EXCLUDED.id, 
#         name         = EXCLUDED.name, 
#         type         = EXCLUDED.type, 
#         dimension    = EXCLUDED.dimension, 
#         resident_cnt = EXCLUDED.resident_cnt 
#     FROM EXCLUDED
#     WHERE (
#         old.id = EXCLUDED.id
#     AND old.resident_cnt <> EXCLUDED.resident_cnt
#     )
#     RETURNING old.id
# )
# INSERT INTO {tablename} (id, name, type, dimension, resident_cnt)
# SELECT 
#     id, 
#     name, 
#     type, 
#     dimension, 
#     resident_cnt
# FROM EXCLUDED
# WHERE EXCLUDED.id NOT IN (
#     SELECT id FROM upsert
# )
# ;
#     """


# check_index_sql = f"""
# SELECT count(*) > 0
# FROM pg_class c
# WHERE c.relname = 'public.{INDEX}' 
# AND c.relkind = 'i';
# """

# create_index_sql = f"""
# CREATE INDEX {INDEX} ON public.{TABLENAME} (id, resident_cnt);
# """
