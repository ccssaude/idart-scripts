pg_dump -Upostgres  --table=drug --data-only --column-inserts efila > efila_padronizado/drug.sql

pg_dump -Upostgres  --table=regimendrugs --data-only --column-inserts efila > efila_padronizado/regimendrugs.sql