SELECT alter_table_set_access_method('fct_clima', 'heap');
SELECT alter_table_set_access_method('fct_queimada', 'heap');

VACUUM FULL fct_clima;
VACUUM FULL fct_queimada;
