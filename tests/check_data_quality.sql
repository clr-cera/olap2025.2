-- Data Quality Checks for OLAP Database

-- 1. Row Counts
SELECT 'dim_data' as table_name, COUNT(*) as count FROM dim_data
UNION ALL
SELECT 'dim_horario_clima', COUNT(*) FROM dim_horario_clima
UNION ALL
SELECT 'dim_horario_queimada', COUNT(*) FROM dim_horario_queimada
UNION ALL
SELECT 'dim_local_clima', COUNT(*) FROM dim_local_clima
UNION ALL
SELECT 'dim_local_queimada', COUNT(*) FROM dim_local_queimada
UNION ALL
SELECT 'fct_clima', COUNT(*) FROM fct_clima
UNION ALL
SELECT 'fct_queimada', COUNT(*) FROM fct_queimada;

-- 2. Referential Integrity Checks (Orphans)
-- These queries return rows in child tables that do not have a corresponding parent.
-- If constraints are enabled, these should return 0 rows.

-- fct_queimada -> dim_data
SELECT COUNT(*) as fct_queimada_orphaned_data
FROM fct_queimada f
LEFT JOIN dim_data d ON f.id_data = d.id_data
WHERE d.id_data IS NULL;

-- fct_queimada -> dim_local_queimada
SELECT COUNT(*) as fct_queimada_orphaned_local
FROM fct_queimada f
LEFT JOIN dim_local_queimada l ON f.id_local = l.id_local
WHERE l.id_local IS NULL;

-- fct_queimada -> dim_horario_queimada
SELECT COUNT(*) as fct_queimada_orphaned_horario
FROM fct_queimada f
LEFT JOIN dim_horario_queimada h ON f.id_horario = h.id_horario
WHERE h.id_horario IS NULL;

-- fct_clima -> dim_data
SELECT COUNT(*) as fct_clima_orphaned_data
FROM fct_clima f
LEFT JOIN dim_data d ON f.id_data = d.id_data
WHERE d.id_data IS NULL;

-- fct_clima -> dim_local_clima
SELECT COUNT(*) as fct_clima_orphaned_local
FROM fct_clima f
LEFT JOIN dim_local_clima l ON f.id_local = l.id_local
WHERE l.id_local IS NULL;

-- fct_clima -> dim_horario_clima
SELECT COUNT(*) as fct_clima_orphaned_horario
FROM fct_clima f
LEFT JOIN dim_horario_clima h ON f.id_horario = h.id_horario
WHERE h.id_horario IS NULL;

-- dim_local_queimada -> dim_local_clima
SELECT COUNT(*) as dim_local_queimada_orphaned_clima
FROM dim_local_queimada q
LEFT JOIN dim_local_clima c ON q.id_local_clima = c.id_local
WHERE c.id_local IS NULL;

-- dim_horario_queimada -> dim_horario_clima
SELECT COUNT(*) as dim_horario_queimada_orphaned_clima
FROM dim_horario_queimada q
LEFT JOIN dim_horario_clima c ON q.id_horario_clima = c.id_horario
WHERE c.id_horario IS NULL;

-- 3. Null Checks on Critical Columns
SELECT COUNT(*) as fct_queimada_null_keys
FROM fct_queimada
WHERE id_data IS NULL OR id_local IS NULL OR id_horario IS NULL;

SELECT COUNT(*) as fct_clima_null_keys
FROM fct_clima
WHERE id_data IS NULL OR id_local IS NULL OR id_horario IS NULL;

-- 4. Duplicate Checks (based on PK definition)
SELECT id_data, id_local, id_horario, COUNT(*)
FROM fct_queimada
GROUP BY id_data, id_local, id_horario
HAVING COUNT(*) > 1;

SELECT id_data, id_local, id_horario, COUNT(*)
FROM fct_clima
GROUP BY id_data, id_local, id_horario
HAVING COUNT(*) > 1;

-- 5. Data Validity Checks (Examples)

-- Check for negative precipitation in fct_queimada
SELECT COUNT(*) as negative_precipitacao_queimada
FROM fct_queimada
WHERE precipitacao < 0;

-- Check for negative precipitation in fct_clima
SELECT COUNT(*) as negative_precipitacao_clima
FROM fct_clima
WHERE precipitacao_dia < 0;

-- Check for invalid latitude/longitude in dim_local_queimada
SELECT COUNT(*) as invalid_lat_long
FROM dim_local_queimada
WHERE latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180;
