/*
    Snowflake -> Datacore sync SQL prerequisites.

    02_views.sql

    Run after 01_tables.sql.

    These are utility views only. They are not part of the core sync path, but
    are useful for monitoring storage growth in replicated and operational tables.
*/

CREATE OR ALTER VIEW ops.index_storage
AS
SELECT 
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    i.type_desc,
    i.is_unique,
    i.is_primary_key,
    i.index_id,
    CAST(SUM(a.total_pages) * 8.0 / 1024 AS DECIMAL(18,2)) AS total_mb
FROM sys.tables t
JOIN sys.schemas s 
    ON t.schema_id = s.schema_id
JOIN sys.indexes i 
    ON t.object_id = i.object_id
JOIN sys.partitions p 
    ON i.object_id = p.object_id 
    AND i.index_id = p.index_id
JOIN sys.allocation_units a 
    ON p.partition_id = a.container_id
WHERE i.index_id > 0
GROUP BY 
    s.name,
    t.name,
    i.name,
    i.type_desc,
    i.is_unique,
    i.is_primary_key,
    i.index_id;
GO

CREATE OR ALTER VIEW ops.table_storage
AS
WITH row_counts AS (
    SELECT 
        t.object_id,
        s.name AS schema_name,
        t.name AS table_name,
        SUM(p.rows) AS row_count
    FROM sys.tables t
    JOIN sys.schemas s 
        ON t.schema_id = s.schema_id
    JOIN sys.partitions p 
        ON t.object_id = p.object_id 
        AND p.index_id IN (0,1)
    GROUP BY t.object_id, s.name, t.name
),
table_sizes AS (
    SELECT 
        t.object_id,
        CAST(SUM(a.total_pages) * 8.0 / 1024 AS DECIMAL(18,2)) AS total_mb,
        CAST(SUM(a.used_pages) * 8.0 / 1024 AS DECIMAL(18,2)) AS used_mb,
        CAST(SUM(a.data_pages) * 8.0 / 1024 AS DECIMAL(18,2)) AS data_mb
    FROM sys.tables t
    JOIN sys.indexes i 
        ON t.object_id = i.object_id 
        AND i.index_id IN (0,1)
    JOIN sys.partitions p 
        ON i.object_id = p.object_id 
        AND i.index_id = p.index_id
    JOIN sys.allocation_units a 
        ON p.partition_id = a.container_id
    GROUP BY t.object_id
)
SELECT 
    rc.schema_name,
    rc.table_name,
    rc.row_count,
    ts.total_mb
FROM row_counts rc
JOIN table_sizes ts 
    ON rc.object_id = ts.object_id;
GO
