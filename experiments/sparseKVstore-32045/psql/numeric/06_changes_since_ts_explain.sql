-- 1. Pick a starting point and derive the Numeric range
SELECT
    shard AS p_sh, 
    key AS p_key, 
    -- Keep it as NUMERIC to avoid the BigInt 64-bit limit
    floor(ts / 1000000000000) AS raw_ts1,
    -- p_v1 is our starting numeric value
    ts AS p_v1, 
    -- Perform math using numeric literals (no casting to bigint)
    ((floor(ts / 1000000000000) + 3600000) * 1000000000000 + 999999999999) AS p_v2
FROM journal
ORDER by key, ts
OFFSET floor(random() * (:keys_per_shard * 0.1))
LIMIT 1
\gset

\echo '----------------------------------------------------'
\echo 'Testing Range Scan (NUMERIC) for Shard ' :p_sh
\echo 'Time Range (Derived): ' :raw_ts1 ' to ' (:'raw_ts1' + 3600000)
\echo 'Numeric Range: ' :p_v1 ' to ' :p_v2
\echo 'Starting Key: >= ' :p_key ' | Page Size: ' :page_size
\echo '----------------------------------------------------'

-- 2. The Explain Query
EXPLAIN (ANALYZE, BUFFERS)
SELECT shard, ts, key, value1, value2
FROM journal
WHERE shard = :p_sh 
  AND ts >= :p_v1 
  AND ts <= :p_v2
  AND key >= :p_key
ORDER BY shard, key, ts
LIMIT :page_size;