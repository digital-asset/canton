SELECT
    shard AS p_sh, 
    key AS p_key, 
    ts AS p_v1, 
    (((ts >> 64) + 3600000::int16) << 64) AS p_v2 -- add 1 hour
FROM journal
ORDER by shard, key, ts
OFFSET floor(random() * (:num_shards * :keys_per_shard * 0.05))
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
  AND ts >= :'p_v1'::int16
  AND ts <= :'p_v2'::int16
  AND key >= :p_key
ORDER BY shard, key, ts
LIMIT :page_size;