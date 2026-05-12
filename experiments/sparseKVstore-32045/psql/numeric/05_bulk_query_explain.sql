-- 1. Pick a random starting point for the pagination
-- We select an existing record to ensure our 'WHERE' clause has a valid range to work with.
SELECT 
    shard AS p_sh, 
    key AS p_key, 
    ts AS p_ts
FROM journal
ORDER by key, ts
OFFSET floor(random() * (:keys_per_shard * 0.1)) -- Pick from the first 10% to ensure room for a full page
LIMIT 1
\gset

\echo '----------------------------------------------------'
\echo 'Testing Bulk query for:'
\echo 'Shard: ' :p_sh ' | Starting Key: >= ' :p_key
\echo 'Point-in-Time: ' :p_ts
\echo 'Page Size: ' :page_size
\echo '----------------------------------------------------'

-- 2. Run the Explain on the Paginated bulk query
EXPLAIN (ANALYZE, BUFFERS)
WITH active_keys AS (
  SELECT 
    shard, 
    key, 
    -- Finding the lexicographical maximum for each key up to our point-in-time
    -- Note tie_breaker is max 1000
    MAX(ts) as ts
  FROM journal
  WHERE shard = :p_sh 
    AND key >= :p_key 
    AND ts <= :p_ts
  GROUP BY shard, key
  -- Note: ORDER BY here must match the shard/key to ensure stable pagination
  ORDER BY shard, key
  LIMIT :page_size
)
SELECT j.shard, j.key, j.ts, j.value1, j.value2
FROM journal j
-- Matching the (shard, key, packed_time) tuple
WHERE (j.shard, j.key, j.ts) IN (SELECT shard, key, ts FROM active_keys);