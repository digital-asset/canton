------------------------- UUID backed 'Pointwise lookup' query -------------------------

-- 1. Pick a random existing record to use as our 'Reference Point'
-- Uses \gset to turn the columns of the result into psql variables
SELECT 
    shard AS p_sh, 
    key AS p_key,
    ts AS p_ts1
FROM journal
OFFSET floor(random() * :keys_per_shard) 
LIMIT 1
\gset

-- we need a non relevant random ts where we might not have key change or we have more than one
SELECT 
    ts AS p_ts
FROM journal
WHERE ts <> :'p_ts1'::uuid
OFFSET floor(random() * :keys_per_shard) 
LIMIT 1
\gset

\echo '----------------------------------------------------'
\echo 'Testing Point-in-Time Lookup for:'
\echo 'Shard: ' :p_sh ' | Key: ' :p_key
\echo 'Timestamp: ' :p_ts
\echo '----------------------------------------------------'

-- 2. Run the Pointwise lookup
-- We look for the most recent state <= our reference point
EXPLAIN (ANALYZE, BUFFERS)
SELECT shard, key, value1, value2, ts
FROM journal
WHERE shard = :p_sh 
  AND key = :p_key
  AND ts <= :'p_ts'::uuid
ORDER BY shard, key, ts DESC
LIMIT 1;