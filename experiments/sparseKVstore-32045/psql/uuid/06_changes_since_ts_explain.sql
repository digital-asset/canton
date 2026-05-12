------------------------- UUID backed 'Changes since' query -------------------------

-- 1. Pick a starting point and extract the TS part from the UUID
SELECT
    shard AS p_sh, 
    key AS p_key, 
    -- Extracting the BigInt TS from the first 8 bytes of the ts UUID
    (('x' || replace(substring(ts::text, 1, 18), '-', ''))::bit(64)::bigint) AS raw_ts1,
    -- Packing Start ts: TS1 + Min Tie Breaker (0)
    ts AS p_v1, 
    -- Packing End ts: (TS1 + 1hr) + Max Tie Breaker (all f's)
    (lpad(to_hex((('x' || replace(substring(ts::text, 1, 18), '-', ''))::bit(64)::bigint) + 3600000), 16, '0') 
     || 'ffffffffffffffff')::uuid AS p_v2
FROM journal
ORDER by key, ts
OFFSET floor(random() * (:keys_per_shard * 0.1))
LIMIT 1
\gset

\echo '----------------------------------------------------'
\echo 'Testing Range Scan for Shard ' :p_sh
\echo 'Time Range (Derived): ' :raw_ts1 ' to ' (:'raw_ts1'::bigint + 3600000)
\echo 'UUID Range: ' :p_v1 ' to ' :p_v2
\echo 'Starting Key: >= ' :p_key ' | Page Size: ' :page_size
\echo '----------------------------------------------------'

-- 2. The Explain Query
EXPLAIN (ANALYZE, BUFFERS)
SELECT shard, ts, key, value1, value2
FROM journal
WHERE shard = :p_sh 
  AND ts >= :'p_v1'::uuid 
  AND ts <= :'p_v2'::uuid 
  AND key >= :p_key
ORDER BY shard, key, ts
LIMIT :page_size;