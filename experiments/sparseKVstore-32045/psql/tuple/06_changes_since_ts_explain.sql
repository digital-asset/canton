------------------------- 'Changes since' query for tuple -------------------------

-- 1. Pick a starting point and extract the TS part from the UUID
SELECT
    shard AS p_sh, 
    key AS p_key, 
    ts AS p_ts_start,
    tie_breaker AS p_tie,
    (ts + 3600000::bigint) AS p_ts_end -- +1 hour, the tie remain the same
FROM journal
ORDER by key, ts, tie_breaker
OFFSET floor(random() * (:keys_per_shard * 0.1))
LIMIT 1
\gset

\echo '----------------------------------------------------'
\echo 'Testing Range Scan for Shard ' :p_sh
\echo 'Time Range (Derived): ' :p_ts_start ' to ' p_ts_end
\echo 'On tie breaker of: ' :p_tie
\echo 'Starting Key: >= ' :p_key ' | Page Size: ' :page_size
\echo '----------------------------------------------------'

-- 2. The Explain Query
EXPLAIN (ANALYZE, BUFFERS)
SELECT shard, ts, tie_breaker, key, value1, value2
FROM journal
WHERE shard = :p_sh
  AND (ts, tie_breaker) >= (:p_ts_start, :p_tie) 
  AND (ts, tie_breaker) <= (:p_ts_end, :p_tie)
  AND key >= :p_key
ORDER BY shard, key, ts, tie_breaker
LIMIT :page_size;