------------------------- 'Bulk' query for tuple -------------------------

-- 1. Pick a random starting point for the pagination
-- We select an existing record to ensure our 'WHERE' clause has a valid range to work with.
SELECT 
    shard AS p_sh, 
    key AS p_key, 
    ts AS p_ts,
    tie_breaker AS p_tie
FROM journal
ORDER by key, ts, tie_breaker
OFFSET floor(random() * (:keys_per_shard * 0.1)) -- Pick from the first 10% to ensure room for a full page
LIMIT 1
\gset

\echo '----------------------------------------------------'
\echo 'Testing Bulk query for:'
\echo 'Shard: ' :p_sh ' | Starting Key: >= ' :p_key
\echo 'Point-in-Time: ' :p_ts ' Tie breaker: ' :p_tie
\echo 'Page Size: ' :page_size
\echo '----------------------------------------------------'

-- 2. Run the Explain on the Paginated bulk query
EXPLAIN (ANALYZE, BUFFERS)
WITH RECURSIVE key_batch AS (
    (
        SELECT key
        FROM journal
        WHERE shard = :p_sh AND key >= :p_key AND (ts, tie_breaker) <= (:p_ts, :p_tie)
        ORDER BY key
        LIMIT 1
    )
    UNION ALL
    SELECT (
        SELECT j.key
        FROM journal j
        WHERE j.shard = :p_sh
          AND j.key > kb.key
          AND (j.ts, j.tie_breaker) <= (:p_ts, :p_tie)
        ORDER BY j.key ASC
        LIMIT 1
    )
    FROM key_batch kb
)
SELECT j.*
FROM key_batch b
  CROSS JOIN LATERAL (
    SELECT * FROM journal
    WHERE shard = :p_sh 
      AND key = b.key 
      AND (ts, tie_breaker) <= (:p_ts, :p_tie)
    ORDER BY ts DESC
    LIMIT 1
  ) AS j
LIMIT :page_size;