------------------------- UUID backed 'Bulk' query -------------------------

-- 1. Pick a random starting point for the pagination
-- We select an existing record to ensure the Bulk query's 'WHERE' clause has a valid range to work with.
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
WITH RECURSIVE key_batch AS (
    (
        SELECT key
        FROM journal
        WHERE shard = :p_sh AND key >= :p_key AND ts <= :'p_ts'::uuid
        ORDER BY key
        LIMIT 1
    ) -- starting line of recursion: It finds the first key that is greater than or equal to :p_key
    UNION ALL
    SELECT (
        SELECT j.key
        FROM journal j
        WHERE j.shard = :p_sh
          AND j.key > kb.key
          AND j.ts <= :'p_ts'::uuid
        ORDER BY j.key ASC
        LIMIT 1
    )
    FROM key_batch kb -- recursive step
    -- base (termination) case: when it gives back NULL
    -- otherwise: takes the key found in the previous step (kb.key) and searches for the next smallest key that is strictly greater (j.key > kb.key).
)
SELECT j.*
FROM key_batch b
  CROSS JOIN LATERAL ( -- foreach unique key in b
    SELECT * FROM journal
    WHERE shard = :p_sh 
      AND key = b.key
      AND ts <= :'p_ts'::uuid
    ORDER BY ts DESC
    LIMIT 1
  ) AS j
LIMIT :page_size;