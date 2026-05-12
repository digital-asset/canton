------------------------- UUID backed 'Changes since - Crash recovery' DELETE -------------------------

-- 1. Pick a random shard and find its 'Latest Truth'
SELECT COUNT(1) as p_count
FROM journal_checkpoint
\gset

SELECT 
    shard AS p_sh, 
    ts AS p_max_ts
FROM journal_checkpoint
ORDER BY ts DESC
OFFSET floor(random() * (:p_count * 0.05))
LIMIT 1
\gset

\echo '--------------------------------------------------'
\echo 'Testing Crash recovery Operation '
\echo 'for Shard: ' :p_sh
\echo 'Deleting data newer than TS: ' :p_max_ts
\echo '--------------------------------------------------'

BEGIN;

-- 2. The Explain Query wrapped in a transaction for safety
EXPLAIN (ANALYZE, BUFFERS)
DELETE FROM journal
WHERE shard = :p_sh 
  AND ts > :'p_max_ts'::uuid;

-- NOTE: We rollback so we don't actually delete the data 
-- during the benchmark run!
ROLLBACK;