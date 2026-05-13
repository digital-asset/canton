------------------------- UUID backed 'Pruning of journal and checkpoint' -------------------------

-- 1. Pick a boundary that is 'old' (e.g., 20% into the dataset)
-- This ensures we actually find data to prune.
SELECT 
    shard AS p_sh, 
    ts AS p_boundary_ts -- Using 'version' for precision
FROM journal
WHERE shard = 1 -- Focus on one shard
ORDER BY ts ASC -- Start from the OLDEST
LIMIT 1
OFFSET :keys_per_shard
\gset

\echo '----------------------------------------------------'
\echo 'Testing Deep Pruning for Shard: ' :p_sh
\echo 'Pruning Boundary TS: < ' :p_boundary_ts
\echo '----------------------------------------------------'

BEGIN;

-- STAGE 1: Cleanup old checkpoints
EXPLAIN (ANALYZE, BUFFERS)
DELETE FROM journal_checkpoint
WHERE shard = :p_sh AND ts <= :'p_boundary_ts'::uuid;

-- STAGE 2: Cleanup replaced historical entries
-- This tests the 'replaces' logic
EXPLAIN (ANALYZE, BUFFERS)
WITH replaced_entries AS (
  SELECT key, replaces_ts
  FROM journal
  WHERE shard = :p_sh AND ts <= :'p_boundary_ts'::uuid
)
DELETE FROM journal
WHERE shard = :p_sh 
  AND (key, ts) IN (SELECT key, replaces_ts FROM replaced_entries);

-- STAGE 3: Cleanup tombstones
EXPLAIN (ANALYZE, BUFFERS)
DELETE FROM journal
WHERE shard = :p_sh 
  AND ts <= :'p_boundary_ts'::uuid
  -- delete all NULL values
  AND (value1 IS NULL AND value2 IS NULL);

-- NOTE: We rollback so we don't actually delete the data 
-- during the benchmark run!
ROLLBACK;