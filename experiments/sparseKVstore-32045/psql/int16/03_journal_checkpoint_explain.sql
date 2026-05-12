-- 1. Pick a random existing record to use as our 'Reference Point'
-- Uses \gset to turn the columns of the result into psql variables
SELECT 
    shard AS p_sh, 
    ts AS p_ts
FROM journal
OFFSET floor(random() * :keys_per_shard) 
LIMIT 1
\gset

\echo '----------------------------------------------------'
\echo 'Testing journal checkpoint count for'
\echo 'Shard: ' :p_sh ' '
\echo 'Timestamp: ' :p_ts
\echo '----------------------------------------------------'

-- 2. Run explain on count of journal_checkpoint
EXPLAIN (ANALYZE, BUFFERS)
SELECT 1
FROM journal_checkpoint
WHERE shard = :p_sh
  AND ts = :'p_ts'::int16
