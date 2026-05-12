-- Set Global Variables
\if :{?num_shards}
\else
  \set num_shards 4
\endif

\if :{?keys_per_shard}
\else
   \set keys_per_shard 10000
\endif

\if :{?page_size}
\else
   \set page_size 10
\endif

\echo '----------------------------------------------------'
\echo 'Testing Sparse KV journal schema performance '
\echo 'Schema creation is starting... '
\echo '----------------------------------------------------'

-- Run schema creation
\i 01_schema.sql


\echo '----------------------------------------------------'
\echo 'Data generation is starting... '
\echo '----------------------------------------------------'

-- And data generation
\i 02_generation.sql

\echo '----------------------------------------------------'
\echo 'Benchmark of operations is starting... '
\echo '----------------------------------------------------'

-- Run explain scripts for the required operations
\i 03_journal_checkpoint_explain.sql
\i 04_pointwise_lookup_explain.sql
\i 05_bulk_query_explain.sql
\i 06_changes_since_ts_explain.sql
\i 07_crash_recovery_explain.sql
\i 08_pruning_explain.sql