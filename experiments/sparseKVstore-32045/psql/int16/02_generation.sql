-- 1. Bridge the psql variables into the SQL session
SET custom.num_shards = :num_shards;
SET custom.keys_per_shard = :keys_per_shard;

DO $$
DECLARE
    v_shards          INTEGER := current_setting('custom.num_shards')::INTEGER;
    v_keys_per_shard  INTEGER := current_setting('custom.keys_per_shard')::INTEGER;
    v_total_keys      INTEGER := v_shards * v_keys_per_shard;
    v_volatile_count  INTEGER := GREATEST(1, (v_total_keys * 0.2)::int);
    v_event_count     INTEGER := GREATEST(1000, v_total_keys * 5); 
    v_base_start      BIGINT := (EXTRACT(EPOCH FROM (NOW() - INTERVAL '24 hours')) * 1000)::BIGINT;
    v_divisor         INTEGER := 10;
    v_found_count     INTEGER := 0;
BEGIN
    RAISE NOTICE 'Step 1: Building Global Event Timeline...';
    CREATE TEMP TABLE global_timeline AS
    WITH base_buckets AS (
        SELECT 
            row_number() OVER() as raw_id,
            ((row_number() OVER() - 1) / 3) as bucket_id
        FROM generate_series(1, v_event_count)
    )
    SELECT 
        v_base_start + (bucket_id * 10)::bigint as ts,
        row_number() OVER(PARTITION BY bucket_id ORDER BY random()) as tie,
        raw_id as event_id
    FROM base_buckets;

    RAISE NOTICE 'Step 2: Assigning Volatile and Stable keys (int16 Packing)...';

    INSERT INTO journal (shard, key, ts, value1, value2, replaces_ts)
    WITH key_pool AS (
        SELECT i as key_id, (i % v_shards) + 1 as shard,
               CASE WHEN i <= v_volatile_count THEN 'HIGH' ELSE 'STABLE' END as tier
        FROM generate_series(1, v_total_keys) i
    ),
    assignments AS (
        SELECT k.shard, k.key_id, t.ts, t.tie, t.event_id
        FROM global_timeline t
        JOIN key_pool k ON k.tier = 'HIGH' 
          AND k.key_id % (v_volatile_count / 60) = t.event_id % (v_volatile_count / 60)::bigint
        UNION ALL
        SELECT k.shard, k.key_id, t.ts, t.tie, t.event_id
        FROM global_timeline t
        JOIN key_pool k ON k.tier = 'STABLE'
          AND k.key_id % ((v_total_keys - v_volatile_count) / 15) = t.event_id % ((v_total_keys - v_volatile_count) / 15)::bigint
        WHERE t.event_id % 5::bigint = 0
    ),
    linked_updates AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY key_id ORDER BY ts, tie) as key_update_seq,
            LAG(ts) OVER (PARTITION BY key_id ORDER BY ts, tie) as prev_ts,
            LAG(tie) OVER (PARTITION BY key_id ORDER BY ts, tie) as prev_tie
        FROM assignments
    )
    SELECT
        shard,
        key_id,
        -- We use (ts::int16 << 64) to move TS to the high 64 bits
        -- then bitwise OR (|) to drop the tie-breaker into the low 64 bits
        ((ts::int16 << 64) | tie::int16) as ts,
        gen_random_bytes(16),
        gen_random_bytes(16),
        CASE WHEN key_update_seq > 1 THEN
            ((prev_ts::int16 << 64) | prev_tie::int16)
        ELSE NULL END
    FROM linked_updates;

    RAISE NOTICE 'Step 3: Recording checkpoints...';
    
    LOOP
        INSERT INTO journal_checkpoint (shard, ts)
        SELECT DISTINCT shard, ts
        FROM journal
        WHERE (ts % v_divisor::int16) = 0::int16
        ON CONFLICT DO NOTHING;

        -- Check how many rows we actually just inserted
        GET DIAGNOSTICS v_found_count = ROW_COUNT;

        IF v_found_count > 0 THEN
            EXIT; -- Exit the loop
        ELSE
            -- Fallback: If no rows found, try a different number 
            -- to change the sampling alignment.
            v_divisor := v_divisor - 1;
            
            -- Safety break: don't let it go to 1 (which would select every row!)
            IF v_divisor < 2 THEN
                RAISE WARNING 'Could not find sparse checkpoints. Something is not okay with the generated data, please review!';
                EXIT;
            END IF;
        END IF;

        RAISE NOTICE 'No rows for modulo %. Retrying with %...', v_divisor + 1, v_divisor;
    END LOOP;

    DROP TABLE global_timeline;
END $$;

ANALYZE journal;
ANALYZE journal_checkpoint;

\echo '----------------------------------------------------'
\echo 'Generated data verification'

\echo '----------------------------------------------------'
\echo 'Checkpoint count per shard'
\echo '----------------------------------------------------'

SELECT shard, count(*) FROM journal_checkpoint GROUP BY shard ORDER BY shard;

\echo '----------------------------------------------------'
\echo 'Journal count per shard '
\echo '----------------------------------------------------'


SELECT shard, count(*) FROM journal GROUP BY shard ORDER BY shard;

\echo '----------------------------------------------------'
\echo 'A. 20% high volatility changes, 80% stable'
\echo '----------------------------------------------------'

WITH tier_stats AS (
    SELECT 
        CASE 
            -- We replicate the 20% logic based on total key count
            WHEN key <= (SELECT (count(DISTINCT key) * 0.2)::int FROM journal) THEN 'HIGH (20% of keys)'
            ELSE 'STABLE (80% of keys)'
        END as tier,
        count(*) as total_rows,
        count(DISTINCT key) as unique_keys
    FROM journal
    GROUP BY 1
)
SELECT 
    tier,
    unique_keys as "Key Count",
    total_rows as "Total Versions",
    round(total_rows::numeric / unique_keys, 2) as "Avg Versions/Key",
    round((total_rows::numeric / sum(total_rows) OVER()) * 100, 2) || '%' as "Share of Table"
FROM tier_stats;


\echo '----------------------------------------------------'
\echo 'Intervening updates - first 20 rows only'
\echo '----------------------------------------------------'

WITH key_history AS (
    SELECT 
        shard,
        key,
        ts as current_v,
        -- Reach forward to the next version for this specific key
        LEAD(ts) OVER (PARTITION BY shard, key ORDER BY ts) as next_v
    FROM journal
    WHERE key <= 500 -- Sampling a subset for performance
)
SELECT 
    kh.key,
    kh.current_v,
    kh.next_v,
    (
        SELECT count(*) 
        FROM journal j 
        WHERE j.shard = kh.shard
          -- Find any version from ANY other key that falls between these two
          AND j.ts > kh.current_v 
          AND j.ts < kh.next_v
          AND j.key != kh.key
    ) as intervening_updates
FROM key_history kh
WHERE kh.next_v IS NOT NULL
LIMIT 20;