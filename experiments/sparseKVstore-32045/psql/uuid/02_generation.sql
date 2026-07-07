------------------------- UUID backed KV journal data generation -------------------------

-- Bridge the psql variables into the SQL session
SET custom.num_shards = :num_shards;
SET custom.keys_per_shard = :keys_per_shard;

DO $$
DECLARE
    -- A. Configuration
    v_shards          INTEGER := current_setting('custom.num_shards')::INTEGER;
    v_keys_per_shard  INTEGER := current_setting('custom.keys_per_shard')::INTEGER;
    v_total_keys      INTEGER := v_shards * v_keys_per_shard;
    v_volatile_count  INTEGER := GREATEST(1, (v_total_keys * 0.2)::int);
    -- B. Timeline Sizing
    -- We want enough events so that keys are interleaved across a broad history
    v_event_count     INTEGER := GREATEST(1000, v_total_keys * 5); 
    v_base_start      BIGINT := (EXTRACT(EPOCH FROM (NOW() - INTERVAL '24 hours')) * 1000)::BIGINT;
    v_prime           BIGINT := 999999999989; -- this is a large prime number for modulo operation
    v_divisor         INTEGER := 10;
    v_found_count     INTEGER := 0;
BEGIN
    RAISE NOTICE 'Step 1: Building Global Event Timeline (3 events per timestamp bucket)...';

    -- Create a temporary timeline of ACS Changes
    CREATE TEMP TABLE global_timeline AS
    WITH base_buckets AS (
        SELECT 
            -- Calculate the raw sequence and the bucket ID first
            row_number() OVER() as raw_id,
            -- increasing in group of 3 (0,0,0,1,1,1,2,2,2 ...)
            ((row_number() OVER() - 1) / 3) as bucket_id
        FROM generate_series(1, v_event_count)
    )
    SELECT 
        v_base_start + (bucket_id * 10)::bigint as ts, -- (ts0,ts0,ts0,ts1,ts1,ts1,ts2,ts2,ts2 ...)
        -- Now we can PARTITION BY the static bucket_id
        row_number() OVER(PARTITION BY bucket_id ORDER BY random()) as tie, -- randomize the order (1,0,1,2,0,1,2,2,0 ...)
        raw_id as event_id
    FROM base_buckets;

    RAISE NOTICE 'Step 2: Assigning Volatile (20%%) and Stable (80%%) keys to events...';

    -- 3. The Core Generation
    INSERT INTO journal (shard, key, ts, value1, value2, replaces_ts)
    WITH key_pool AS (
        SELECT 
            i as key_id, -- increasing sequence of 1..v_total_keys
            (i % v_shards) + 1 as shard,
            CASE WHEN i <= v_volatile_count THEN 'HIGH' ELSE 'STABLE' END as tier -- classification for volatile/non-volatile
        FROM generate_series(1, v_total_keys) i
    ),
    assignments AS (
        -- HIGH VOLATILITY (20% of keys)
        SELECT k.shard, k.key_id, t.ts, t.tie
        FROM global_timeline t
        JOIN key_pool k ON k.tier = 'HIGH' 
          -- We can adjust X=60 to change density: (v_volatile_count / X) = keys per event
          AND k.key_id % (v_volatile_count / 60) = t.event_id % (v_volatile_count / 60)
        
        UNION ALL

        -- STABLE (80% of keys)
        SELECT k.shard, k.key_id, t.ts, t.tie
        FROM global_timeline t
        JOIN key_pool k ON k.tier = 'STABLE'
          AND k.key_id % ((v_total_keys - v_volatile_count) / 15) = t.event_id % ((v_total_keys - v_volatile_count) / 15)
        WHERE t.event_id % 5 = 0 -- Keep them sparse
    ),
    linked_updates AS (
        SELECT 
            *,
            -- Generate local sequence for this key to find the 'replaces' link
            ROW_NUMBER() OVER (PARTITION BY key_id ORDER BY ts, tie) as key_update_seq,
            -- LAG reaches back to the previous row in the specific key's history
            LAG(ts) OVER (PARTITION BY key_id ORDER BY ts, tie) as prev_ts, -- LAG(field) = previous row's value
            LAG(tie) OVER (PARTITION BY key_id ORDER BY ts, tie) as prev_tie -- ditto
        FROM assignments
    )
    SELECT 
        shard,
        key_id,
        -- ts: Packed TS (High 64) and TS % PRIME + Tie (Low 64)
        (lpad(to_hex(ts), 16, '0') || lpad(to_hex(ts % v_prime + tie), 16, '0'))::uuid as ts,
        -- 5% is NULL
        CASE WHEN random() > 0.1 THEN gen_random_bytes(16) ELSE NULL END,
        CASE WHEN random() > 0.1 THEN gen_random_bytes(16) ELSE NULL END,
        -- Replaces_ts: NULL for the first entry, previous UUID for others
        CASE WHEN key_update_seq > 1 THEN
            (lpad(to_hex(prev_ts), 16, '0') || lpad(to_hex(prev_ts % v_prime + prev_tie), 16, '0'))::uuid
        ELSE NULL END
    FROM linked_updates;

    RAISE NOTICE 'Step 3: Recording checkpoints...';

    LOOP
        INSERT INTO journal_checkpoint (shard, ts)
        SELECT DISTINCT shard, ts
        FROM journal
        WHERE ('x' || encode(substring(uuid_send(ts) from 9 for 8), 'hex'))::bit(64)::bigint % v_divisor = 0
        ON CONFLICT DO NOTHING;

        -- Check how many rows we actually just inserted
        GET DIAGNOSTICS v_found_count = ROW_COUNT;

        IF v_found_count > 0 THEN
            EXIT; -- Exit the loop
        END IF;
        
        -- Fallback: If no rows found, try a different number 
        -- to change the sampling alignment.
        v_divisor := v_divisor - 1;
        
        -- Safety break: don't let it go to 1 (which would select every row!)
        IF v_divisor < 2 THEN
            RAISE WARNING 'Could not find sparse checkpoints. Something is not okay with the generated data, please review!';
            EXIT;
        END IF;
    END LOOP;

    DROP TABLE global_timeline;
    
    RAISE NOTICE 'Generation complete. Shard stats:';
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