-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Accepted transactions are looked up by their (unique) hash here.
ALTER TABLE lapi_update_meta ADD COLUMN transaction_hash bytea;
CREATE INDEX idx_lapi_update_meta_tx_hash
    ON lapi_update_meta USING hash (transaction_hash)
    WHERE transaction_hash IS NOT NULL;

-- Only rejections (update_id IS NULL) are looked up by hash on completions;
-- accepted commands are found via lapi_update_meta above. A rejection hash is
-- not unique (retries yield multiple rejected rows), so completion_offset is
-- part of the key to order them.
ALTER TABLE lapi_command_completions ADD COLUMN transaction_hash bytea;
CREATE INDEX idx_lapi_command_completions_tx_hash
    ON lapi_command_completions (transaction_hash, completion_offset)
    WHERE transaction_hash IS NOT NULL AND update_id IS NULL;

-- These should go in the R___.sql file, since these are `create or replace` commands.
create or replace view debug.lapi_update_meta as
  select
    lower(encode(update_id, 'hex')) as update_id,
    event_offset,
    debug.canton_timestamp(publication_time) as publication_time,
    debug.canton_timestamp(record_time) as record_time,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    event_sequential_id_first,
    event_sequential_id_last,
    lower(encode(transaction_hash, 'hex')) as transaction_hash
  from lapi_update_meta;

create or replace view debug.lapi_command_completions as
select
    completion_offset,
    debug.canton_timestamp(record_time) as record_time,
    debug.canton_timestamp(publication_time) as publication_time,
    user_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    command_id,
    lower(encode(update_id, 'hex')) as update_id,
    submission_id,
    deduplication_offset,
    deduplication_duration_seconds,
    deduplication_duration_nanos,
    debug.lapi_rejection_status_code(rejection_status_code) as rejection_status_code,
    rejection_status_message,
    lower(encode(rejection_status_details, 'hex')) as rejection_status_details,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    message_uuid,
    is_transaction,
    lower(encode(trace_context, 'hex')) as trace_context,
    traffic_cost,
    lower(encode(transaction_hash, 'hex')) as transaction_hash
from lapi_command_completions;