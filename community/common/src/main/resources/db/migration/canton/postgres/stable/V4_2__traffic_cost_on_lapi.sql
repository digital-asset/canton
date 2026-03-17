-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Add traffic_cost column to the lapi_command_completions to make the cost available on completions
alter table lapi_command_completions add column traffic_cost bigint default null;
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
    traffic_cost
from lapi_command_completions;
