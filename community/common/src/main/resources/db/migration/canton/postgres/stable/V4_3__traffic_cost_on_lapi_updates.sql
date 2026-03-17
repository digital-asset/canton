-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Add traffic_cost column to the lapi_events_[de]activate_contract and lapi_events_various_witnessed
-- to make the cost available on updates
alter table lapi_events_activate_contract add column traffic_cost bigint default null;
create or replace view debug.lapi_events_activate_contract as
select
    -- update related columns
    event_offset,
    lower(encode(update_id, 'hex')) as update_id,
    workflow_id,
    command_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    debug.canton_timestamp(record_time) as record_time,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    lower(encode(trace_context, 'hex')) as trace_context,
    lower(encode(external_transaction_hash, 'hex')) as external_transaction_hash,

    -- event related columns
    debug.lapi_event_type(event_type) as event_type,
    event_sequential_id,
    node_id,
    debug.resolve_lapi_interned_strings(additional_witnesses) as additional_witnesses,
    debug.resolve_lapi_interned_string(source_synchronizer_id) as source_synchronizer_id,
    reassignment_counter,
    lower(encode(reassignment_id, 'hex')) as reassignment_id,
    debug.resolve_lapi_interned_string(representative_package_id) as representative_package_id,

    -- contract related columns
    internal_contract_id,
    create_key_hash,

    -- update related
    traffic_cost
from lapi_events_activate_contract;

alter table lapi_events_deactivate_contract add column traffic_cost bigint default null;
create or replace view debug.lapi_events_deactivate_contract as
select
    -- update related columns
    event_offset,
    lower(encode(update_id, 'hex')) as update_id,
    workflow_id,
    command_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    debug.canton_timestamp(record_time) as record_time,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    lower(encode(trace_context, 'hex')) as trace_context,
    lower(encode(external_transaction_hash, 'hex')) as external_transaction_hash,

    -- event related columns
    debug.lapi_event_type(event_type) as event_type,
    event_sequential_id,
    node_id,
    deactivated_event_sequential_id,
    debug.resolve_lapi_interned_strings(additional_witnesses) as additional_witnesses,
    debug.resolve_lapi_interned_string(exercise_choice) as exercise_choice,
    debug.resolve_lapi_interned_string(exercise_choice_interface) as exercise_choice_interface,
    lower(encode(exercise_argument, 'hex')) as exercise_argument,
    lower(encode(exercise_result, 'hex')) as exercise_result,
    debug.resolve_lapi_interned_strings(exercise_actors) as exercise_actors,
    exercise_last_descendant_node_id,
    debug.lapi_compression(exercise_argument_compression) as exercise_argument_compression,
    debug.lapi_compression(exercise_result_compression) as exercise_result_compression,
    lower(encode(reassignment_id, 'hex')) as reassignment_id,
    assignment_exclusivity,
    debug.resolve_lapi_interned_string(target_synchronizer_id) as target_synchronizer_id,
    reassignment_counter,

    -- contract related columns
    lower(encode(contract_id, 'hex')) as contract_id,
    internal_contract_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(package_id) as package_id,
    debug.resolve_lapi_interned_strings(stakeholders) as stakeholders,
    debug.canton_timestamp(ledger_effective_time) as ledger_effective_time,
    -- update related
    traffic_cost
from lapi_events_deactivate_contract;

alter table lapi_events_various_witnessed add column traffic_cost bigint default null;
create or replace view debug.lapi_events_various_witnessed as
select
    -- update related columns
    event_offset,
    lower(encode(update_id, 'hex')) as update_id,
    workflow_id,
    command_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    debug.canton_timestamp(record_time) as record_time,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    lower(encode(trace_context, 'hex')) as trace_context,
    lower(encode(external_transaction_hash, 'hex')) as external_transaction_hash,

    -- event related columns
    debug.lapi_event_type(event_type) as event_type,
    event_sequential_id,
    node_id,
    debug.resolve_lapi_interned_strings(additional_witnesses) as additional_witnesses,
    consuming,
    debug.resolve_lapi_interned_string(exercise_choice) as exercise_choice,
    debug.resolve_lapi_interned_string(exercise_choice_interface) as exercise_choice_interface,
    lower(encode(exercise_argument, 'hex')) as exercise_argument,
    lower(encode(exercise_result, 'hex')) as exercise_result,
    debug.resolve_lapi_interned_strings(exercise_actors) as exercise_actors,
    exercise_last_descendant_node_id,
    debug.lapi_compression(exercise_argument_compression) as exercise_argument_compression,
    debug.lapi_compression(exercise_result_compression) as exercise_result_compression,
    debug.resolve_lapi_interned_string(representative_package_id) as representative_package_id,

    -- contract related columns
    lower(encode(contract_id, 'hex')) as contract_id,
    internal_contract_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(package_id) as package_id,
    debug.canton_timestamp(ledger_effective_time) as ledger_effective_time,

    -- update related
    traffic_cost
from lapi_events_various_witnessed;
