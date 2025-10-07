-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Update LAPI event tables to add the external_transaction_hash column
ALTER TABLE lapi_events_consuming_exercise
    ADD COLUMN external_transaction_hash bytea;

ALTER TABLE lapi_events_create
    ADD COLUMN external_transaction_hash bytea;

ALTER TABLE lapi_events_non_consuming_exercise
    ADD COLUMN external_transaction_hash bytea;

-- Update views as well
create or replace view debug.lapi_events_consuming_exercise as
  select
    event_sequential_id,
    debug.canton_timestamp(ledger_effective_time) as ledger_effective_time,
    node_id,
    event_offset,
    update_id,
    workflow_id,
    command_id,
    user_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    lower(encode(contract_id, 'hex')) as contract_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(package_name) as package_name,
    debug.resolve_lapi_interned_strings(flat_event_witnesses) as flat_event_witnesses,
    debug.resolve_lapi_interned_strings(tree_event_witnesses) as tree_event_witnesses,
    create_key_value,
    exercise_choice,
    exercise_argument,
    exercise_result,
    debug.resolve_lapi_interned_strings(exercise_actors) as exercise_actors,
    exercise_last_descendant_node_id,
    debug.lapi_compression(create_key_value_compression) as create_key_value_compression,
    debug.lapi_compression(exercise_argument_compression) as exercise_argument_compression,
    debug.lapi_compression(exercise_result_compression) as exercise_result_compression,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    trace_context,
    debug.canton_timestamp(record_time) as record_time,
    external_transaction_hash
  from lapi_events_consuming_exercise;

  create or replace view debug.lapi_events_create as
    select
      event_sequential_id,
      debug.canton_timestamp(ledger_effective_time) as ledger_effective_time,
      node_id,
      event_offset,
      update_id,
      workflow_id,
      command_id,
      user_id,
      debug.resolve_lapi_interned_strings(submitters) as submitters,
      lower(encode(contract_id, 'hex')) as contract_id,
      debug.resolve_lapi_interned_string(template_id) as template_id,
      debug.resolve_lapi_interned_string(package_name) as package_name,
      debug.resolve_lapi_interned_strings(flat_event_witnesses) as flat_event_witnesses,
      debug.resolve_lapi_interned_strings(tree_event_witnesses) as tree_event_witnesses,
      create_argument,
      debug.resolve_lapi_interned_strings(create_signatories) as create_signatories,
      debug.resolve_lapi_interned_strings(create_observers) as create_observers,
      create_key_value,
      create_key_hash,
      debug.lapi_compression(create_argument_compression) as create_argument_compression,
      debug.lapi_compression(create_key_value_compression) as create_key_value_compression,
      driver_metadata,
      debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
      debug.resolve_lapi_interned_strings(create_key_maintainers) as create_key_maintainers,
      trace_context,
      debug.canton_timestamp(record_time) as record_time,
      external_transaction_hash
    from lapi_events_create;

  create or replace view debug.lapi_events_non_consuming_exercise as
    select
      event_sequential_id,
      debug.canton_timestamp(ledger_effective_time) as ledger_effective_time,
      node_id,
      event_offset,
      update_id,
      workflow_id,
      command_id,
      user_id,
      debug.resolve_lapi_interned_strings(submitters) as submitters,
      lower(encode(contract_id, 'hex')) as contract_id,
      debug.resolve_lapi_interned_string(template_id) as template_id,
      debug.resolve_lapi_interned_string(package_name) as package_name,
      debug.resolve_lapi_interned_strings(tree_event_witnesses) as tree_event_witnesses,
      create_key_value,
      exercise_choice,
      exercise_argument,
      exercise_result,
      debug.resolve_lapi_interned_strings(exercise_actors) as exercise_actors,
      exercise_last_descendant_node_id,
      debug.lapi_compression(create_key_value_compression) as create_key_value_compression,
      debug.lapi_compression(exercise_argument_compression) as exercise_argument_compression,
      debug.lapi_compression(exercise_result_compression) as exercise_result_compression,
      debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
      trace_context,
      debug.canton_timestamp(record_time) as record_time,
      external_transaction_hash
    from lapi_events_non_consuming_exercise;
