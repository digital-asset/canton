-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create index sequencer_members_onboarding_idx
    on sequencer_members (status, registered_ts)
    where onboarded_ts is not null;

-- participant event category to activation table
alter table lapi_events_activate_contract add column participant_event_category int;
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
    traffic_cost,

    -- add remove event related
    participant_event_category
from lapi_events_activate_contract;

-- participant event category to deactivation table
alter table lapi_events_deactivate_contract add column participant_event_category int;
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
    traffic_cost,

    -- add remove event related
    participant_event_category
from lapi_events_deactivate_contract;

-- traffic enforcement uncharged completions and projection state
create table par_traffic_enforcement_uncharged_completions
(
    -- Ledger API offset of the completion
    completion_offset bigint              not null primary key,
    -- Hash of the Ledger API change id
    change_id_hash    varchar collate "C" not null,
    submission_id     varchar collate "C" not null,
    paid_traffic_cost bigint              not null,
    -- Why the completion wasn't charged
    reason            smallint            not null,
    -- Shape of the completion's act-as parties
    act_as_category   smallint            not null,
    -- Record time of the completion
    timestamp         bigint              not null
);

create index idx_traffic_uncharged_completion_timestamp on par_traffic_enforcement_uncharged_completions (timestamp);

create or replace function debug.tea_uncharged_reason(smallint) returns varchar as -- collate "C" result. This comment is necessary to satisfy our linter
$$
select
  case
    when $1 = 0 then 'missing_correlation'
    when $1 = 1 then 'no_account_selected'
    when $1 = 2 then 'rejected_completion_charging_disabled'
    else $1::text
  end;
$$
  language sql
  immutable
  returns null on null input;

create or replace function debug.tea_act_as_category(smallint) returns varchar as -- collate "C" result. This comment is necessary to satisfy our linter
$$
select
  case
    when $1 = 0 then 'multiple_acting_parties'
    when $1 = 1 then 'admin_acting_party'
    when $1 = 2 then 'single_non_admin_acting_party'
    else $1::text
  end;
$$
  language sql
  immutable
  returns null on null input;

create or replace view debug.par_traffic_enforcement_uncharged_completions as
select
    completion_offset,
    change_id_hash,
    submission_id,
    paid_traffic_cost,
    debug.tea_uncharged_reason(reason) as reason,
    debug.tea_act_as_category(act_as_category) as act_as_category,
    debug.canton_timestamp(timestamp) as timestamp
from par_traffic_enforcement_uncharged_completions;

create table par_traffic_enforcement_state
(
    -- For example "last_completion_offset"
    name        varchar collate "C" not null primary key,
    state_value varchar collate "C" not null
);

create or replace view debug.par_traffic_enforcement_state as
select
    name,
    state_value
from par_traffic_enforcement_state;

-- traffic debit rights on user rights
-- Set only for CanDebitTrafficFrom rights
alter table lapi_user_rights add column account_id varchar collate "C";

alter table lapi_user_rights add constraint lapi_user_rights_party_or_account_check
    check (for_party is null or account_id is null);

drop index lapi_user_rights_user_internal_id_user_right_idx;

create unique index lapi_user_rights_user_internal_id_user_right_idx
    on lapi_user_rights using btree (user_internal_id, user_right)
    where (for_party is null and account_id is null);

create unique index lapi_user_rights_user_internal_id_user_right_account_idx
    on lapi_user_rights using btree (user_internal_id, user_right, account_id)
    where (account_id is not null);

create or replace function debug.lapi_user_right(integer) returns varchar as -- collate "C" result. This comment is necessary to satisfy our linter
$$
select
  case
    when $1 = 1 then 'ParticipantAdmin'
    when $1 = 2 then 'CanActAs'
    when $1 = 3 then 'CanReadAs'
    when $1 = 4 then 'IdentityProviderAdmin'
    when $1 = 5 then 'CanReadAsAnyParty'
    when $1 = 6 then 'CanExecuteAs'
    when $1 = 7 then 'CanExecuteAsAnyParty'
    when $1 = 8 then 'CanActAsAnyParty'
    when $1 = 9 then 'CanDebitTrafficFrom'
    when $1 = 10 then 'CanDebitTrafficFromAnyAccount'
    else $1::text
  end;
$$
  language sql
  immutable
  returns null on null input;

create or replace view debug.lapi_user_rights as
  select
    debug.resolve_lapi_user(user_internal_id) as user_internal_id,
    debug.lapi_user_right(user_right) as user_right,
    for_party,
    debug.canton_timestamp(granted_at) as granted_at,
    account_id
  from lapi_user_rights;

create index lapi_string_interning_es_idx on lapi_string_interning using btree (external_string) include (internal_id);
