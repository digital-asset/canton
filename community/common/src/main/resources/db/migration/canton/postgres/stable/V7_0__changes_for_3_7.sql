-- Offset corresponding to the offboarding of the latest locally hosted stakeholder, if any
-- Set only if the reassignment is incomplete unassigned at offset incomplete_last_stakeholder_offboarded_target_offset
alter table par_reassignments add column incomplete_last_stakeholder_offboarded_target_offset bigint default null;

create or replace view debug.par_reassignments as
select
    debug.resolve_common_static_string(target_synchronizer_idx) as target_synchronizer_idx,
    debug.resolve_common_static_string(source_synchronizer_idx) as source_synchronizer_idx,
    reassignment_id,
    unassignment_global_offset,
    assignment_global_offset,
    debug.canton_timestamp(unassignment_timestamp) as unassignment_timestamp,
    unassignment_data,
    stakeholders,
    debug.canton_timestamp(assignment_timestamp) as assignment_timestamp,
    incomplete_last_stakeholder_offboarded_target_offset
from par_reassignments;

alter table sequencer_events drop column topology_timestamp cascade;
create or replace view debug.sequencer_events as
select
    debug.canton_timestamp(ts) as ts,
    node_index,
    event_type,
    message_id,
    debug.resolve_sequencer_member(sender) as sender,
    debug.resolve_sequencer_members(recipients) as recipients,
    payload_id,
    trace_context,
    error,
    consumed_cost,
    extra_traffic_consumed,
    base_traffic_remainder
from sequencer_events;

drop table lapi_party_entries cascade;

-- The party queries (knownParties/parties) use this index to efficiently retrieve the list of candidate parties.
create index lapi_events_party_to_participant_party_idx
    on lapi_events_party_to_participant
    using btree (party, event_sequential_id desc)
    include (party_id);

-- The party queries (knownParties/parties) use this index to refine the result set in the inner queries.
create index lapi_events_party_to_participant_party_id_idx
    on lapi_events_party_to_participant
    using btree (party_id, participant_id, synchronizer_id, event_sequential_id desc)
    include (participant_authorization_event);

-- vvv Allow traffic purchases for non-registered participants (#34369) vvv
alter table sequencer_members
    add column status smallint not null default 1; -- Active by default

-- Onboarding members do not have a registration timestamp yet
alter table sequencer_members
    alter column registered_ts drop not null;

-- Time from which a member is onboarding, i.e. the sequencing time of the first traffic purchased for it ahead of its
-- registration. Kept after the member is registered, so that the member status can be reconstructed at earlier times.
alter table sequencer_members
    add column onboarded_ts bigint;

update sequencer_members
set status = 2 -- Disabled
where enabled = false;

drop view debug.sequencer_members;

create view debug.sequencer_members as
select
    member,
    id,
    debug.canton_timestamp(registered_ts) as registered_ts,
    debug.canton_timestamp(pruned_previous_event_timestamp) as pruned_previous_event_timestamp,
    status,
    debug.canton_timestamp(onboarded_ts) as onboarded_ts
from sequencer_members;

alter table sequencer_members
drop column enabled;
-- ^^^ Allow traffic purchases for non-registered participants (#34369) ^^^
