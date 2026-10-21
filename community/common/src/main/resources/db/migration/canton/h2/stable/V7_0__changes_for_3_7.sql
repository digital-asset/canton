-- Offset corresponding to the offboarding of the latest locally hosted stakeholder, if any
-- Set only if the reassignment is incomplete unassigned at offset incomplete_last_stakeholder_offboarded_target_offset
alter table par_reassignments add column incomplete_last_stakeholder_offboarded_target_offset bigint default null;

-- Rename the offset store table to preserve lowercase casing
alter table PEKKO_PROJECTION_OFFSET_STORE rename to "pekko_projection_offset_store";

alter table "pekko_projection_offset_store" alter column PROJECTION_NAME rename to "projection_name";
alter table "pekko_projection_offset_store" alter column PROJECTION_KEY rename to "projection_key";
alter table "pekko_projection_offset_store" alter column CURRENT_OFFSET rename to "current_offset";
alter table "pekko_projection_offset_store" alter column MANIFEST rename to "manifest";
alter table "pekko_projection_offset_store" alter column MERGEABLE rename to "mergeable";
alter table "pekko_projection_offset_store" alter column LAST_UPDATED rename to "last_updated";

-- Rename the management table to preserve lowercase casing
alter table PEKKO_PROJECTION_MANAGEMENT rename to "pekko_projection_management";

alter table "pekko_projection_management" alter column PROJECTION_NAME rename to "projection_name";
alter table "pekko_projection_management" alter column PROJECTION_KEY rename to "projection_key";
alter table "pekko_projection_management" alter column PAUSED rename to "paused";
alter table "pekko_projection_management" alter column LAST_UPDATED rename to "last_updated";

-- Rename the index to exact lowercase
alter index PROJECTION_NAME_INDEX rename to "projection_name_index";

alter table sequencer_events drop column topology_timestamp;

drop table lapi_party_entries;

-- See the Postgres counterpart of this migration for the rationale.
create index lapi_events_party_to_participant_party_idx
    on lapi_events_party_to_participant (party, event_sequential_id desc);

-- See the Postgres counterpart of this migration for the rationale.
create index lapi_events_party_to_participant_party_id_idx
    on lapi_events_party_to_participant (party_id, participant_id, synchronizer_id, event_sequential_id desc);

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

alter table sequencer_members
drop column enabled;
--- ^^^ Allow traffic purchases for non-registered participants (#34369) ^^^
