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
