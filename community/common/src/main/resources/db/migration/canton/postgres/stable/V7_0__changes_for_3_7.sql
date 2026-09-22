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
