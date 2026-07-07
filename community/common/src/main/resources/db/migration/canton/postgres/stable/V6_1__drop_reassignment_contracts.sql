-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- The 'stakeholders' column was added and backfilled from 'contracts' by the preceding Scala
-- migration (V6_0__reassignment_store_stakeholders.scala). The redundant 'contracts' blob column
-- can now be dropped. The debug view
-- depends on the column, so it is dropped first and recreated against 'stakeholders'.
drop view debug.par_reassignments;

alter table par_reassignments alter column stakeholders set not null;
alter table par_reassignments drop column contracts;

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
    debug.canton_timestamp(assignment_timestamp) as assignment_timestamp
  from par_reassignments;
