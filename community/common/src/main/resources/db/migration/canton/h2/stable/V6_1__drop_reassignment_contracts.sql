-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- The 'stakeholders' column was added and backfilled from 'contracts' by the preceding Scala
-- migration (V6_0__reassignment_store_stakeholders.scala). The redundant 'contracts' blob column
-- can now be dropped.
alter table par_reassignments alter column stakeholders set not null;
alter table par_reassignments drop column contracts;
