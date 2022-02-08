-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- table to contain the values provided by the domain to the sequencer node for initialization.
-- we persist these values to ensure that the sequencer can always initialize itself with these values
-- even if it was to crash during initialization.
create table sequencer_domain_configuration (
  -- this lock column ensures that there can only ever be a single row: https://stackoverflow.com/questions/3967372/sql-server-how-to-constrain-a-table-to-contain-a-single-row
  lock char(1) not null default 'X' primary key check (lock = 'X'),
  domain_id varchar(300) not null,
  static_domain_parameters binary large object not null
);
