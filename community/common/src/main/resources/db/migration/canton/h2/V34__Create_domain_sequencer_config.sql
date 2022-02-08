-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table domain_sequencer_config
(
    -- this lock column ensures that there can only ever be a single row: https://stackoverflow.com/questions/3967372/sql-server-how-to-constrain-a-table-to-contain-a-single-row
    lock char(1) not null default 'X' primary key check (lock = 'X'),
    sequencer_connection binary large object not null
);