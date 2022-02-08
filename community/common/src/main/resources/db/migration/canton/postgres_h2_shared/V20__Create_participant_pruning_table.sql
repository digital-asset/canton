-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Data about the last pruning operations
create table pruning_operation (
    -- dummy field to enforce a single row
    name varchar(40) not null primary key,
    started_up_to_inclusive bigint,
    completed_up_to_inclusive bigint
);
