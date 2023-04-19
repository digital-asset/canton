-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates
--
-- Proprietary code. All rights reserved.

create table blocks (
    id bigint generated always as identity primary key,
    event binary large object not null,
    uuid varchar(36) unique not null
);