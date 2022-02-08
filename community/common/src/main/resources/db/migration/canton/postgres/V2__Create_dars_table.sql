-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table dars (hash_hex varchar(300) not null  primary key, hash bytea not null, data bytea not null, name varchar(300) not null)
