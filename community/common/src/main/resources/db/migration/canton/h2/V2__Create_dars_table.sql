-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table dars (hash_hex varchar(300) not null  primary key, hash binary large object not null, data binary large object not null, name varchar(300) not null)
