-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Renaming this file:  V1_1__* instead of V1__* to prevent clash with Flyway baseline schema considered as V1
create table daml_packages (package_id varchar(300) not null primary key, data bytea not null, source_description varchar not null default 'default')
