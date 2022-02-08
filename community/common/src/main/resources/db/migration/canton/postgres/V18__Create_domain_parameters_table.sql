-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- the (current) domain parameters for the given domain
create table static_domain_parameters (
    domain_id varchar(300) primary key,
    -- serialized form
    params bytea not null
);
