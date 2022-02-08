-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table register_topology_transaction_responses (
  request_id varchar(300) primary key,
  response binary large object not null,
  completed boolean not null
);
