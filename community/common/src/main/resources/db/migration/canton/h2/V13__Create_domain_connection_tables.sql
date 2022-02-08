-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table participant_domain_connection_configs(
      domain_alias varchar(300) not null primary key,
      config binary large object -- the protobuf-serialized versioned domain connection config
);

-- used to register all domains that a participant connects to
create table participant_domains(
      -- to keep track of the order domains were registered
      order_number serial not null primary key,
      -- domain human readable alias
      alias varchar(300) not null unique,
      -- domain node id
      domain_id varchar(300) not null unique,
      CONSTRAINT participant_domains_unique unique (alias, domain_id)
);
