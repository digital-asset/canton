-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- store nonces that have been requested for authentication challenges
create table sequencer_authentication_nonces (
     nonce varchar(300) primary key,
     member varchar(300) not null,
     generated_at_ts bigint not null,
     expire_at_ts bigint not null
);

create index idx_nonces_for_member on sequencer_authentication_nonces (member, nonce);

-- store tokens that have been generated for successful authentication requests
create table sequencer_authentication_tokens (
     token varchar(300) primary key,
     member varchar(300) not null,
     expire_at_ts bigint not null
);

create index idx_tokens_for_member on sequencer_authentication_tokens (member);
