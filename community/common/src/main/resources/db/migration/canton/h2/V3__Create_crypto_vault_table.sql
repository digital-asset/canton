-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table crypto_hmac_secret (
    hmac_secret_id integer default 1,
    data binary large object not null,

    -- We only support a single stored HMAC secret right now, enforced through an ID that must be 1.
    primary key (hmac_secret_id),
    constraint hmac_secret_only_one check (hmac_secret_id = 1)
);

create table crypto_private_keys (
    -- fingerprint of the key
    key_id varchar(300) primary key,
    -- key purpose identifier
    purpose smallint not null,
    -- Protobuf serialized key including metadata
    data binary large object not null,
    -- optional name of the key
    name varchar(300)
);

create table crypto_certs (
    cert_id varchar(300) primary key,
    -- PEM serialized certificate
    data binary large object not null
);

create table crypto_public_keys (
    -- fingerprint of the key
    key_id varchar(300) primary key,
    -- key purpose identifier
    purpose smallint not null,
    -- Protobuf serialized key
    data binary large object not null,
    -- optional name of the key
    name varchar(300)
);
