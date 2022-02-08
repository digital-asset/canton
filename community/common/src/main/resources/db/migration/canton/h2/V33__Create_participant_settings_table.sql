-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table participant_settings(
  client integer primary key, -- dummy field to enforce at most one row
  max_dirty_requests integer,
  max_rate integer,
  max_deduplication_time binary large object, -- non-negative finite duration
  -- whether the participant provides unique-contract key semantics
  -- Don't pre-fill the table from what we find in the domain alias store. We do that dynamically at startup
  -- because we can't parse the Protobuf-encoded domain parameters in a SQL script to detect
  -- whether the participant has previously been connected to a UCK domain
  unique_contract_keys boolean
);
