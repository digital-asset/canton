-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop table common_party_metadata;

alter table seq_block_height add column latest_pending_topology_ts bigint;
