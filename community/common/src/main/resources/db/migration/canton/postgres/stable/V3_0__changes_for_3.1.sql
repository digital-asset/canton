-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- This index helps fetching the latest checkpoint for a member
create index idx_sequencer_counter_checkpoints_by_member_ts on sequencer_counter_checkpoints(member, ts);
