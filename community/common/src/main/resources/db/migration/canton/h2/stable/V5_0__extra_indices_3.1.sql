-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- This index helps joining traffic receipts without a member reference
create index on seq_traffic_control_consumed_journal(sequencing_timestamp);
