-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Add traffic_cost column to the lapi_command_completions to make the cost available on completions
alter table lapi_command_completions add column traffic_cost bigint default null;
