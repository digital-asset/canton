-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE event_log DROP COLUMN causality_update;
DROP TABLE per_party_causal_dependencies;
