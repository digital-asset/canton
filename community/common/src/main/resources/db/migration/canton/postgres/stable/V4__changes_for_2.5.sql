-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE INDEX active_contracts_pruning_idx on active_contracts (domain_id, ts) WHERE change = 'deactivation';

ALTER TABLE contracts ADD COLUMN contract_salt bytea;
