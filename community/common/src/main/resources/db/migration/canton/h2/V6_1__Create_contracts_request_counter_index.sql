-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Index for pruning
-- Using an index on all elements because H2 does not support partial indices.
create index idx_contracts_request_counter on contracts(domain_id, request_counter);