-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Partial index for pruning
create index idx_contracts_request_counter on contracts(domain_id, request_counter) where creating_transaction_id is null;