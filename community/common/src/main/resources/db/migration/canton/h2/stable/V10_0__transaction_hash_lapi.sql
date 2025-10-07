-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Update LAPI event tables to add the external_transaction_hash column
ALTER TABLE lapi_events_consuming_exercise
    ADD COLUMN external_transaction_hash bytea;

ALTER TABLE lapi_events_create
    ADD COLUMN external_transaction_hash bytea;

ALTER TABLE lapi_events_non_consuming_exercise
    ADD COLUMN external_transaction_hash bytea;
