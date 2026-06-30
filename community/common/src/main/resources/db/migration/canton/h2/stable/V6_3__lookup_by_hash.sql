-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Accepted transactions are looked up by their (unique) hash here.
ALTER TABLE lapi_update_meta ADD COLUMN transaction_hash bytea;
CREATE INDEX idx_lapi_update_meta_tx_hash
    ON lapi_update_meta (transaction_hash);

-- Only rejections (update_id IS NULL) are looked up by hash on completions;
-- accepted commands are found via lapi_update_meta above. A rejection hash is
-- not unique (retries yield multiple rejected rows), so completion_offset is
-- part of the key to order them.
ALTER TABLE lapi_command_completions ADD COLUMN transaction_hash bytea;
CREATE INDEX idx_lapi_command_completions_tx_hash
    ON lapi_command_completions (transaction_hash, completion_offset);
