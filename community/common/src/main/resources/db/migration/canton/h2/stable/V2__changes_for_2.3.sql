-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- TODO(#9014) migrate to non null
ALTER TABLE topology_transactions ADD sequenced bigint null;

-- A stored HMAC secret is not used anymore
DROP TABLE crypto_hmac_secret;
