-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- More predictable pagination for retrieving complete synchronizer topology
create index idx_common_topology_transactions_store_id_id
    on common_topology_transactions
        using btree (store_id, id);
