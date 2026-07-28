-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop view debug.lapi_party_entries;

drop index lapi_party_entries_idx;

alter table lapi_party_entries
  drop column recorded_at,
  drop column submission_id,
  drop column typ,
  drop column rejection_reason;

create or replace view debug.lapi_party_entries as
select
    ledger_offset,
    party,
    is_local,
    debug.resolve_lapi_interned_string(party_id) as party_id
from lapi_party_entries;
