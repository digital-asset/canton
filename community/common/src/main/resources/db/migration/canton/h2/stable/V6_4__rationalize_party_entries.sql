-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop index lapi_party_entries_idx;

alter table lapi_party_entries drop constraint check_party_entry_type;

alter table lapi_party_entries drop column recorded_at, submission_id, typ, rejection_reason;
