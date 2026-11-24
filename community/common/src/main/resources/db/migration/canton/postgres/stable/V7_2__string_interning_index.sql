-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create index lapi_string_interning_es_idx on lapi_string_interning using btree (external_string) include (internal_id);
