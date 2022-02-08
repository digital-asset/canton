-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- H2 does not (yet) support partial indices, so we index everything
create index idx_event_log_associated_domain on event_log (log_id, associated_domain, ts)
