-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter table common_pending_operations drop column operation_trigger cascade;
drop type pending_operation_trigger_type cascade;
create or replace view debug.common_pending_operations as
  select
    operation_name,
    operation_key,
    operation,
    synchronizer_id
  from common_pending_operations;
