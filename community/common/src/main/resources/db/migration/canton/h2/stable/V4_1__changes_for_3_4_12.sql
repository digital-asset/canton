-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter table common_pending_operations drop column operation_trigger;
drop type pending_operation_trigger_type;
