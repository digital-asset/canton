// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.SynchronizerId

final case class ConvertRecordTimeToOffsetRequest(
    recordTime: CantonTimestamp,
    synchronizerId: SynchronizerId,
)
