// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer

import com.digitalasset.canton.sequencing.protocol.AggregationId
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberId
import com.digitalasset.canton.util.signalling.Notification

package object sequencer {
  type InFlightAggregationUpdates = Map[AggregationId, InFlightAggregationUpdate]
  type WriteNotification = Notification[SequencerMemberId]
}
