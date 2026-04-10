// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.time

import com.digitalasset.canton.data.CantonTimestamp

/** This indicates that all messages with sequencing time >= ts should not be delivered. SHOULD be
  * used only in disaster recovery scenarios. The value MUST be the same on all sequencers. The
  * value MUST be set before the time reaches ts.
  *
  * @param ts
  *   The max sequencing time (inclusive).
  */
final case class DisasterRecoverySequencingTimeUpperBound(ts: CantonTimestamp) extends AnyVal
