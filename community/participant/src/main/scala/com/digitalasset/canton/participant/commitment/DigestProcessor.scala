// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.data.CantonTimestamp

sealed trait DigestProcessor extends BaseDigestProcessor

trait ReinitializingDigestProcessor extends DigestProcessor {
  def reinitializingTimepoint: Timepoint
}

trait DigestConsistencyCheckProcessor extends BaseDigestProcessor {

  /** Returns the timestamp of the latest started consistency check.
    *
    * None if no check has been started yet.
    */
  def startTimestamp: Option[CantonTimestamp]
}

trait RunningDigestProcessor extends DigestProcessor

object DigestConsistencyCheckProcessor {
  final case class Status(isRunning: Boolean, startTimestamp: Option[CantonTimestamp])
}
