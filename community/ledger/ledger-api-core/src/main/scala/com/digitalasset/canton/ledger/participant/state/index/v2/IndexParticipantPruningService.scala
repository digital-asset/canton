// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import com.daml.logging.LoggingContext
import com.digitalasset.canton.ledger.offset.Offset

import scala.concurrent.Future

/** Serves as a backend to implement
  * ParticipantPruningService.
  */
trait IndexParticipantPruningService {
  def prune(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      loggingContext: LoggingContext
  ): Future[Unit]

}
