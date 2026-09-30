// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.topology.SynchronizerId

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateService]]
  */
trait IndexStateService extends LedgerEndService {
  def highestOffsetBeforeOrFirstAt(synchronizerId: SynchronizerId, recordTime: CantonTimestamp)(
      implicit loggingContext: LoggingContextWithTrace
  ): Future[Option[Offset]]

  def latestPrunedOffset()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Offset]]
}
