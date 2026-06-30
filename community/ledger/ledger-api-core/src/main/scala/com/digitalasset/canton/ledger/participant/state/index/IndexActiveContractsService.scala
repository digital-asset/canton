// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.EventFormat
import com.digitalasset.canton.ledger.api.messages.state.AcsRangeInfo
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref.Party
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateService]]
  */
trait IndexActiveContractsService {

  def getActiveContracts(
      eventFormat: EventFormat,
      activeAt: Option[Offset],
      rangeInfo: AcsRangeInfo,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed]

  def acs(
      synchronizerId: SynchronizerId,
      activeAt: Offset,
      stakeholders1: Set[Party],
      stakeholders2: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[InternalIndexService.ActiveContract, NotUsed]

}
