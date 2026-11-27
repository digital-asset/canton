// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.daml.ledger.api.v2.state_service.GetHostedPartiesResponse
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.util.PageTokenUtils
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

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

  /** Streams matching hosted parties at an inclusive snapshot offset.
    *
    * @param filter
    *   the parties, participants and synchronizers of interest
    * @param validAt
    *   The inclusive snapshot offset, not the topology-event offsets in the returned hostings.
    * @param fromExcl
    *   only parties positioned after this party in the stream order are streamed
    * @param limit
    *   The maximum number of parties streamed. A party's matching hostings are not split.
    * @param continuationChecksum
    *   the request checksum embedded in the stream continuation token of each response
    */
  def hostedParties(
      filter: IndexerHostedPartyFilter,
      validAt: Offset,
      fromExcl: Option[Ref.Party],
      limit: Option[Int],
      continuationChecksum: PageTokenUtils.Checksum,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetHostedPartiesResponse, NotUsed]
}
