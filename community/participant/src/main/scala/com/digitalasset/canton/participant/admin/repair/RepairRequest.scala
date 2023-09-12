// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{StaticDomainParameters, TransactionId}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.TopologySnapshot

private[repair] final case class RepairRequest(
    domainId: DomainId,
    domainAlias: String, // for logging
    domainParameters: StaticDomainParameters,
    transactionId: TransactionId,
    requestCounters: NonEmpty[Seq[RequestCounter]],
    timestamp: CantonTimestamp,
    context: RepairContext,
)(val domainPersistence: SyncDomainPersistentState, val topologySnapshot: TopologySnapshot) {
  def timesOfChange: Seq[TimeOfChange] =
    requestCounters.map(rc => TimeOfChange(rc, timestamp))

  def requestData: Seq[RequestData] =
    // Trace context persisted explicitly doubling as a marker for repair requests in the request journal
    requestCounters.map(rc => RequestData(rc, RequestState.Pending, timestamp, Some(context)))

  def tryOneRequestCounter: RequestCounter =
    if (requestCounters.sizeIs == 1) requestCounters.head1
    else
      throw new RuntimeException(
        s"Expected 1 request counter, actual size is ${requestCounters.size}"
      )

  def tryOneTimeOfChange: TimeOfChange =
    TimeOfChange(tryOneRequestCounter, timestamp)

}
