// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.MultiDomainCausalityStore
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.protocol.messages.VectorClock
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

// Whilst the [[com.digitalasset.canton.participant.store.MultiDomainCausalityStore]] maintains all the state needed in-memory,
// this store does not need to do anything. However, it is expected that this changes in future.
class InMemoryMultiDomainCausalityStore(val loggerFactory: NamedLoggerFactory)(implicit
    val ec: ExecutionContext
) extends MultiDomainCausalityStore {

  override def loadTransferOutStateFromPersistentStore(
      transferId: TransferId,
      parties: Set[LfPartyId],
  )(implicit tc: TraceContext): Future[Option[Map[LfPartyId, VectorClock]]] =
    Future.successful(None)

  override protected def persistCausalityMessageState(
      id: TransferId,
      vectorClocks: List[VectorClock],
  )(implicit tc: TraceContext): Future[Unit] = Future.unit

}
