// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SingleDomainCausalDependencyStore
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPartyId, RequestCounter}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** The [[SingleDomainCausalDependencyStore]]
  * maintains all the state it needs in an in-memory "cache". Persistance is only used for restarts.
  * Therefore the in-memory store does not need to do anything until we have moved away from the in-memory cache.
  */
class InMemorySingleDomainCausalDependencyStore(
    override val domainId: DomainId,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends SingleDomainCausalDependencyStore
    with NamedLogging {
  override val state = new TrieMap()

  override protected def persistentInsert(
      rc: RequestCounter,
      ts: CantonTimestamp,
      deps: Map[LfPartyId, Map[DomainId, CantonTimestamp]],
      transferOutId: Option[TransferId],
  )(implicit tc: TraceContext): Future[Unit] = {
    // This is an in-memory store, so a persistent insert doesn't do anything
    Future.unit
  }

  override def initialize(lastIncluded: Option[RequestCounter])(implicit
      tc: TraceContext
  ): Future[Unit] = Future.unit

  override val initialized: Future[Unit] = Future.unit
}
