// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.DomainId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinaryApplicationHandler
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.store.SequencedEventStore
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil._

import scala.concurrent.ExecutionContext

/** Transformer for [[com.digitalasset.canton.sequencing.OrdinaryApplicationHandler]]
  * that stores all event batches in the [[com.digitalasset.canton.store.SequencedEventStore]]
  * before passing them on to the given handler. Complains if events have the wrong domain ID.
  */
class StoreSequencedEvent(
    store: SequencedEventStore,
    domainId: DomainId,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  def apply(
      handler: OrdinaryApplicationHandler[ClosedEnvelope]
  ): OrdinaryApplicationHandler[ClosedEnvelope] =
    tracedEvents =>
      tracedEvents.withTraceContext { implicit batchTraceContext => events =>
        val wrongDomainEvents = events.filter(_.signedEvent.content.domainId != domainId)
        for {
          _ <- FutureUnlessShutdown.outcomeF(
            ErrorUtil.requireArgumentAsync(
              wrongDomainEvents.isEmpty, {
                val wrongDomainIds = wrongDomainEvents.map(_.signedEvent.content.domainId).distinct
                val wrongDomainCounters = wrongDomainEvents.map(_.signedEvent.content.counter)
                show"Cannot store sequenced events from domains $wrongDomainIds in store for domain $domainId\nSequencer counters: $wrongDomainCounters"
              },
            )
          )
          _ <- FutureUnlessShutdown.outcomeF(store.store(events))
          result <- handler(tracedEvents)
        } yield result
      }
}

object StoreSequencedEvent {
  def apply(store: SequencedEventStore, domainId: DomainId, loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext
  ): StoreSequencedEvent =
    new StoreSequencedEvent(store, domainId, loggerFactory)
}
