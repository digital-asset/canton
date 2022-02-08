// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.store.{CausalityStoresTest, EventLogId}
import com.digitalasset.canton.participant.sync.TimestampedEvent

import scala.concurrent.Future

class CausalityStoresTestInMemory extends CausalityStoresTest {
  override def persistEvents(
      events: Seq[(EventLogId, TimestampedEvent, Option[GlobalOffset])]
  ): Future[Unit] =
    Future.unit // Nothing to do as we don't have persistence

  "CausalityStoresTestInMemory" should {
    behave like causalityStores(
      () => {
        val mdcs = new InMemoryMultiDomainCausalityStore(loggerFactory)
        val single = new InMemorySingleDomainCausalDependencyStore(writeToDomain, loggerFactory)
        for {
          _unit <- single.initialize(None)
        } yield TestedStores(mdcs, single)
      },
      persistence = false,
    )
  }
}
