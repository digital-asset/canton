// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.GlobalCausalOrderer
import com.digitalasset.canton.participant.protocol.SingleDomainCausalTracker.EventPerPartyCausalState
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.protocol.messages.VectorClock
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId, RequestCounter}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.HashSet

class GlobalCausalOrdererTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  lazy val participant: ParticipantId = ParticipantId("p1-mydomain")

  lazy val domain1: DomainId = DomainId.tryFromString("domain1::namespace")
  lazy val domain2: DomainId = DomainId.tryFromString("domain2::namespace")
  lazy val domain3: DomainId = DomainId.tryFromString("domain3::namespace")
  lazy val domain4: DomainId = DomainId.tryFromString("domain4::namespace")

  lazy val alice: LfPartyId = LfPartyId.assertFromString("alice")
  lazy val bob: LfPartyId = LfPartyId.assertFromString("bob")

  def createForTesting(
      connectedDomains: List[DomainId] = List(domain1, domain2, domain3, domain4)
  ): GlobalCausalOrderer = {
    new GlobalCausalOrderer(
      participant,
      HashSet(connectedDomains: _*),
      DefaultProcessingTimeouts.testing,
      new InMemoryMultiDomainCausalityStore(loggerFactory),
      loggerFactory,
    )
  }

  private lazy val uniqueTimestamps: AtomicInteger = new AtomicInteger(0)

  private def eventClock(
      domain: DomainId,
      dependencies: Map[LfPartyId, Map[DomainId, CantonTimestamp]],
      rc: RequestCounter,
      timestamp: CantonTimestamp =
        CantonTimestamp.Epoch.plusSeconds(uniqueTimestamps.getAndIncrement().toLong),
  ): EventPerPartyCausalState =
    EventPerPartyCausalState((domain), timestamp, rc.asLocalOffset)(dependencies)

  "basic ordering of two events" in {
    val sut = createForTesting()

    val clock2 = eventClock(domain2, Map(), RequestCounter(0))
    val clock1 =
      eventClock(domain1, Map(alice -> Map(domain2 -> clock2.localTs)), RequestCounter(0))

    val canPublish1F = sut.waitPublishable(clock1.clock)
    val canPublish2F = sut.waitPublishable(clock2.clock)

    sut.flush().futureValue
    canPublish1F.isCompleted shouldBe false

    val () = canPublish2F.futureValue
    sut.registerPublished(clock2.clock)

    val () = canPublish1F.futureValue

  }

  "ordering where an event has multiple causal dependencies on different domains" in {
    val sut = createForTesting(connectedDomains = List(domain1, domain2, domain4))

    val clock1 = eventClock(domain1, Map(), RequestCounter(0))
    val clock2 = eventClock(domain2, Map(), RequestCounter(0))
    val clock4 = eventClock(
      domain4,
      Map(
        alice -> Map(
          domain3 -> CantonTimestamp.Epoch.plusSeconds(3), // Not "connected" to domain 3
          domain2 -> clock2.localTs,
          domain1 -> clock1.localTs,
        )
      ),
      RequestCounter(0),
    )

    val canPublish1F = sut.waitPublishable(clock1.clock)
    val canPublish2F = sut.waitPublishable(clock2.clock)
    val canPublish4F = sut.waitPublishable(clock4.clock)

    sut.flush().futureValue
    canPublish4F.isCompleted shouldBe false

    canPublish1F.futureValue
    canPublish2F.futureValue

    sut.registerPublished(clock1.clock)

    sut.flush().futureValue
    canPublish4F.isCompleted shouldBe false

    sut.registerPublished(clock2.clock)

    canPublish4F.futureValue
  }

  "allow out-of-order publishing on a single domain" in {
    // We rely on the event publication task to enforce the single domain ordering, not the global causal orderer

    val sut = createForTesting()

    val clock2 = eventClock(domain2, Map(), RequestCounter(0))
    val clock3 = eventClock(domain1, Map(), RequestCounter(1))
    val clock1 =
      eventClock(domain1, Map(alice -> Map(domain2 -> clock2.localTs)), RequestCounter(0))

    val canPublish1F = sut.waitPublishable(clock1.clock)
    val canPublish2F = sut.waitPublishable(clock2.clock)
    val canPublish3F = sut.waitPublishable(clock3.clock)

    sut.flush().futureValue
    canPublish1F.isCompleted shouldBe false

    // clock3 is publishable before clock1 despite being a later event on the same domain
    val () = canPublish3F.futureValue

    val () = canPublish2F.futureValue
    sut.registerPublished(clock2.clock)

    canPublish1F.futureValue
  }

  "an event has several causal children" in {
    val sut = createForTesting()

    val clock1 = eventClock(domain2, Map(), RequestCounter(0))
    val clock2 =
      eventClock(domain1, Map(alice -> Map(domain2 -> clock1.localTs)), RequestCounter(0))
    val clock3 =
      eventClock(
        domain1,
        Map(alice -> Map(domain2 -> clock1.localTs.minusSeconds(1))),
        RequestCounter(0),
      )

    val canPublish1F = sut.waitPublishable(clock1.clock)
    val canPublish2F = sut.waitPublishable(clock2.clock)
    val canPublish3F = sut.waitPublishable(clock3.clock)

    sut.flush().futureValue
    canPublish2F.isCompleted shouldBe false
    canPublish3F.isCompleted shouldBe false

    val () = canPublish1F.futureValue
    sut.registerPublished(clock1.clock)

    val () = canPublish2F.futureValue
    val () = canPublish3F.futureValue
  }

  "an event is causally dependant on two events on a single domain" in {
    val sut = createForTesting()

    val clock2 = eventClock(domain2, Map(), RequestCounter(1))
    val clock3 = eventClock(domain2, Map(), RequestCounter(0))
    val clock1 =
      eventClock(domain1, Map(alice -> Map(domain2 -> clock3.localTs)), RequestCounter(0))

    val canPublish1F = sut.waitPublishable(clock1.clock)
    val canPublish2F = sut.waitPublishable(clock2.clock)
    val canPublish3F = sut.waitPublishable(clock3.clock)

    sut.flush().futureValue
    canPublish1F.isCompleted shouldBe false

    val () = canPublish2F.futureValue
    val () = canPublish3F.futureValue

    sut.registerPublished(clock2.clock)

    sut.flush().futureValue
    canPublish1F.isCompleted shouldBe false

    sut.registerPublished(clock3.clock)

    canPublish1F.futureValue

  }

  "an event is causally dependant on different domains for different parties" in {
    val sut = createForTesting()

    val clock2 = eventClock(domain2, Map(), RequestCounter(1))
    val clock3 = eventClock(domain3, Map(), RequestCounter(0))
    val clock1 =
      eventClock(
        domain1,
        Map(alice -> Map(domain3 -> clock3.localTs), bob -> Map(domain2 -> clock2.localTs)),
        RequestCounter(0),
      )

    val canPublish1F = sut.waitPublishable(clock1.clock)
    val canPublish2F = sut.waitPublishable(clock2.clock)
    val canPublish3F = sut.waitPublishable(clock3.clock)

    sut.flush().futureValue
    canPublish1F.isCompleted shouldBe false

    val () = canPublish2F.futureValue
    val () = canPublish3F.futureValue

    sut.registerPublished(clock2.clock)

    sut.flush().futureValue
    canPublish1F.isCompleted shouldBe false

    sut.registerPublished(clock3.clock)

    canPublish1F.futureValue

  }

  "transfer-ins block on transfer-outs" in {
    val sut = createForTesting()

    val id = TransferId(sourceDomain = domain1, CantonTimestamp.Epoch)
    val party = LfPartyId.assertFromString("Alice::domain")
    val vc =
      VectorClock(domain1, CantonTimestamp.Epoch, party, Map.empty)

    val registeredF = sut.awaitTransferOutRegistered(id, Set(party))

    sut.flush().futureValue
    registeredF.isCompleted shouldBe false

    sut.domainCausalityStore.registerTransferOut(id, Set(vc))

    sut.flush().futureValue
    val map = registeredF.futureValue
    map shouldBe Map(party -> VectorClock(domain1, CantonTimestamp.Epoch, party, Map.empty))

  }

}
