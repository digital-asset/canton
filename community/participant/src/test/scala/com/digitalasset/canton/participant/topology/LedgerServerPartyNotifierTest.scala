// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, ParticipantEventPublisher}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.memory.InMemoryPartyMetadataStore
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.{BaseTest, LedgerParticipantId}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

class LedgerServerPartyNotifierTest extends AsyncWordSpec with BaseTest {

  import com.digitalasset.canton.topology.DefaultTestIdentities.*
  import com.digitalasset.canton.topology.client.EffectiveTimeTestHelpers.*

  private lazy val crypto =
    new TestingOwnerWithKeys(domainManager, loggerFactory, directExecutionContext)

  private lazy val ts = CantonTimestamp.Epoch

  private class Fixture {
    val store = new InMemoryPartyMetadataStore()
    val observed = ListBuffer[LedgerSyncEvent]()
    val eventPublisher = mock[ParticipantEventPublisher]
    val clock = mock[Clock]
    when(clock.now).thenReturn(ts)
    when(clock.scheduleAt(any[CantonTimestamp => Unit], any[CantonTimestamp]))
      .thenAnswer[CantonTimestamp => Unit, CantonTimestamp] { case (action, _) =>
        action(ts)
        FutureUnlessShutdown.unit
      }
    val notifier =
      new LedgerServerPartyNotifier(
        participant1,
        eventPublisher,
        store,
        clock,
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      )

    when(eventPublisher.publish(any[LedgerSyncEvent])(anyTraceContext)).thenAnswer {
      event: LedgerSyncEvent =>
        observed += event
        Future.unit
    }

    def evaluate(
        expectedPartyId: PartyId,
        expectedDisplayName: String,
        expectedParticipantId: String,
    ): Assertion = {
      observed should have length (1)
      observed.collect {
        case LedgerSyncEvent.PartyAddedToParticipant(
              partyId,
              displayName,
              participantId,
              _,
              _,
            ) =>
          (partyId, displayName, participantId)
      } shouldBe ListBuffer(
        (
          expectedPartyId.toLf,
          expectedDisplayName,
          LedgerParticipantId.assertFromString(expectedParticipantId),
        )
      )
    }

  }

  private def genTransactionFromMapping(mapping: TopologyStateUpdateMapping) =
    crypto.mkTrans(TopologyStateUpdate.createAdd(mapping, testedProtocolVersion))(
      directExecutionContext
    )

  private def genTransaction(partyId: PartyId, participantId: ParticipantId) = {
    genTransactionFromMapping(
      PartyToParticipant(RequestSide.From, partyId, participantId, ParticipantPermission.Submission)
    )
  }

  "ledger server notifier" should {

    "update party to participant mappings" in {
      val fixture = new Fixture()
      for {
        _ <- fixture.notifier.observedF(ts, ts, genTransaction(party1, participant1)).unwrap
        _ <- fixture.notifier.flush()
      } yield fixture.evaluate(party1, "", participant1.uid.toProtoPrimitive)
    }

    "update display names" in {
      val fixture = new Fixture()
      val testMe = String255.tryCreate("TestMe")
      for {
        _ <- fixture.notifier.observedF(ts, ts, genTransaction(party1, participant1)).unwrap
        _ <- fixture.notifier.flush()
        _ = fixture.observed.clear()
        _ <- fixture.notifier.setDisplayName(party1, testMe)
        _ <- fixture.notifier.flush()
      } yield fixture.evaluate(party1, testMe.unwrap, participant1.uid.toProtoPrimitive)
    }

    "add admin parties" in {
      val fixture = new Fixture()
      for {
        _ <- fixture.notifier
          .observedF(
            ts,
            ts,
            genTransactionFromMapping(
              ParticipantState(
                RequestSide.Both,
                domainId,
                participant1,
                ParticipantPermission.Submission,
                TrustLevel.Vip,
              )
            ),
          )
          .unwrap
        _ <- fixture.notifier.flush()
      } yield fixture.evaluate(
        participant1.adminParty,
        "",
        participant1.uid.toProtoPrimitive,
      )
    }

    "prefer current participant" in {
      val fixture = new Fixture()
      for {
        _ <- fixture.notifier.observedF(ts, ts, genTransaction(party1, participant1)).unwrap
        _ <- fixture.notifier.observedF(ts, ts, genTransaction(party1, participant2)).unwrap
        _ <- fixture.notifier.flush()
      } yield fixture.evaluate(party1, "", participant1.uid.toProtoPrimitive)

    }

    "combine names and ids" in {
      val fixture = new Fixture()
      val test = String255.tryCreate("test")
      for {
        _ <- fixture.notifier.observedF(ts, ts, genTransaction(party1, participant1)).unwrap
        _ <- fixture.notifier.flush()
        _ = fixture.observed.clear()
        _ <- fixture.notifier.setDisplayName(party1, test)
        _ <- fixture.notifier.flush()
      } yield fixture.evaluate(party1, test.unwrap, participant1.uid.toProtoPrimitive)
    }

    "not send duplicate updates" in {
      val fixture = new Fixture()
      for {
        _ <- fixture.notifier.observedF(ts, ts, genTransaction(party1, participant1)).unwrap
        _ <- fixture.notifier.observedF(ts, ts, genTransaction(party1, participant1)).unwrap
        _ <- fixture.notifier.flush()
      } yield {
        fixture.observed should have length (1)
      }
    }

  }

}
