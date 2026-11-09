// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.api.v2.EventOuterClass
import com.digitalasset.canton.BigDecimalImplicits.DoubleToBigDecimal
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.topology.PartyId
import org.scalatest.OptionValues

import scala.jdk.CollectionConverters.*

trait ConsoleCommandIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with HasCycleUtils
    with EntitySyntax
    with OptionValues {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  override val defaultParticipant: String = "participant1"

  private def connectParticipant()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    utils.retry_until_true(participant1.synchronizers.active(daName))
  }

  private def prepareContracts()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    connectParticipant()
    val _ = participant1.dars.upload(CantonExamplesPath)
  }

  "the command enable" should {
    "succeed when party name has length 185" in { implicit env =>
      import env.*

      connectParticipant()
      participant1.parties.enable("a" * 185)
    }
  }

  "the command allocate" should {
    "succeed when party name has length at most 185" in { implicit env =>
      // see commands in `UniqueIdentifier` for why this will fail with party names of length >185
      import env.*

      connectParticipant()
      participant1.ledger_api.parties.allocate("a" * 185, synchronizerId = daId.logical)
    }
  }

  "the command acs.of_all" should {
    "succeed when never connected to a synchronizer" in { implicit env =>
      import env.*

      participant1.synchronizers.list_all_registered() shouldBe empty
      participant1.ledger_api.state.acs.count() shouldBe 0
    }

    "succeed when disconnected from a synchronizer" in { implicit env =>
      import env.*

      prepareContracts()

      val acsCountBefore = participant1.ledger_api.state.acs.count()
      createCycleContract(participant1, participant1.adminParty, "cycle contract p1 p2")
      participant1.synchronizers.disconnect(daName)

      val acsCountAfter = participant1.ledger_api.state.acs.count()
      (acsCountAfter - acsCountBefore) shouldBe 1
    }

    "succeed when there are only admin parties" in { implicit env =>
      import env.*

      prepareContracts()

      val acsCountBefore = participant1.ledger_api.state.acs.count()
      createCycleContract(participant1, participant1.adminParty, "cycle contract p1 p2")
      val acsCountAfter = participant1.ledger_api.state.acs.count()

      (acsCountAfter - acsCountBefore) shouldBe 1
    }

    "succeed when parties are allocated" in { implicit env =>
      import env.*

      prepareContracts()

      val alice: String = "Alice"
      participant1.ledger_api.parties.allocate(alice)
      eventually() {
        val plist1 = participant1.parties.list(alice)
        plist1 should have length 1
        plist1.map(_.party.toProtoPrimitive) contains alice
      }

      val acsCountBefore = participant1.ledger_api.state.acs.count()
      acsCountBefore shouldBe 0
      createCycleContract(participant1, alice.toPartyId(), "cycle contract p1 alice")
      val acsCountAfter = participant1.ledger_api.state.acs.count()

      (acsCountAfter - acsCountBefore) shouldBe 1
    }
  }

  "the command acs.of_party" should {
    val alice: String = "Alice"

    "succeed when never connected to a synchronizer" in { implicit env =>
      import env.*

      participant1.synchronizers.list_all_registered() shouldBe empty
      participant1.ledger_api.state.acs.of_party(participant1.adminParty) shouldBe empty
    }

    "succeed when disconnected from a synchronizer" in { implicit env =>
      import env.*

      prepareContracts()
      participant1.ledger_api.parties.allocate(alice)

      val acsP1Before = participant1.ledger_api.state.acs.of_party(alice.toPartyId())
      acsP1Before shouldBe empty
      createCycleContract(participant1, alice.toPartyId(), "cycle contract p1 p2")

      participant1.synchronizers.disconnect(daName)

      val acsP1After = participant1.ledger_api.state.acs.of_party(alice.toPartyId())
      (acsP1After.size - acsP1Before.size) shouldBe 1
    }

    "succeed when there are only admin parties" in { implicit env =>
      import env.*

      prepareContracts()

      val acsBefore = participant1.ledger_api.state.acs.of_party(participant1.adminParty)
      acsBefore shouldBe empty
      createCycleContract(participant1, participant1.adminParty, "cycle contract p1 p2")
      val acsAfter = participant1.ledger_api.state.acs.of_party(participant1.adminParty)

      (acsAfter.size - acsBefore.size) shouldBe 1
    }

    "succeed when parties are allocated" in { implicit env =>
      import env.*

      prepareContracts()
      val aliceId = participant1.parties.enable(alice)

      val acsBefore = participant1.ledger_api.state.acs.of_party(aliceId)

      createCycleContract(participant1, aliceId, "cycle contract p1 alice")
      val acsAfter = participant1.ledger_api.state.acs.of_party(aliceId)

      (acsAfter.size - acsBefore.size) shouldBe 1
    }
  }

  "the service ledger_api.event_query" should {

    def createIou(
        actAs: PartyId,
        participant: LocalParticipantReference,
        payer: PartyId,
        owner: PartyId,
        amount: Amount,
    ): EventOuterClass.CreatedEvent =
      participant.ledger_api.javaapi.commands
        .submit(
          actAs = Seq(actAs),
          commands = new Iou(
            payer.toProtoPrimitive,
            owner.toProtoPrimitive,
            amount,
            List.empty.asJava,
          ).create
            .commands()
            .asScala
            .toSeq,
        )
        .getEvents
        .asScala
        .loneElement
        .toProtoEvent
        .getCreated

    "retrieve events by contract id" in { implicit env =>
      import env.*

      prepareContracts()
      val testPartyId = participant1.parties.enable("consoleCommandTestParty")
      val expected = createIou(
        actAs = testPartyId,
        participant = participant1,
        payer = testPartyId,
        owner = testPartyId,
        amount = new Amount(100.0.toBigDecimal, "USD"),
      )
      val actual = participant1.ledger_api.javaapi.event_query
        .by_contract_id(expected.getContractId, Seq(testPartyId))
        .getCreated
        .getCreatedEvent
      actual shouldBe expected
    }
  }
}

class ConsoleCommandIntegrationTestDefault extends ConsoleCommandIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

//class ConsoleCommandIntegrationTestPostgres extends ConsoleCommandIntegrationTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseBftSequencer(loggerFactory))
//}
