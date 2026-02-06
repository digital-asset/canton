// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{DbConfig, PositiveDurationSeconds, StorageConfig}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseH2,
  UsePostgres,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.performance.util.ParticipantSimulator
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import monocle.macros.syntax.lens.*

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Trivial test which can be used as a first end to end test */
sealed trait ParticipantSimulatorIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.topology.dispatchQueueBackpressureLimit)
            .replace(NonNegativeInt.tryCreate(10000))
        ),
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.topology.dispatchQueueBackpressureLimit)
            .replace(NonNegativeInt.tryCreate(10000))
        ),
      )
      .addConfigTransform(ConfigTransforms.disableAdditionalConsistencyChecks)

  "we can run a trivial ping" in { implicit env =>
    import env.*

    clue("participant1 connect") {
      participant1.synchronizers.connect_local(sequencer1, daName)
    }
    clue("maybe ping") {
      participant1.health.maybe_ping(
        participant1,
        timeout = 30.seconds,
      ) shouldBe defined
    }

    synchronizerOwners1.foreach(
      _.topology.synchronizer_parameters
        .propose_update(
          daId,
          _.update(reconciliationInterval = PositiveDurationSeconds.ofSeconds(1)),
        )
    )

    participant1.dars.upload(CantonExamplesPath)

    val numParties = 50
    val numObservers = 3
    val numParticipants = 10
    val numContracts = 1000

    require(numObservers <= numParticipants)

    // copy the packages to vet from participant1
    val packagesToVet = participant1.topology.vetted_packages
      .list(daId, filterParticipant = participant1.filterString)
      .loneElement
      .item
      .packages

    val simulator = new ParticipantSimulator(
      sequencer1,
      Seq(sequencer1),
      sequencer1.synchronizer_parameters.static.get().toInternal,
      respondToAcsCommitments = true,
      environment.loggerFactory,
      environmentTimeouts,
    )
    simulator.uploadRootCert()

    // onboard the participants
    val pids = Seq.tabulate(numParticipants)(i =>
      simulator
        .onboardNewVirtualParticipant(s"vp-$i", packagesToVet, synchronize = None)
        .value
        .futureValueUS
        .value
    )

    eventually(timeUntilSuccess = 2.minutes) {
      sequencer1.topology.synchronizer_trust_certificates
        .list(daId)
        .size should be >= numParticipants
    }

    // start the virtual participant subscriptions
    val closables = pids.map(simulator.startSubscriptionForParticipant(_, _ => {}))

    // create an iterator of endless observing participant groups
    val observerGroups = Iterator.unfold(pids.toVector) { ps =>
      val (observers, rest) = ps.splitAt(numObservers)
      Some(observers -> (rest ++ observers))
    }

    // allocate parties hosted with different observing participants
    val partyIds = (1 to numParties).zip(observerGroups).map { case (partyIndex, observers) =>
      val partyId = PartyId.tryCreate(s"party-$partyIndex", participant1.namespace)
      Seq[InstanceReference](participant1, sequencer1).foreach(
        _.topology.party_to_participant_mappings.propose(
          partyId,
          newParticipants = (participant1.id -> Submission) +: observers.map(_ -> Observation),
          synchronize = None,
          store = daId,
        )
      )
      partyId
    }

    // make sure parties have been fully allocated from participant1' POV
    partyIds.foreach { party =>
      eventually() {
        participant1.parties
          .list(party.filterString)
          .loneElement
          .participants should have size (numObservers.toLong + 1)
      }
    }

    // Future for observing the expected number of contracts for all allocated parties
    val receivedAllContracts = Future(
      participant1.ledger_api.updates
        .transactions(partyIds.toSet, completeAfter = PositiveInt.tryCreate(numContracts))
    )

    // create IOUs asynchronously. participant goes VROOOOOM
    Iterator
      .continually(partyIds)
      .flatten
      .take(numContracts)
      .toSeq
      .foreach { party =>
        val createIouCmds = IouSyntax.testIou(party, party).create().commands().asScala.toSeq
        participant1.ledger_api.javaapi.commands.submit_async(
          Seq(party),
          createIouCmds,
          daId,
        )
      }

    // wait for all contracts to be fully processed
    receivedAllContracts.futureValue.discard

    closables.foreach(_.close())
  }
}

class ParticipantSimulatorIntegrationTestInMemory extends ParticipantSimulatorIntegrationTest {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransform(ConfigTransforms.allInMemory)
      .addConfigTransform(_.focus(_.monitoring.logging.api.messagePayloads).replace(false))

  registerPlugin(new UseReferenceBlockSequencer[StorageConfig.Memory](loggerFactory))

}

class ParticipantSimulatorReferenceIntegrationTestH2 extends ParticipantSimulatorIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}

class ParticipantSimulatorReferenceIntegrationTestPostgres
    extends ParticipantSimulatorIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

class ParticipantSimulatorBftOrderingIntegrationTestPostgres
    extends ParticipantSimulatorIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
