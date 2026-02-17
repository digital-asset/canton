// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.admin.api.client.data.SequencerConnections
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.OnboardsNewSequencerNode
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.synchronizer.sequencer.time.TimeAdvancingTopologySubscriber.TimeAdvanceBroadcastMessageIdPrefix
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.DurationConverters.*

class TopologyTicksIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with OnboardsNewSequencerNode {

  // using 1 second, instead of the default value because this gives us enough milliseconds so that
  // the passage of time of 1 milli per empty block does not cause transactions to become effective
  // before the test intends them to.
  val epsilon = NonNegativeFiniteDuration.tryCreate(1.seconds.toJava)

  def runAsyncAndAdvanceClockUntilFinished(f: () => Unit, clock: SimClock)(implicit
      ec: ExecutionContext
  ): Unit = {
    val future = Future(f())
    eventually(maxPollInterval = 10.millis) {
      clock.advance(epsilon.duration)
      future.isCompleted shouldBe true
    }
    // important to call this so that we don't swallow the exception in case of a failed future
    future.futureValue
  }

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M1_Config
      .addConfigTransform(ConfigTransforms.useStaticTime)
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(
          // in tests using the sim clock, epsilon is by default set to 0, but for this test we want it to be
          // a non-zero value so we can control when a topology transaction should become effective.
          S1M1.withTopologyChangeDelay(epsilon.toConfig)
        )
      }
      .addConfigTransforms(
        (ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.parameters.producePostOrderingTopologyTicks) // turn on the feature being tested
            .replace(true)
        ) +: ConfigTransforms
          // When submitting new topology transactions while advancing the sim clock, we might end up advancing
          // the clock more than the default topologyTransactionObservationTimeout value, before completing the operation.
          // So by having a higher value here we avoid unnecessary warnings in the test.
          .updateAllNodesTopologyConfig(
            _.focus(_.topologyTransactionObservationTimeout)
              .replace(config.NonNegativeFiniteDuration.ofDays(100))
          ))*
      )

  registerPlugin(new UsePostgres(loggerFactory))
  val bftP = new UseBftSequencer(
    loggerFactory,
    dynamicallyOnboardedSequencerNames = Seq(InstanceName.tryCreate("sequencer2")),
  )
  registerPlugin(bftP)
  override val bftSequencerPlugin: Option[UseBftSequencer] = Some(bftP)
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private val sendPolicy: SendPolicy = { _ => submission =>
    // let's drop any time proof requests and time advancing messages so that only topology ticks can be used
    // to advance time across all clients
    if (TimeProof.isTimeProofSubmission(submission)) SendDecision.Drop
    else if (submission.messageId.unwrap.startsWith(TimeAdvanceBroadcastMessageIdPrefix)) {
      fail(
        "we do not expect any time advancing messages when producePostOrderingTopologyTicks is turned on"
      )
    } else SendDecision.Process
  }

  if (testedProtocolVersion >= ProtocolVersion.v35) {
    "Topology ticks" should {
      "be created to signal that topology transactions have become effective after the topology change delay has passed" in {
        implicit env =>
          import env.*
          val simClock = env.environment.simClock.value
          runAsyncAndAdvanceClockUntilFinished(
            // we need to have this command run while advancing the clock, so that topology transactions
            // (such as SynchronizerTrustCertificate) created during the participant connecting to the sequencer
            // become effective and the process can complete
            () => participant1.synchronizers.connect_local(sequencer1, alias = daName),
            simClock,
          )

          val sequencer = getProgrammableSequencer(sequencer1.name)
          sequencer.setPolicy("drop time proofs and time advancing messages")(sendPolicy)

          // create party with synchronize = None so that we don't wait on the topology transaction to become effective
          val partyId = participant1.parties.enable("testParty1", synchronize = None)

          // we see that it is not effective immediately
          participant1.ledger_api.parties.list().map(_.party) should not contain (partyId)

          // but becomes effective after advancing time enough for a topology tick to be created and signal that the
          // topology transaction is effective.
          eventually() {
            simClock.advance(epsilon.duration)
            participant1.ledger_api.parties.list().map(_.party) should contain(partyId)
            sequencer1.topology.party_to_participant_mappings
              .list(daId)
              .map(_.item.partyId) should contain(partyId)
          }
      }

      "be persisted across restarts" in { implicit env =>
        import env.*
        // doing a ping first to make create some events to make sure sequencer time is caught up to sim clock time
        participant1.health.ping(participant1)

        val simClock = env.environment.simClock.value
        val sequencer = getProgrammableSequencer(sequencer1.name)
        sequencer.setPolicy("drop time proofs and time advancing messages")(sendPolicy)

        // create a new party with synchronize = None so that we don't wait on the topology transaction to become effective
        val partyId = participant1.parties.enable("testParty2", synchronize = None)

        // wait a little bit to make sure the sequencer has registered the topology transaction,
        // before we proceed to test crash recovery
        blocking(Thread.sleep(1000))

        // we see that the topology transaction is not effective immediately
        participant1.ledger_api.parties.list().map(_.party) should not contain (partyId)

        // restart the sequencer
        sequencer1.stop()
        sequencer1.start()

        // the sequencer persisted the timestamp of the latest topology transaction and after restart it is able to tell
        // when it has become effective and emit a topology tick
        eventually() {
          simClock.advance(epsilon.duration)
          participant1.ledger_api.parties.list().map(_.party) should contain(partyId)
          sequencer1.topology.party_to_participant_mappings
            .list(daId)
            .map(_.item.partyId) should contain(partyId)
        }
      }

      // what we want to do in this test case is onboard sequencer2 such that:
      // t1 = new sequencer's topology transaction sequencing time
      // t3 = above transaction's effective time
      // t2, such that t1 > t2 > t3 = another topology transaction's sequencing time
      //   that only becomes effective after the new sequencer is onboarded, but is included in its initial snapshot
      // t4 = above transaction's effective time
      // the newly onboarded sequencer2 should then create a topology tick at t4 being consistent
      // with sequencer1's behavior, in order to avoid a ledger fork.
      "be consistent during onboarding" in { implicit env =>
        import env.*
        val simClock = env.environment.simClock.value
        sequencer1.health.initialized() shouldBe true
        sequencer2.health.initialized() shouldBe false

        runAsyncAndAdvanceClockUntilFinished(
          () =>
            bootstrap
              .upload_new_sequencer_identity_transactions(daId.logical, sequencer2, sequencer1),
          simClock,
        )
        // doing a ping first to make create some events to make sure sequencer time is caught up to sim clock time
        participant1.health.ping(participant1)

        // create the new sequencer's topology transaction without advancing time
        bootstrap.propose_new_sequencer_state(
          daId.logical,
          sequencer2,
          sequencer1,
          initializedSynchronizers(daName).synchronizerOwners,
          synchronize = None, // set to none so we don't wait for transactions to be effective
        )

        // advance time by half epsilon, so that new sequencer's topology transactions are not effective yet
        simClock.advance(epsilon.duration.dividedBy(2))
        participant1.health.ping(participant1)

        // now propose new party topology transactions after new sequencer's topology transactions, but before they are effective
        val partyId = participant1.parties.enable("testParty3", synchronize = None)
        participant1.ledger_api.parties.list().map(_.party) should not contain (partyId)
        // advance time so new sequencer's topology transactions become effective, but not the new party topology transactions
        simClock.advance(epsilon.duration.dividedBy(2).plusMillis(1))

        // complete initialization of new sequencer
        bootstrap.wait_for_sequencer_state_to_be_effective(
          daId.logical,
          sequencer2,
          sequencer1,
          true,
          commandTimeouts.bounded,
        )
        bootstrap.complete_new_sequencer_initialization(sequencer2, sequencer1)
        setUpAdditionalConnections(sequencer1, sequencer2)
        sequencer2.health.wait_for_initialized()
        participant1.health.ping(participant1)

        // both sequencers do not consider party topology transaction effective yet
        sequencers.all.foreach { sequencer =>
          sequencer.topology.party_to_participant_mappings
            .list(daId)
            .map(_.item.partyId) should not contain (partyId)
        }
        // neither participant1
        participant1.ledger_api.parties.list().map(_.party) should not contain (partyId)

        // after advancing clock enough to make new party topology transaction effective,
        // both sequencers should create ticks that will advance time enough to see it effective
        simClock.advance(epsilon.duration)
        eventually() {
          sequencers.all.foreach { sequencer =>
            sequencer.topology.party_to_participant_mappings
              .list(daId)
              .map(_.item.partyId) should contain(partyId)
          }
          participant1.ledger_api.parties.list().map(_.party) should contain(partyId)
        }

        // the newly onboarded sequencer2 should have created the same topology tick as sequencer1
        // based on the new party's topology transaction.
        // we can check that by having participant1 disconnect from sequencer1 and connect to sequencer2 and
        // seeing that it does not complain about a previous timestamp mismatch.
        participant1.synchronizers.disconnect(daName)
        participant1.synchronizers.modify(
          daName,
          _.copy(sequencerConnections = SequencerConnections.single(sequencer2.sequencerConnection)),
        )
        runAsyncAndAdvanceClockUntilFinished(
          () => participant1.synchronizers.reconnect(daName),
          simClock,
        )
        participant1.health.ping(participant1)
      }
    }
  }
}
