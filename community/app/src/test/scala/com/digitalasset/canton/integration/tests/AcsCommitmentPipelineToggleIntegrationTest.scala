// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.BigDecimalImplicits.DoubleToBigDecimal
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.SynchronizerTimeTrackerConfig
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.store.AcsDigestStore.allCheckpointsFilter
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ResourceUtil.withResource
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{TestPredicateFiltersFixtureAnyWordSpec, config}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

/** This test establishes that we can restart a participant with a different setting for enabling
  * the new commitment processor pipeline.
  */
abstract class AcsCommitmentPipelineToggleIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with TestPredicateFiltersFixtureAnyWordSpec {

  private val interval: java.time.Duration = java.time.Duration.ofSeconds(5)

  override protected def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(
          // Initially disable the pipeline.
          _.focus(_.parameters.acsCommitments.enableRunningDigestProcessor)
            .replace(false)
            // Emit checkpoints quickly so that the digest processor increases its checkpoint watermark
            // so that the matcher processes the incoming commitments quickly.
            .focus(_.ledgerApi.indexService.idleStreamOffsetCheckpointTimeout)
            .replace(config.NonNegativeFiniteDuration.ofSeconds(1))
        ),
      )

  // setup happens as a test case rather than `withSetup` so that we don't repeat this on the other environments we create in the test
  "setup the environment" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
    implicit env =>
      import env.*

      sequencer1.topology.synchronisation.await_idle()
      sequencer2.topology.synchronisation.await_idle()
      initializedSynchronizers foreach { case (_, initializedSynchronizer) =>
        initializedSynchronizer.synchronizerOwners.foreach(
          _.topology.synchronizer_parameters
            .propose_update(
              initializedSynchronizer.synchronizerId,
              _.update(reconciliationInterval = config.PositiveDurationSeconds(interval)),
            )
        )
      }

      // Set the observation latency to 0 such that `await_time` works in sim clock
      val daSequencerConnection =
        SequencerConnections.single(sequencer1.sequencerConnection.withAlias(daName.toString))
      participants.all.synchronizers.connect(
        SynchronizerConnectionConfig(
          synchronizerAlias = daName,
          sequencerConnections = daSequencerConnection,
          timeTracker = SynchronizerTimeTrackerConfig(
            observationLatency = config.NonNegativeFiniteDuration.Zero
          ),
        )
      )
      participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
      participants.all.foreach { p =>
        p.dars.upload(CantonExamplesPath, synchronizerId = daId)
        p.dars.upload(CantonExamplesPath, synchronizerId = acmeId)
      }
  }

  private def stopAllNodes()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    participants.all.synchronizers.disconnect_all()
    nodes.local.stop()
  }

  private def restartAllNodes()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    nodes.local.start()
    participants.all.synchronizers.reconnect_all()
  }

  private def setPipelineStatus[A](
      oldEnv: TestConsoleEnvironment,
      participantNames: Seq[String],
      enablePipeline: Boolean,
      stopNodes: Boolean = true,
  )(f: TestConsoleEnvironment => A): A = {
    if (stopNodes) stopAllNodes()(oldEnv)
    logger.info(
      s"Setting pipeline status to $enablePipeline for ${participantNames.mkString(", ")}"
    )
    val newEnv = manualCreateEnvironmentWithPreviousState(
      oldEnv.actualConfig,
      _ =>
        ConfigTransforms.applyMultiple(
          participantNames.map(p =>
            ConfigTransforms.updateParticipantConfig(p)(
              _.focus(_.parameters.acsCommitments.enableRunningDigestProcessor)
                .replace(enablePipeline)
            )
          )
        )(oldEnv.actualConfig),
    )
    withResource(newEnv) { implicit env =>
      restartAllNodes()
      f(env)
    }
  }

  private def assertDigestCheckpoint(
      participant: LocalParticipantReference,
      synchronizerId: SynchronizerId,
      empty: Boolean,
  ): Assertion = {
    val digestStore =
      participant.underlying.value.sync.syncPersistentStateManager
        .acsDigestStore(synchronizerId)
        .value

    val checkpoint =
      digestStore.latestCheckpointUpTo(Offset.MaxValue, allCheckpointsFilter).futureValueUS
    if (empty) checkpoint shouldBe None
    else checkpoint should not be None
  }

  protected def createIou(
      synchronizerId: SynchronizerId,
      submittingParticipant: ParticipantReference,
      otherParticipants: ParticipantReference*
  )(implicit
      env: TestConsoleEnvironment,
      traceContext: TraceContext,
  ): iou.Iou.Contract = {
    import env.*
    import scala.jdk.CollectionConverters.*

    val issuer = submittingParticipant.adminParty
    val observers = otherParticipants.map(_.adminParty)

    environment.simClock.value.advanceTo(environment.simClock.value.uniqueTime().immediateSuccessor)

    val owner = observers.headOption.getOrElse(issuer)
    logger.info(
      s"Creating the iou contract on participant ${submittingParticipant.name} for issuer $issuer and owner $owner"
    )

    val createIouCmds =
      new iou.Iou(
        issuer.toProtoPrimitive,
        owner.toProtoPrimitive,
        new iou.Amount(1.0.toBigDecimal, "USD"),
        observers.map(_.toProtoPrimitive).asJava,
      ).create().commands().asScala.toSeq

    val tx = submittingParticipant.ledger_api.javaapi.commands
      .submit(Seq(issuer), createIouCmds, synchronizerId)
    val contract = JavaDecodeUtil.decodeAllCreated(iou.Iou.COMPANION)(tx).loneElement

    logger.info(s"Waiting for the participants to see the contract in their ACS")
    eventually() {
      (submittingParticipant +: otherParticipants).foreach(p =>
        p.ledger_api.state.acs.of_all().filter(_.contractId == contract.id.contractId)
          should not be empty
      )
    }
    contract
  }

  private def advanceBeyondNextTick()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    logger.info(s"Advancing ${participant1.name} to the next tick")

    val simClock = environment.simClock.value
    val now = simClock.uniqueTime()
    simClock.advanceTo(now.add(interval))
    // Fetch synchronizer times from participant3 to ensure that the next block will be after `now`
    participant3.testing.fetch_synchronizer_times()
    participant1.health.ping(participant2)
  }

  private def awaitNextTick()(implicit env: TestConsoleEnvironment): CommitmentPeriod = {
    import env.*

    logger.info(s"Advancing ${participant1.name} to the next tick")

    val simClock = environment.simClock.value
    val now = simClock.uniqueTime()
    simClock.advanceTo(now.add(interval))
    // Fetch synchronizer times from participant3 to ensure that the next block will be after `now`
    participant3.testing.fetch_synchronizer_times()
    participant1.health.ping(participant2)

    val store =
      participant1.underlying.value.sync.syncPersistentStateManager.acsDigestStore(daId).value
    val checkpoint = eventually() {
      val checkpoint = store.latestReconciliationCheckpoint().futureValueUS.value
      checkpoint.recordTime should be >= now
      checkpoint
    }
    CommitmentPeriod.tryCreate(checkpoint.recordTime.immediatePredecessor, checkpoint.recordTime)
  }

  protected def checkMatchingCommitment(
      period: CommitmentPeriod,
      participant: LocalParticipantReference,
      sender: ParticipantId,
      synchronizer: SynchronizerId,
  ): Unit = {
    val store = participant.underlying.value.sync.syncPersistentStateManager
      .acsCommitmentPeriodStore(synchronizer)
      .value
    val interning =
      participant.underlying.value.sync.ledgerApiIndexer.asEval.value.ledgerApiStore.stringInterningView
    val internedSender = interning.participantId.internalize(sender.toLf)
    eventually() {
      val matches = store.lookupMatched(Seq(internedSender -> period)).futureValueUS.toSeq
      matches.size should be >= 1
    }
  }

  "happily toggle pipeline disabled/enabled" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
    disabledEnv =>
      {
        implicit val env: TestConsoleEnvironment = disabledEnv
        import env.*

        logger.info(
          "Deploying first round of contracts with pipeline disabled on both participants"
        )

        createIou(daId, participant1, participant2)
        // Make sure that the shared ACS on the second domain is non-empty so that we also produce commitments there
        createIou(acmeId, participant1, participant2)

        advanceBeyondNextTick()
        always() {
          assertDigestCheckpoint(participant1, daId, empty = true)
          assertDigestCheckpoint(participant2, daId, empty = true)
        }
      }

      setPipelineStatus(disabledEnv, Seq("participant1", "participant2"), enablePipeline = true) {
        implicit enabledEnv =>
          import enabledEnv.*
          // Advance the clock so that we do not overlap with the previous environment's timestamps
          environment.simClock.value.advanceTo(CantonTimestamp.ofEpochSecond(60))
          // Ensure that the block sequencers observe time advancing
          participant3.testing.fetch_synchronizer_times()

          logger.info(
            s"Deploying second round of contracts with pipeline enabled on both participants."
          )

          // Sanity-check that the config really was applied
          participant1.underlying.value.config.parameters.acsCommitments.enableRunningDigestProcessor shouldBe true
          participant2.underlying.value.config.parameters.acsCommitments.enableRunningDigestProcessor shouldBe true

          createIou(daId, participant1, participant2)

          val period = awaitNextTick()
          checkMatchingCommitment(period, participant1, participant2, daId)
          checkMatchingCommitment(period, participant2, participant1, daId)

          stopAllNodes()
      }

      {
        implicit val env: TestConsoleEnvironment = disabledEnv
        import env.*
        restartAllNodes()

        // Advance the clock so that we do not overlap with the previous environment's timestamps
        // Use absolute timestamps because the different environments have unsynchronized sim clocks
        environment.simClock.value.advanceTo(CantonTimestamp.ofEpochSecond(120))
        // Ensure that the block sequencers observe time advancing
        participant3.testing.fetch_synchronizer_times()

        logger.info(
          "Deploying third round of contracts with the pipeline disabled on both participants"
        )
        createIou(daId, participant1, participant2)

        advanceBeyondNextTick()
        assertDigestCheckpoint(participant1, daId, empty = true)
        assertDigestCheckpoint(participant2, daId, empty = true)
      }

      setPipelineStatus(disabledEnv, Seq("participant1"), enablePipeline = true) {
        implicit enabledP1Env =>
          import enabledP1Env.*

          // Advance the clock so that we do not overlap with the previous environment's timestamps
          environment.simClock.value.advanceTo(CantonTimestamp.ofEpochSecond(180))
          // Ensure that the block sequencers observe time advancing
          participant3.testing.fetch_synchronizer_times()

          logger.info(
            "Deploying fourth round of contract with the pipeline enabled only on participant1"
          )
          createIou(daId, participant1, participant2)

          advanceBeyondNextTick()
          assertDigestCheckpoint(participant1, daId, empty = false)
          assertDigestCheckpoint(participant2, daId, empty = true)
      }

      setPipelineStatus(
        disabledEnv,
        Seq("participant1", "participant2"),
        enablePipeline = true,
        stopNodes = false,
      ) { implicit enabledEnv =>
        import enabledEnv.*

        // Advance the clock so that we do not overlap with the previous environment's timestamps
        environment.simClock.value.advanceTo(CantonTimestamp.ofEpochSecond(240))
        // Ensure that the block sequencers observe time advancing
        participant3.testing.fetch_synchronizer_times()

        logger.info(
          "Deploying fifth round of contracts with the pipeline enabled only both participants"
        )
        createIou(daId, participant1, participant2)

        val period = awaitNextTick()
        assertDigestCheckpoint(participant1, daId, empty = false)
        assertDigestCheckpoint(participant2, daId, empty = false)

        checkMatchingCommitment(period, participant1, participant2, daId)
        checkMatchingCommitment(period, participant2, participant1, daId)
      }
  }
}

@AcsCommitmentTest
class AcsCommitmentPipelineIntegrationTestPostgres
    extends AcsCommitmentPipelineToggleIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
}
