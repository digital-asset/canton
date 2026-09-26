// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import cats.data.EitherT
import com.digitalasset.canton.admin.api.client.commands.{GrpcAdminCommand, TopologyAdminCommands}
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.crypto.SigningKeyUsage.Namespace
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.Fixture
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.topology.SequencerConnectionSuccessorListener
import com.digitalasset.canton.synchronizer.sequencer.config.LsuSequencingBoundsOverride
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  GrpcConnection,
  LsuAnnouncement,
  LsuSequencerConnectionSuccessor,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{ForceFlag, OpaquePhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.nonempty.NonEmpty
import io.scalaland.chimney.dsl.*
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

/** The goal is to ensure that a PN can survive an LSU announcement with a PV that it doesn't
  * support.
  *
  * Test setup:
  *   - an LSU and a sequencer successor are announced by manually crafting the topology txs
  *   - we assert the warnings and errors in the PN
  */
@nowarn("msg=dead code")
final class LsuAnnounceUnsupportedPvIntegrationTest extends LsuBase {
  override protected def testName: String = "lsu-announce-unsupported-pv"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")
  override protected lazy val newOldMediators: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")

  private var synchronizerId: SynchronizerId = _
  private var managingNode: LocalInstanceReference = _

  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override protected def configTransforms: List[ConfigTransform] = {
    val allNewNodes = Set("sequencer2", "mediator2")

    List(
      ConfigTransforms.disableAutoInit(allNewNodes),
      ConfigTransforms.useStaticTime,
    ) ++ ConfigTransforms.enableDevVersionSupport
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 1,
        numSequencers = 2,
        numMediators = 2,
      )
      /*
      The test is made slightly more robust by controlling explicitly which nodes are running.
      This allows to ensure that correct synchronizer nodes are used for each LSU.
       */
      .withManualStart
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(S1M1.copy(synchronizerOwners = Seq(sequencer1)))
      }
      .addConfigTransforms(configTransforms*)
      // Change below is required for the roll forward
      .addConfigTransform(ConfigTransforms.updateSequencerConfig("sequencer2") {
        _.focus(_.parameters.lsuRepair.lsuSequencingBoundsOverride).replace(
          Some(
            LsuSequencingBoundsOverride(
              lowerBoundSequencingTimeExclusive = upgradeTime,
              upgradeTime = upgradeTime,
            )
          )
        )
      })
      .withSetup { implicit env =>
        import env.*

        synchronizerId = sequencer1.synchronizer_id
        managingNode = sequencer1

        participants.local.start()

        participants.all.synchronizers.connect(defaultSynchronizerConnectionConfig())

        participants.all.dars.upload(CantonExamplesPath)
        participant1.health.ping(participant1)

        setDefaultsDynamicSynchronizerParameters(daId, synchronizerOwners1)
      }

  /** Check whether an LSU is ongoing
    * @param successor
    *   Defined iff an upgrade is ongoing
    */
  private def checkLsuOngoing(
      successor: Option[SynchronizerSuccessor]
  )(implicit env: TestConsoleEnvironment) = {
    import env.*

    val connectedSynchronizer = participant1.underlying.value.sync
      .connectedSynchronizerForAlias(daName)
      .value

    connectedSynchronizer.ephemeral.recordOrderPublisher.getSynchronizerSuccessor shouldBe successor

    connectedSynchronizer.synchronizerCrypto.currentSnapshotApproximation.futureValueUS.ipsSnapshot
      .announcedLsu()
      .futureValueUS
      .map { case (successor, _) => successor } shouldBe successor
  }

  private def loadTransactions(env: TestConsoleEnvironment)(
      txs: Seq[GenericSignedTopologyTransaction],
      synchronize: Option[NonNegativeDuration] = Some(
        env.commandTimeouts.unbounded
      ),
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = runAdminCommand(
    TopologyAdminCommands.Write
      .AddTransactions(
        txs,
        store = synchronizerId,
        ForceFlag.AlienMember,
        synchronize,
      )
  )

  private def runAdminCommand[Result](
      command: GrpcAdminCommand[?, ?, Result]
  )(implicit traceContext: TraceContext, ec: ExecutionContext) =
    managingNode.consoleEnvironment.grpcAdminCommandRunner
      .runCommandAsync(
        managingNode.name,
        command,
        managingNode.config.clientAdminApi,
        managingNode.config.adminApi.adminTokenConfig.fixedAdminToken,
      )
      ._2
      .mapK(FutureUnlessShutdown.outcomeK)

  "LSU with an unsupported protocol version" should {
    "not break the participant or the upgrade" in { implicit env =>
      import env.*

      val currentPsid = sequencer1.physical_synchronizer_id

      val lsuAnnouncement = TopologyTransaction.tryCreate(
        op = TopologyChangeOp.Replace,
        mapping = LsuAnnouncement(
          currentPsid.opaque.copy(protocolVersionNumber = 999, serial = NonNegativeInt.one),
          upgradeTime = upgradeTime,
        ),
        serial = PositiveInt.one,
        protocolVersion = testedProtocolVersion,
      )

      val signingKeys = sequencer1.crypto.cryptoPublicStore.signingKeys.futureValueUS
        .filter(_.usage.contains(Namespace))
        .map(_.fingerprint)

      val signedLsuAnnouncementTx = clue("create and sign an lsu announcement") {
        SignedTopologyTransaction
          .signAndCreate(
            transaction = lsuAnnouncement,
            signingKeys = NonEmpty.from(signingKeys).value,
            isProposal = false,
            crypto = sequencer1.crypto.privateCrypto,
            protocolVersion = testedProtocolVersion,
            multiHash = None,
          )
          .futureValueUS
          .value
      }

      val expectedMessage = OpaquePhysicalSynchronizerId.unparseablePSIdMessage(
        SynchronizerSuccessor(
          psid = lsuAnnouncement.mapping.successorSynchronizerId,
          upgradeTime = lsuAnnouncement.mapping.upgradeTime,
        ),
        "",
      )

      def announceLsu(): Unit = {
        clue("submit the lsu announcement") {
          loadTransactions(env)(Seq(signedLsuAnnouncementTx)).value.futureValueUS shouldBe Right(())
        }

        clue("observe the announcement") {
          eventually() {
            val announcements = sequencer1.topology.lsu.announcement.list()
            announcements should not be empty
            announcements.head.item shouldBe lsuAnnouncement.mapping
          }
        }
      }

      clue("logs a warning about upgrading") {
        loggerFactory.assertEventuallyLogsSeq(
          SuppressionRule.Level(Level.WARN) && SuppressionRule.forLogger[RecordOrderPublisher]
        )(
          announceLsu(),
          forAtLeast(1, _)(
            _.warningMessage should (include(expectedMessage))
          ),
        )
      }

      clue("check that the LSU is ongoing") {
        checkLsuOngoing(
          Some(
            SynchronizerSuccessor(
              psid = lsuAnnouncement.mapping.successorSynchronizerId,
              upgradeTime = lsuAnnouncement.mapping.upgradeTime,
            )
          )
        )
      }

      val sequencerSuccessor = TopologyTransaction.tryCreate(
        op = TopologyChangeOp.Replace,
        mapping = LsuSequencerConnectionSuccessor(
          sequencerId = sequencer1.id,
          successorPsid = lsuAnnouncement.mapping.successorSynchronizerId,
          connection = GrpcConnection(
            NonEmpty.mk(Set, Endpoint("vegan-schnitzel", Port.tryCreate(999))),
            transportSecurity = false,
            customTrustCertificates = None,
          ),
        ),
        serial = PositiveInt.one,
        protocolVersion = testedProtocolVersion,
      )
      val signedSequencerSuccessorTx = clue("create and sign sequencer successor") {
        SignedTopologyTransaction
          .signAndCreate(
            transaction = sequencerSuccessor,
            signingKeys = NonEmpty.from(signingKeys).value,
            isProposal = false,
            crypto = sequencer1.crypto.privateCrypto,
            protocolVersion = testedProtocolVersion,
            multiHash = None,
          )
          .futureValueUS
          .value
      }

      def announceSequencerSuccessor(): Unit = {
        clue("submit the sequencer successor") {
          loadTransactions(env)(Seq(signedSequencerSuccessorTx)).value.futureValueUS shouldBe Right(
            ()
          )
        }

        clue("observe the sequencer successor") {
          eventually() {
            val successors = participant1.topology.lsu.sequencer_successors.list()
            successors should not be empty
            successors.head.item shouldBe sequencerSuccessor.mapping
          }
        }
      }

      clue("should warn about outdated Canton binary on observing a sequencer successor") {
        loggerFactory.assertEventuallyLogsSeq(
          SuppressionRule.Level(Level.WARN) && SuppressionRule
            .forLogger[SequencerConnectionSuccessorListener]
        )(
          announceSequencerSuccessor(),
          forAtLeast(1, _)(
            _.warningMessage should (include(expectedMessage))
          ),
        )
      }

      clue("on PN restart should retry the successor config creation and warn again") {
        loggerFactory.assertEventuallyLogsSeq(
          SuppressionRule.Level(Level.WARN) && (SuppressionRule
            .forLogger[SequencerConnectionSuccessorListener] || SuppressionRule
            .forLogger[RecordOrderPublisher]
            || SuppressionRule.forLogger[CantonSyncService])
        )(
          {
            participant1.stop()
            participant1.start()
            participant1.synchronizers.reconnect_all()
          },
          forAtLeast(4, _)(
            _.warningMessage should (include(expectedMessage))
          ),
        )
      }

      val clock = environment.simClock.value
      clue("participant1 observes the upgrade time") {
        loggerFactory.assertEventuallyLogsSeq(
          SuppressionRule.Level(Level.ERROR) && SuppressionRule.forLogger[RecordOrderPublisher]
        )(
          clock.advanceTo(upgradeTime.immediateSuccessor),
          forAtLeast(1, _)(
            _.errorMessage should (include(expectedMessage))
          ),
        )
      }
    }

    "succeed to recover with a roll forward" in { implicit env =>
      import env.*
      val currentPsid = sequencer1.physical_synchronizer_id
      val fixture = Fixture(
        currentPsid = currentPsid,
        upgradeTime = null, // it should not be used
        // read topology state from S1
        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1)),
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer2), Seq(mediator2)),
        newOldNodesResolution = Map(
          "sequencer2" -> "sequencer1",
          "mediator2" -> "mediator1",
        ),
        oldSynchronizerOwners = null, // it should not be used
        newPV = testedProtocolVersion, // potentially a downgrade
        newSerial = NonNegativeInt.two,
      )
      sequencer2.start()
      mediator2.start()

      migrateSynchronizerNodes(fixture, ignorePsidCheck = true)
      eventually() {
        val ts = sequencer2.underlying.value.sequencer.sequencer.sequencingTime.futureValueUS.value
        ts should be >= upgradeTime
      }
      transferTraffic(Some(fixture), trafficTsOverride = Some(upgradeTime))

      participant1.synchronizers.perform_manual_lsu(
        currentPsid = fixture.currentPsid,
        successorPsid = fixture.newPsid,
        upgradeTime = Some(upgradeTime),
        sequencerSuccessors =
          Map(sequencer1.id -> sequencer2.sequencerConnection.transformInto[GrpcConnection]),
      )

      participant1.synchronizers.is_connected(fixture.newPsid) shouldBe true
      participant1.health.ping(participant1)
    }
  }
}
