// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.pkgdars

import com.daml.ledger.api.v2.admin.package_management_service.{
  UpdateVettedPackagesForceFlag,
  VettedPackagesChange,
  VettedPackagesRef,
}
import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.javaapi.data
import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Integrity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{CommandFailure, ParticipantReference}
import com.digitalasset.canton.crypto.{CryptoPureApi, SigningKeyUsage}
import com.digitalasset.canton.damltests.java.conflicttest.Many
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseProgrammableSequencer}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.pkgdars.PackageUsableMixin
import com.digitalasset.canton.integration.tests.security.SecurityTestHelpers
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.NotFound
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.admin.CantonPackageServiceError
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentDataHelpers
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.UnvettedPackages
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.ModelConformance
import com.digitalasset.canton.protocol.messages.{LocalApprove, Verdict}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignSpecificMappings
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Observation
import com.digitalasset.canton.topology.transaction.{VettedPackage, VettedPackages}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfPackageId, config}
import com.digitalasset.daml.lf.archive.{DamlLf, DarParser, DarReader}
import com.digitalasset.daml.lf.data.Ref.PackageId
import org.scalatest.Assertion

import java.io.File
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

sealed trait PackageVettingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with PackageUsableMixin
    with SecurityTestSuite
    with AccessTestScenario
    with HasCycleUtils
    with SecurityTestHelpers
    with VettingOperations {

  val ledgerIntegrity: SecurityTest =
    SecurityTest(property = Integrity, asset = "virtual shared ledger")

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  private var steve: PartyId = _

  private var maliciousP2: MaliciousParticipantNode = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag
      )
      .withSetup { implicit env =>
        import env.*

        // Increase the reconciliation interval to a large value to effectively disable the AcsCommitmentProcessor.
        // Otherwise, it could complain about mismatches, as some tests fork the ledger.
        runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .propose_update(
              synchronizer.synchronizerId,
              _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(100000)),
            )
        )

        Seq(participant1, participant2, participant3).foreach(
          _.synchronizers.connect_local(sequencer1, alias = daName)
        )
        Seq(participant1, participant2, participant4).foreach(
          _.synchronizers.connect_local(sequencer2, alias = acmeName)
        )

        participant2.dars.upload(CantonTestsPath, synchronizerId = daId)
        participant2.dars.upload(CantonTestsPath, synchronizerId = acmeId)

        // Enable steve on participant2 on both synchronizers (da+acme)
        steve = participant2.parties.enable("steve", synchronizer = Some(daName))
        participant2.parties.enable(
          "steve",
          synchronizer = Some(acmeName),
          synchronizeParticipants = Seq(participant1, participant4),
        )

        // Enable steve on participant4 on acme.
        // No need to do that an da, as participant4 is not connected to da.
        participant2.topology.party_to_participant_mappings
          .propose_delta(
            steve,
            adds = Seq(participant4.id -> Observation),
            store = acmeId,
          ) // requires auth by party
        participant4.topology.party_to_participant_mappings
          .propose_delta(
            steve,
            adds = Seq(participant4.id -> Observation),
            mustFullyAuthorize = true,
            store = acmeId,
          )

        // Verify that steve has been successfully enabled
        eventually() {
          val p2p = participant2.topology.party_to_participant_mappings
            .list(acmeId, filterParty = steve.filterString)
            .loneElement
            .item
          p2p.participants
            .map(_.participantId)
            .toSet shouldBe Set(participant2.id, participant4.id)
        }

        pureCryptoRef.set(participant1.crypto.pureCrypto)

        maliciousP2 = MaliciousParticipantNode(
          participant2,
          daId,
          testedProtocolVersion,
          defaultProtocolLimits,
          timeouts,
          loggerFactory,
        )
      }

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  "auto-vetting of dars works and doesn't block on disconnected synchronizers" taggedAs_ {
    ledgerIntegrity.setHappyCase(_)
  } in { implicit env =>
    import env.*

    def getVetted(synchronizerId: SynchronizerId) =
      vettedPackages(participant1, synchronizerId.toPhysical)

    val before = getVetted(daId)

    // disconnect acme, but keep da connected
    participant1.synchronizers.disconnect(acmeName)

    // vet CantonTests, let upload automatically select the only connected synchronizer
    participant1.dars.upload(CantonTestsPath, vetAllPackages = true, synchronizeVetting = true)

    val darPackageIds =
      DarReader.assertReadArchiveFromFile(new File(CantonTestsPath)).all.map(_.pkgId).toSet

    // check that vettings from the DAR upload were registered with the sequencer
    val newPackagesInSync = getVetted(daId) -- before
    val newPackagesToBeAddedByDar = darPackageIds.map(_.toString) -- before

    newPackagesInSync shouldBe newPackagesToBeAddedByDar

    // reconnect acme
    participant1.synchronizers.reconnect(acmeName)

    // wait until acme has observed vetting txs
    participant1.packages.synchronize_vetting()

    // check that acme does *not* receive the vetting changes that were applied
    // to the connected synchronizer
    (getVetted(acmeId) -- before) shouldBe empty

    // uploading a second time (this time specifically on each synchronizer) doesn't make me bail
    participant1.dars.upload(
      CantonTestsPath,
      vetAllPackages = true,
      synchronizeVetting = true,
      synchronizerId = daId,
    )
    participant1.dars.upload(
      CantonTestsPath,
      vetAllPackages = true,
      synchronizeVetting = true,
      synchronizerId = acmeId,
    )

  }

  private def exploitUnknownPackage(role: String): Attack = Attack(
    actor = "ledger api user",
    s"submit a command referring to a package unknown to $role",
    "reject the command",
  )

  private def assertSynchronizerDiscardedPackageNotVettedByReason(
      synchronizerId: PhysicalSynchronizerId,
      partyId: PartyId,
      packageNameWithoutVettedPackages: String,
      expectedErrorCode: ErrorCode = CommandExecutionErrors.PackageSelectionFailed,
      expectedPreambleErrorMessage: String =
        "No synchronizers satisfy the topology requirements for the submitted command",
  )(logEntry: LogEntry): Assertion = {
    logEntry.shouldBeCantonErrorCode(expectedErrorCode)
    logEntry.message should include(expectedPreambleErrorMessage)
    logEntry.message should include(
      show"$synchronizerId: Failed to select package-id for package-name '$packageNameWithoutVettedPackages' appearing in a command root node due to: No package with package-name '$packageNameWithoutVettedPackages' is consistently vetted by all hosting participants of party $partyId"
    )
  }

  "cannot submit command referring to a package that has not been vetted by my participant" taggedAs ledgerIntegrity
    .setAttack(exploitUnknownPackage("the submitting participant")) in { implicit env =>
    import env.*

    val p3p = participant3.id.adminParty
    val p2p = participant2.id.adminParty

    assertThrowsAndLogsCommandFailures(
      participant3.ledger_api.javaapi.commands.submit(
        Seq(participant3.id.adminParty),
        createIouCmds(p3p, p2p).toList.asJava.overridePackageId(M.iou.Iou.PACKAGE_ID).asScala.toSeq,
      ),
      _.shouldBeCommandFailure(NotFound.Package),
    )

  }

  "cannot submit a command referring to a package that has not been vetted by a confirming participant" taggedAs ledgerIntegrity
    .setAttack(exploitUnknownPackage("a confirming participant")) in { implicit env =>
    import env.*

    val p3p = participant3.id.adminParty
    val p2p = participant2.id.adminParty

    assertThrowsAndLogsCommandFailures(
      participant2.ledger_api.javaapi.commands.submit(
        Seq(participant2.id.adminParty),
        createIouCmds(p2p, p3p),
      ),
      assertSynchronizerDiscardedPackageNotVettedByReason(
        synchronizerId = daId,
        partyId = participant3.adminParty,
        packageNameWithoutVettedPackages = M.iou.Iou.PACKAGE_NAME,
      ),
    )
  }

  "cannot submit a command referring to a package that has not been vetted by an informee participant" taggedAs ledgerIntegrity
    .setAttack(exploitUnknownPackage("an informee participant")) in { implicit env =>
    import env.*

    participant1.dars.upload(CantonTestsPath, synchronizerId = daId)
    participant1.dars.upload(CantonTestsPath, synchronizerId = acmeId)

    val p3p = participant3.id.adminParty
    val p2p = participant2.id.adminParty
    val p1p = participant1.id.adminParty

    assertThrowsAndLogsCommandFailures(
      participant2.ledger_api.javaapi.commands.submit(
        Seq(participant2.id.adminParty),
        createIouCmds(p2p, p1p, p3p),
      ),
      assertSynchronizerDiscardedPackageNotVettedByReason(
        synchronizerId = daId,
        partyId = participant3.adminParty,
        packageNameWithoutVettedPackages = M.iou.Iou.PACKAGE_NAME,
      ),
    )
  }

  // TODO(#20873): test the interaction between package vetting and package version selection for upgrading
  "rolls back a view referring to a package that has not been vetted by an informee participant" taggedAs ledgerIntegrity
    .setAttack(
      Attack(
        "a malicious participant",
        "submits a request referring to a package unknown to an informee participant",
        "alarm and rollback the request",
      )
    ) in { implicit env =>
    import env.*

    val rawCmds =
      createIouCmds(participant2.adminParty, participant2.adminParty, participant3.adminParty)
        .map(c => Command.fromJavaProto(c.toProtoCommand))
    val cmd = CommandsWithMetadata(rawCmds, Seq(participant2.adminParty))

    val ((_, events), _) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
      replacingConfirmationResult(
        daId,
        sequencer1,
        mediator1,
        withMediatorVerdict(Verdict.Approve(testedProtocolVersion)),
      ) {
        trackingLedgerEvents(Seq(participant2), Seq.empty) {
          maliciousP2.submitCommand(cmd).futureValueUS
        }
      },
      LogEntry.assertLogSeq(
        // Use (?s) to enable java.util.regex.Pattern.DOTALL pattern matching
        mustContainWithClue = Seq(
          (
            _.shouldBeCantonError(
              MalformedRejects.ModelConformance,
              _ should include regex raw"(?s)DAMLeError.*EngineError.*MissingPackage",
            ),
            "unvetted packages error",
          ),
          (
            _.shouldBeCantonError(
              SyncServiceAlarm,
              _ shouldBe "Mediator approved a request that has been locally rejected.",
            ),
            "unexpected mediator approval",
          ),
        ),
        mayContain = Seq(
          _.loggerName should include(
            "participant=participant2"
          ) // Ignore errors from malicious P2
        ),
      ),
    )

    events.assertNoTransactions()
  }

  "rolls back a view referring to a package with ledger time outside the package validity period" taggedAs ledgerIntegrity
    .setAttack(
      Attack(
        "a malicious participant",
        "submits a request with ledger time not falling in the validity period of a referred package",
        "alarm and rollback the request",
      )
    ) in { implicit env =>
    import env.*
    val archive = tryReadDar(CantonExamplesPath)

    val iouPackage = PackageId.assertFromString(M.iou.Iou.PACKAGE_ID)
    val validityEnd = environment.clock.now

    vettingCmd(
      adds = Seq(archive.main),
      validUntil = Some(validityEnd),
      targetParticipantOtherwiseParticipant3 = Some(participant1),
    )
    eventually() {
      vettedPackages(participant1, daId) should contain(iouPackage)
    }

    val rawCmds =
      createIouCmds(participant2.adminParty, participant1.adminParty)
        .map(c => Command.fromJavaProto(c.toProtoCommand))
    val cmd = CommandsWithMetadata(
      rawCmds,
      Seq(participant2.adminParty),
      ledgerTime = validityEnd.plusMillis(1L).underlying,
    )

    def unvettedPackagesError(participant: ParticipantReference): (LogEntry => Assertion, String) =
      (
        _.shouldBeCantonError(
          MalformedRejects.ModelConformance,
          _ should include(UnvettedPackages(Map(participant1.id -> Set(iouPackage))).toString),
          loggerAssertion = _ should include(s"participant=${participant.name}"),
        ),
        s"unvetted packages error for ${participant.name}",
      )

    val (_, events) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
      trackingLedgerEvents(Seq(participant1, participant2), Seq.empty) {
        maliciousP2.submitCommand(cmd).futureValueUS
      },
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq(
          unvettedPackagesError(participant1),
          unvettedPackagesError(participant2),
        )
      ),
    )

    events.assertNoTransactions()
  }

  "fail gracefully if the underlying package is not vetted on the target synchronizer" in {
    // target state: fail gracefully, do not fork the ledger
    implicit env =>
      import env.*

      // create an iou contract on da
      val iouContract =
        IouSyntax.createIou(participant2, Some(daId))(participant2.adminParty, steve)
      val iouId = iouContract.id.contractId
      val iouInstance = participant2.testing
        .acs_search(daName, exactId = iouId, limit = PositiveInt.one)
        .loneElement

      // Let participant2 maliciously unassign the contract from da.

      val helpers = ReassignmentDataHelpers(
        contract = iouInstance,
        sourceSynchronizer = Source(daId),
        targetSynchronizer = Target(acmeId),
        pureCrypto = pureCrypto,
        targetTimestamp = Target(participant2.testing.fetch_synchronizer_time(acmeId)),
      )

      val unassignmentTree = helpers
        .fullUnassignmentTree(
          participant2.adminParty.toLf,
          participant2,
          MediatorGroupRecipient(NonNegativeInt.zero),
        )()

      logger.info("Unassigning contract from da...")

      // participant2 abstains locally because it has not vetted the package on the target
      // synchronizer, then we override that with a Local approve. A local abstain is
      // compatible with the mediator's approve, so no alarm is raised.
      val (_, events) =
        replacingConfirmationResponses(
          participant2,
          sequencer1,
          daId,
          withLocalVerdict(
            LocalApprove(testedProtocolVersion)
          ),
        ) {
          trackingLedgerEvents(Seq(participant2), Seq.empty) {
            TraceContext.withNewTraceContext("attack")(implicit traceContext =>
              maliciousP2.submitUnassignmentRequest(unassignmentTree).futureValueUS
            )
          }
        }

      val unassignment = events.unassignments(participant2).futureValue.loneElement

      participant2.testing
        .acs_search(daName, exactId = iouId, limit = PositiveInt.one) shouldBe empty

      // Let p2 assign the contract to acme.

      logger.info("Assigning contract to acme...")

      loggerFactory.assertLogs(
        participant2.ledger_api.commands.submit_assign(
          participant2.adminParty,
          unassignment.reassignmentId,
          daId,
          acmeId,
          timeout = None,
        ),
        // participant4 complains due to missing package
        _.shouldBeCantonErrorCode(ModelConformance),
      )

      // Both participants remain responsive.
      assertPingSucceeds(participant2, participant4)

      // The contract is active for participant2 (as well as any honest participant who has vetted the package), but not for participant4.
      // Hence, there is a ledger fork!
      participant2.testing
        .acs_search(acmeName, exactId = iouId, limit = PositiveInt.one)
        .loneElement
      participant4.testing
        .acs_search(acmeName, exactId = iouId, limit = PositiveInt.one) shouldBe empty
  }

  "The topology manager" when {

    val archive = tryReadDar(CantonTestsPath)
    val packId = DamlPackageStore.readPackageId(archive.main)

    "a package has not been uploaded" must {
      "refuse to vet the package" taggedAs_ { mit =>
        ledgerIntegrity.setAttack(
          Attack(
            actor = "participant operator",
            threat = "vet a missing package",
            mitigation = mit,
          )
        )
      } in { implicit env =>
        import env.*
        val currentVettedPackages = vettedPackages(participant3, daId)

        currentVettedPackages should not contain packId

        // cannot vet as package is unknown
        clue("vetting of missing packages") {
          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            vettingCmd(adds = List(archive.main)),
            forAll(_)(_.shouldBeCantonErrorCode(vetMissingPackageErrorCode)),
          )
        }
      }
    }

    val darMainPackageId = new AtomicReference[Option[String]](None)
    "upload the dar but without vetting the package" in { implicit env =>
      import env.*

      clue("upload examples") {
        darMainPackageId.set(
          Some(participant3.dars.upload(CantonTestsPath, vetAllPackages = false))
        )
      }
    }

    "all packages have been vetted" must {
      "allow us to use the package" taggedAs ledgerIntegrity.setHappyCase(
        "use a package, if it has been vetted (including all dependencies)"
      ) in { implicit env =>
        import env.*

        archive.dependencies.foreach { dep =>
          vettingCmd(adds = List(dep))
          eventually() {
            vettedPackages(participant3, daId) should contain(
              PackageId.fromString(dep.getHash).value
            )
          }
        }
        unvettedPackages(archive.dependencies) shouldBe empty

        // can vet dependencies and then main
        participant3.dars.vetting.enable(darMainPackageId.get().getOrElse(fail("Should be here")))
        unvettedPackages(archive.all) shouldBe empty

        assertPackageUsable(participant3, participant3, daId)
      }
    }

    "cannot submit command referring to a package outside of its validity period" taggedAs ledgerIntegrity
      .setAttack(exploitUnknownPackage("the submitting participant")) in { implicit env =>
      import env.*

      // vet packages with a validity end date in the past
      vettingCmd(
        adds = archive.all,
        validUntil = Some(environment.clock.now.minusSeconds(3600)),
      )
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        submitCommand(participant3, participant3, daId),
        assertSynchronizerDiscardedPackageNotVettedByReason(
          synchronizerId = daId,
          partyId = participant3.adminParty,
          packageNameWithoutVettedPackages = Many.PACKAGE_NAME,
          expectedErrorCode = InvalidPrescribedSynchronizerId,
          expectedPreambleErrorMessage =
            show"Cannot submit transaction to prescribed synchronizer `${daId.logical}`",
        ),
      )

      // vet packages with a validity start date in the future
      vettingCmd(
        adds = archive.all,
        validFrom = Some(environment.clock.now.plusSeconds(3600)),
      )
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        submitCommand(participant3, participant3, daId),
        assertSynchronizerDiscardedPackageNotVettedByReason(
          synchronizerId = daId,
          partyId = participant3.adminParty,
          packageNameWithoutVettedPackages = Many.PACKAGE_NAME,
          expectedErrorCode = InvalidPrescribedSynchronizerId,
          expectedPreambleErrorMessage =
            show"Cannot submit transaction to prescribed synchronizer `${daId.logical}`",
        ),
      )

    }

    "the usage of a package falls within the package validity period" must {
      "allow us to use the package" taggedAs ledgerIntegrity.setHappyCase(
        "use a package, if the usage falls within the package validity period"
      ) in { implicit env =>
        import env.*

        // vet packages with validity +/- 1 hour
        vettingCmd(
          adds = archive.all,
          validFrom = Some(environment.clock.now.minusSeconds(3600)),
          validUntil = Some(environment.clock.now.plusSeconds(3600)),
        )

        assertPackageUsable(participant3, participant3, daId)
      }
    }

    "package id is used by an active contract" must {
      "allow to unvet a package with active contracts" in { implicit env =>
        import env.*
        assertPackageUsable(participant3, participant3, daId)
        vettingCmd(removes = Seq(archive.main))

        // vet the package again so that we can archive the contract again for cleanup
        vettingCmd(adds = archive.all)
        archiveContract(participant3)
      }

    }

    "a package is upgrade-incompatible" must {
      val incompatArchive = tryReadDar(UpgradeTestsIncompatPath)
      val compatArchive = tryReadDar(UpgradeTestsCompatPath)

      "refuse to validate the upgrade-incompatible DAR" in { implicit env =>
        import env.*
        participant3.dars.upload(UpgradeTestsPath, vetAllPackages = true, synchronizerId = daId)
        assertThrowsAndLogsCommandFailures(
          participant3.dars.validate(UpgradeTestsIncompatPath),
          _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.Upgradeability),
        )
      }

      "refuse to vet the upgrade-incompatible package" in { implicit env =>
        import env.*
        // it can upload the upgrade-incompatible package without vetting
        participant3.dars.upload(UpgradeTestsIncompatPath, vetAllPackages = false)

        assertThrowsAndLogsCommandFailures(
          vettingCmd(adds = incompatArchive.all),
          _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.Upgradeability),
        )
      }

      "allow to vet the package with the force flag" in { implicit env =>
        vettingCmd(
          adds = incompatArchive.all,
          allowVetIncompatibleUpgrades = true,
        )
      }

      "fail to vet any other package in the same lineage" in { implicit env =>
        import env.*
        participant3.dars.upload(UpgradeTestsCompatPath, vetAllPackages = false)
        assertThrowsAndLogsCommandFailures(
          vettingCmd(adds = compatArchive.all),
          _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.Upgradeability),
        )
      }

      "allow to vet upgrade-compat package after unvetting the upgrade-incompat package" in {
        implicit env =>
          vettingCmd(removes = Seq(incompatArchive.main))
          vettingCmd(adds = compatArchive.all)
      }

      "allow to vet upgrade package that uses an upgrade-compatible dependency" in { implicit env =>
        import env.*
        val vettingMainCompatDar = tryReadDar(VettingMainCompatPath)

        // upload and vet VettingMain and VettingDep
        participant3.dars.upload(VettingMainPath, synchronizerId = daId)
        participant3.dars.upload(VettingDepCompatPath, synchronizerId = daId)

        // upload VettingMainCompat without vetting
        participant3.dars.upload(VettingMainCompatPath, vetAllPackages = false)

        // vetting VettingMainCompat should succeed
        vettingCmd(adds = Seq(vettingMainCompatDar.main))
      }

      "fail to vet upgrade package that uses an upgrade-incompatible dependency" in {
        implicit env =>
          import env.*
          val vettingDepIncompatDar = tryReadDar(VettingDepIncompatPath)
          val vettingMainIncompatDar = tryReadDar(VettingMainIncompatPath)

          // upload VettingDepIncompat and force vetting
          participant3.dars.upload(VettingDepIncompatPath, vetAllPackages = false)
          vettingCmd(
            adds = Seq(vettingDepIncompatDar.main),
            allowVetIncompatibleUpgrades = true,
          )

          // upload VettingMainIncompat without vetting
          participant3.dars.upload(VettingMainIncompatPath, vetAllPackages = false)

          // vetting VettingMainIncompat should fail
          assertThrowsAndLogsCommandFailures(
            vettingCmd(adds = Seq(vettingMainIncompatDar.main)),
            _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.Upgradeability),
          )

          // unvet VettingDepIncompat
          vettingCmd(removes = Seq(vettingDepIncompatDar.main))
      }

      "fail to vet upgrade package that substitutes one of its dependency with another" in {
        implicit env =>
          import env.*
          val vettingDepSubstitutionDar = tryReadDar(VettingDepSubstitutionPath)
          val vettingMainSubstitutionDar = tryReadDar(VettingMainSubstitutionPath)

          // upload VettingDepSubstitution and force vetting
          participant3.dars.upload(VettingDepSubstitutionPath, vetAllPackages = false)
          vettingCmd(
            adds = Seq(vettingDepSubstitutionDar.main),
            allowVetIncompatibleUpgrades = true,
          )

          // upload VettingMainSubstitution without vetting
          participant3.dars.upload(VettingMainSubstitutionPath, vetAllPackages = false)

          // vetting VettingMainSubstitution should fail
          assertThrowsAndLogsCommandFailures(
            vettingCmd(adds = Seq(vettingMainSubstitutionDar.main)),
            _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.Upgradeability),
          )
      }
    }

    "a package is vetted" must {
      "allow to unvet without any force flag" in { implicit env =>
        import env.*
        // first vet packages again.
        vettingCmd(adds = archive.all)
        eventually()(unvettedPackages(List(archive.main)) shouldBe empty)

        // unvet the package works without any force flag
        vettingCmd(removes = Seq(archive.main))
        eventually()(
          unvettedPackages(List(archive.main)) should contain theSameElementsAs Seq(
            archive.main.getHash
          )
        )

        // vet all the packages again
        vettingCmd(adds = archive.all)

        // Check package is vetted
        eventually()(unvettedPackages(List(archive.main)) shouldBe empty)

        // We don't need the force flag to disable a dar.
        participant3.dars.vetting.disable(
          darMainPackageId
            .get()
            .getOrElse(fail("DAR main package-id should have been set")),
          synchronizerId = daId,
        )
        eventually() {
          val unvetted = unvettedPackages(List(archive.main))
          unvetted should contain theSameElementsAs Seq(archive.main.getHash)
        }

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          submitCommand(participant3, participant3, daId),
          assertSynchronizerDiscardedPackageNotVettedByReason(
            synchronizerId = daId,
            partyId = participant3.adminParty,
            packageNameWithoutVettedPackages = Many.PACKAGE_NAME,
            expectedErrorCode = InvalidPrescribedSynchronizerId,
            expectedPreambleErrorMessage =
              show"Cannot submit transaction to prescribed synchronizer `${daId.logical}`",
          ),
        )
      }

      val vettingDepDar = tryReadDar(VettingDepPath)
      val vettingMainDar = tryReadDar(VettingMainPath)

      s"allow to unvet if the package is used as a dependency" in { implicit env =>
        import env.*

        // upload and vet the main dar and its dependencies
        participant3.dars.upload(VettingMainPath, vetAllPackages = true)

        vettingCmd(removes = Seq(vettingDepDar.main))
      }

      "allow to unvet if the package is used as a dependency and AllowUnvettedDependencies is used" in {
        implicit env =>
          vettingCmd(
            removes = Seq(vettingDepDar.main),
            allowUnvettedDependencies = true,
          )
      }

      s"allow to unvet while vetting a dependent package" in { implicit env =>
        // vet the dep package and unvet the main package
        vettingCmd(
          adds = Seq(vettingDepDar.main),
          removes = Seq(vettingMainDar.main),
        )

        vettingCmd(
          adds = Seq(vettingMainDar.main),
          removes = Seq(vettingDepDar.main),
        )
      }
    }

    "Refuse to issue package vetting command" must {
      "for Alien member" taggedAs_ { _ =>
        ledgerIntegrity.setAttack(
          Attack(
            actor = "sequencer operator",
            threat = "issue a package vetting without force",
            mitigation = "reject the command",
          )
        )
      } in { implicit env =>
        import env.*

        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          sequencer1.topology.vetted_packages.propose_delta(
            participant3.id,
            store = synchronizer1Id,
            adds = VettedPackage.unbounded(archive.all.map(DamlPackageStore.readPackageId)),
            // explicitly specifying the desired signing key and force flag to not trigger
            // the error NO_APPROPRIATE_SINGING_KEY_IN_STORE while automatically determining
            // a suitable signing key
            signedBy = Some(sequencer1.id.fingerprint),
            force = ForceFlags(ForceFlag.AllowUnvalidatedSigningKeys),
          ),
          forAll(_)(
            _.shouldBeCantonErrorCode(
              TopologyManagerError.DangerousCommandRequiresForce
            )
          ),
        )

        // generate a new signing key and register a namespace delegation
        val sequencer1SigningKey = sequencer1.keys.secret.generate_signing_key(
          "signing_keys_p3",
          SigningKeyUsage.NamespaceOnly,
        )

        // propose the namespace delegations
        participant3.topology.namespace_delegations.propose_delegation(
          participant3.namespace,
          sequencer1SigningKey,
          CanSignSpecificMappings(VettedPackages),
          store = synchronizer1Id,
        )

        eventually() {
          sequencer1.topology.namespace_delegations.list(
            synchronizer1Id,
            filterNamespace = participant3.namespace.filterString,
            filterTargetKey = Some(
              sequencer1SigningKey.fingerprint
            ),
          ) should not be empty
        }

        sequencer1.topology.vetted_packages.propose_delta(
          participant3.id,
          adds = VettedPackage.unbounded(archive.all.map(DamlPackageStore.readPackageId)),
          store = synchronizer1Id,
          signedBy = Some(sequencer1SigningKey.fingerprint),
          force = ForceFlags(ForceFlag.AlienMember),
        )

        // Only exit test once package vetting is observable to prevent flakes such as #24162.
        eventually() {
          unvettedPackages(archive.all) shouldBe empty
        }
      }
    }

    "the main package is unvetted and vetted again" must {
      "allow us to use the package" taggedAs ledgerIntegrity.setHappyCase(
        "submit a command referring to a package that has been vetted again"
      ) in { implicit env =>
        import env.*

        // unvet main package so that we can test that we can vet and subsequently submit a command
        vettingCmd(removes = Seq(archive.main))
        eventually() {
          unvettedPackages(archive.all) shouldBe Set(archive.main.getHash)
        }

        // and vet again
        participant3.dars.vetting.enable(darMainPackageId.get().getOrElse(fail("Should be here")))
        participant3.packages.synchronize_vetting()
        eventually() {
          unvettedPackages(archive.all) shouldBe empty
        }
        assertPackageUsable(participant3, participant3, daId)
      }
    }
  }

  private def tryReadDar(darPath: String) =
    DarParser
      .readArchiveFromFile(new java.io.File(darPath))
      .getOrElse(fail(s"cannot read DAR: $darPath"))

  private def createIouCmds(payer: PartyId, owner: PartyId, viewers: PartyId*): Seq[data.Command] =
    IouSyntax.testIou(payer, owner, observers = viewers.toList).create.commands.asScala.toSeq
}

/** Trait created to allow testing both APIs currently supported for managing/quering vetting on the
  * Canton participant
  *
  * TODO(i35849): Remove this trait and deduplicate the API testing once the vetting APIs are
  * deduplicated as well
  */
private[pkgdars] sealed trait VettingOperations {
  protected def unvettedPackages(packageList: List[DamlLf.Archive])(implicit
      env: TestConsoleEnvironment
  ): Set[PackageId]

  protected def vettingCmd(
      adds: Seq[DamlLf.Archive] = Seq.empty,
      removes: Seq[DamlLf.Archive] = Seq.empty,
      validFrom: Option[CantonTimestamp] = None,
      validUntil: Option[CantonTimestamp] = None,
      allowUnvettedDependencies: Boolean = false,
      allowVetIncompatibleUpgrades: Boolean = false,
      targetParticipantOtherwiseParticipant3: Option[ParticipantReference] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit

  protected def vettedPackages(
      participant: => ParticipantReference,
      synchronizerId: => PhysicalSynchronizerId,
  ): Set[String]

  // The error differs depending on the API the condition is triggerred from
  def vetMissingPackageErrorCode: ErrorCode
}

trait LedgerApiVettingOperations extends VettingOperations {
  this: PackageVettingIntegrationTest =>

  def unvettedPackages(packageList: List[DamlLf.Archive])(implicit
      env: TestConsoleEnvironment
  ): Set[PackageId] = {
    import env.*
    val packages = packageList.map(DamlPackageStore.readPackageId).toSet
    val vettedPkgs = participant3.ledger_api.packages
      .list_vetted_packages(synchronizerIds = Seq(daId), participantIds = Seq(participant3.id))
      .vettedPackages
      .loneElement
      .packages
      .map[LfPackageId](_.packageId)
    packages -- vettedPkgs
  }

  def vettingCmd(
      adds: Seq[DamlLf.Archive] = Seq.empty,
      removes: Seq[DamlLf.Archive] = Seq.empty,
      validFrom: Option[CantonTimestamp] = None,
      validUntil: Option[CantonTimestamp] = None,
      allowUnvettedDependencies: Boolean = false,
      allowVetIncompatibleUpgrades: Boolean = false,
      targetParticipantOtherwiseParticipant3: Option[ParticipantReference] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    val targetParticipant = targetParticipantOtherwiseParticipant3.getOrElse(participant3)

    targetParticipant.ledger_api.packages.update_vetted_packages(
      addOrUpdate = adds.map { archive =>
        val packageId = DamlPackageStore.readPackageId(archive)
        VettedPackagesChange.Vet(
          packages =
            Seq(VettedPackagesRef(packageId = packageId, packageName = "", packageVersion = "")),
          newValidFromInclusive = validFrom.map(_.toProtoTimestamp),
          newValidUntilExclusive = validUntil.map(_.toProtoTimestamp),
        )
      },
      remove = removes.map { archive =>
        val packageId = DamlPackageStore.readPackageId(archive)
        VettedPackagesRef(packageId = packageId, packageName = "", packageVersion = "")
      },
      synchronizerId = Some(daId),
      forceFlags =
        (if (allowUnvettedDependencies) Seq.empty
         else
           Seq(
             UpdateVettedPackagesForceFlag.UPDATE_VETTED_PACKAGES_FORCE_FLAG_ALLOW_UNVETTED_DEPENDENCIES
           )) ++ (
          if (allowVetIncompatibleUpgrades)
            Seq(
              UpdateVettedPackagesForceFlag.UPDATE_VETTED_PACKAGES_FORCE_FLAG_ALLOW_VET_INCOMPATIBLE_UPGRADES
            )
          else Seq.empty
        ),
    )
  }

  protected def vettedPackages(
      participant: => ParticipantReference,
      synchronizerId: => PhysicalSynchronizerId,
  ): Set[String] =
    participant.ledger_api.packages
      .list_vetted_packages(
        synchronizerIds = Seq(synchronizerId),
        participantIds = Seq(participant.id),
      )
      .vettedPackages
      .loneElement
      .packages
      .map(_.packageId)
      .toSet

  override def vetMissingPackageErrorCode: ErrorCode =
    CantonPackageServiceError.Vetting.VettingReferenceEmpty
}

trait AdminApiVettingOperations {
  this: PackageVettingIntegrationTest =>

  def unvettedPackages(packageList: List[DamlLf.Archive])(implicit
      env: TestConsoleEnvironment
  ): Set[PackageId] = {
    import env.*
    val packages = packageList.map(DamlPackageStore.readPackageId).toSet
    packages -- participant3.topology.vetted_packages
      .list(
        store = daId,
        filterParticipant = participant3.id.filterString,
      )
      .flatMap(_.item.packages.map(_.packageId))
      .toSet
  }

  def vettingCmd(
      adds: Seq[DamlLf.Archive] = Seq.empty,
      removes: Seq[DamlLf.Archive] = Seq.empty,
      validFrom: Option[CantonTimestamp] = None,
      validUntil: Option[CantonTimestamp] = None,
      allowUnvettedDependencies: Boolean = false,
      allowVetIncompatibleUpgrades: Boolean = false,
      targetParticipantOtherwiseParticipant3: Option[ParticipantReference] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    val targetParticipant = targetParticipantOtherwiseParticipant3.getOrElse(participant3)
    targetParticipant.topology.vetted_packages.propose_delta(
      targetParticipant.id,
      store = daId,
      adds = adds.map(DamlPackageStore.readPackageId).map(VettedPackage(_, validFrom, validUntil)),
      removes = removes.map(DamlPackageStore.readPackageId),
      force = ForceFlags(
        Set(ForceFlag.AllowUnvettedDependencies)
          // No need for force flag if PV supports unvetted dependencies
          .filter(_ => allowUnvettedDependencies) ++
          Set(ForceFlag.AllowVetIncompatibleUpgrades)
            .filter(_ => allowVetIncompatibleUpgrades)
      ),
    )
    // synchronize package vetting, as "raw" vetting commands are unsynced
    participant3.packages.synchronize_vetting()
  }

  protected def vettedPackages(
      participant: => ParticipantReference,
      syncId: => PhysicalSynchronizerId,
  ): Set[String] =
    participant.topology.vetted_packages
      .list(
        Some(TopologyStoreId.Synchronizer(syncId)),
        filterParticipant = participant.id.filterString,
      )
      .flatMap(_.item.packages)
      .map(_.packageId)
      .toSet

  override def vetMissingPackageErrorCode: ErrorCode =
    ParticipantTopologyManagerError.CannotVetDueToMissingPackages
}

class PackageVettingIntegrationTestInMemory_AdminApi
    extends PackageVettingIntegrationTest
    with AdminApiVettingOperations

class PackageVettingIntegrationTestInMemory_LedgerApi
    extends PackageVettingIntegrationTest
    with LedgerApiVettingOperations
