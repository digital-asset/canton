// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.pkgdars

import com.daml.ledger.javaapi.data.Command
import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.{Availability, Integrity}
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.console.{
  LocalParticipantReference,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.damltests.java.conflicttest.Many
import com.digitalasset.canton.damltests.java.iou
import com.digitalasset.canton.damltests.java.iou.Amount
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.pkgdars.PackageUsableMixin
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule.LevelAndAbove
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.{
  CannotRemoveAdminWorkflowPackage,
  CannotRemoveOnlyDarForPackage,
  MainPackageInUse,
  PackageInUse,
  PackagesVetted,
}
import com.digitalasset.canton.participant.admin.PackageService.{DarDescription, DarMainPackageId}
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal.ping.Ping
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.performance.model.java.dvp.asset.Asset
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError.CannotVetDueToMissingPackages
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfPackageId, SynchronizerAlias}
import com.digitalasset.daml.lf.archive.DarParser
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.value.Value.ContractId
import org.scalatest.*
import org.slf4j.event.Level

import java.io.File
import scala.jdk.CollectionConverters.*

sealed trait PackageRemovalIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with PackageUsableMixin
    with SecurityTestSuite
    with AccessTestScenario
    with LoneElement {

  val ledgerIntegrity: SecurityTest =
    SecurityTest(property = Integrity, asset = "virtual shared ledger")

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1

  // Note that CantonTests depends on CantonExamples
  private val cantonTestsPkg = PackageId.assertFromString(Many.PACKAGE_ID)
  private val cantonExamplesPkg = PackageId.assertFromString(Iou.PACKAGE_ID)
  private val adminWorkflowPkg = PackageId.assertFromString(Ping.PACKAGE_ID)
  private val performanceTestPkg = PackageId.assertFromString(Asset.PACKAGE_ID)

  def vettedPackages(participant: LocalParticipantReference)(implicit
      env: TestConsoleEnvironment
  ): Set[LfPackageId] =
    participant.topology.vetted_packages
      .list(
        store = env.daId,
        filterParticipant = participant.id.filterString,
      )
      .flatMap(_.item.packages)
      .map(_.packageId)
      .toSet

  "A participant operator" can {

    def mainPkgUnvetted()(implicit env: TestConsoleEnvironment): Unit = {
      import env.*
      eventually() {
        val pkgs = vettedPackages(participant1)
        pkgs should not contain cantonTestsPkg
      }
      // Make sure we have a clean state
      participant1.packages.synchronize_vetting()
    }

    "remove dars" should {

      "prevent removal of the admin workflow DAR" in { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        val dars = participant1.dars.list()
        dars should have length 1
        val adminWorkflowDar = dars.headOption.value

        assertThrowsAndLogsCommandFailures(
          participant1.dars.remove(adminWorkflowDar.mainPackageId),
          entry =>
            entry.shouldBeCommandFailure(
              com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.code,
              new CannotRemoveAdminWorkflowPackage(adminWorkflowPkg).cause,
            ),
        )
      }

      "succeed with the basic case" in { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)

        val initialVetting = vettedPackages(participant1)

        def checkVettingState(vetted: Boolean): Assertion = {
          val vettedPkgs = vettedPackages(participant1) -- initialVetting
          if (vetted) {
            vettedPkgs should contain(cantonTestsPkg)
          } else {
            vettedPkgs should have size 0
          }
        }

        mainPkgUnvetted()

        participant1.dars.upload(CantonTestsPath)

        // The package must be vetted immediately, due to synchronization within upload.
        checkVettingState(true)

        val dars1 = participant1.dars.list().map(_.mainPackageId).toList
        dars1 should contain(cantonTestsPkg)

        participant1.dars.remove(cantonTestsPkg)

        // The package must be unvetted immediately. In fact, packages need to be unvetted before removing packages from the store.
        checkVettingState(false)

        val dars2 = participant1.dars.list().map(_.mainPackageId).toList
        dars2 should not(contain(cantonTestsPkg))

        val pkgs = participant1.packages.list().map(_.packageId)
        pkgs should not(contain(cantonTestsPkg))
      }

      "check the main package of the dar is unused" in { implicit env =>
        import env.*

        mainPkgUnvetted()

        val (darHex, darDescriptor) = setUp(participant1, sequencer1, daName, daId)

        val cid = activeContractsForPackage(participant1, daName, cantonTestsPkg).loneElement

        assertThrowsAndLogsCommandFailures(
          participant1.dars.remove(darHex),
          entry =>
            entry.shouldBeCommandFailure(
              com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.code,
              new MainPackageInUse(cantonTestsPkg, darDescriptor, cid, daId).cause,
            ),
        )

        archiveContract(participant1, cid)()

        participant1.dars.remove(darHex)

        logger.info(s"DAR should be removed")
        val dars2 = participant1.dars.list().map(_.mainPackageId).toList
        dars2 should (not(contain(darHex)))

        logger.info(s"Main package should be removed")
        val pkgs = participant1.packages.list().map(_.packageId)
        pkgs should not contain cantonTestsPkg
      }

      "check the other packages of the DAR can be found elsewhere" in { implicit env =>
        import env.*

        mainPkgUnvetted()

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        // Check the connection works
        participant1.health.ping(participant1)

        val darHash = participant1.dars.upload(CantonTestsPath)
        lazy val darDescriptor = descriptorForHexHash(participant1, darHash)

        val cmd = new iou.Iou(
          participant1.adminParty.toProtoPrimitive,
          participant1.adminParty.toProtoPrimitive,
          new Amount(1.toBigDecimal, "USD"),
          List.empty.asJava,
        ).create.commands.asScala.toSeq

        participant1.ledger_api.javaapi.commands.submit(Seq(participant1.adminParty), cmd)

        assertThrowsAndLogsCommandFailures(
          participant1.dars.remove(darHash),
          entry =>
            entry.shouldBeCommandFailure(
              com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.code,
              new CannotRemoveOnlyDarForPackage(cantonExamplesPkg, darDescriptor).cause,
            ),
        )

        participant1.dars.upload(CantonExamplesPath)

        participant1.dars.remove(darHash)

        val dars2 = participant1.dars.list().map(_.mainPackageId).toList
        dars2 should (not(contain(darHash)))

        val pkgs = participant1.packages.list().map(_.packageId)
        pkgs should not contain cantonTestsPkg

      }
    }

    "remove packages only once unused and unvetted" taggedAs ledgerIntegrity.setAttack(
      Attack(
        actor = "a Canton user",
        threat = "create a ledger fork by removing packages",
        mitigation = "prevent removal of vetted packages",
      )
    ) in { implicit env =>
      import env.*

      mainPkgUnvetted()

      setUp(participant1, sequencer1, daName, daId)

      checkPackageOnlyRemovedWhenUnusedUnvetted(participant1, cantonTestsPkg, daId)()
    }

    "prevent removal of the ping package" in { implicit env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      assertThrowsAndLogsCommandFailures(
        participant1.packages.remove(adminWorkflowPkg),
        entry =>
          entry.shouldBeCommandFailure(
            com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.code,
            new CannotRemoveAdminWorkflowPackage(adminWorkflowPkg).cause,
          ),
      )
    }

    "remove packages only once unused on disconnected synchronizers" taggedAs ledgerIntegrity
      .setAttack(
        Attack(
          actor = "a Canton user",
          threat = "create a ledger fork by removing packages",
          mitigation = "prevent removal of packages used on disconnected synchronizers",
        )
      ) in { implicit env =>
      import env.*
      val darPath: String = CantonTestsPath

      setUp(participant1, sequencer1, daName, daId)

      val cid = activeContractsForPackage(participant1, daName, cantonTestsPkg).loneElement

      logger.info(s"Cannot remove package that is used and vetted")
      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        new PackageInUse(cantonTestsPkg, cid, daId).cause,
      )

      participant1.synchronizers.disconnect_local(daName)

      logger.info(
        s"Cannot remove package that is used and vetted, when disconnected from synchronizer"
      )
      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        new PackageInUse(cantonTestsPkg, cid, daId).cause,
      )

      participant1.packages.list().map(_.packageId) should contain(cantonTestsPkg)

      participant1.stop()
      participant1.start()

      logger.info(
        s"Cannot remove package that is used and vetted, after a restart, when disconnected"
      )
      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        new PackageInUse(cantonTestsPkg, cid, daId).cause,
      )

      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      logger.info(
        s"Cannot remove package that is used and vetted, after a restart, when reconnected"
      )
      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        new PackageInUse(cantonTestsPkg, cid, daId).cause,
      )

      archiveContract(participant1, cid)()

      logger.info(s"Cannot remove package that is vetted")
      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        PackagesVetted(cantonTestsPkg, daId).cause,
      )

      removeVetting(participant1, daId, darPath)

      logger.info(s"Can remove package that is unused and unvetted")
      participant1.packages.remove(cantonTestsPkg)
    }

    "remove packages only once unvetted on disconnected synchronizers" taggedAs ledgerIntegrity
      .setAttack(
        Attack(
          actor = "a Canton user",
          threat = "create a ledger fork by removing packages",
          mitigation = "prevent removal of packages vetted on disconnected synchronizers",
        )
      ) in { implicit env =>
      import env.*
      val darPath: String = CantonTestsPath

      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      participant1.dars.upload(CantonTestsPath)

      logger.info(s"Cannot remove package that is vetted")
      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        PackagesVetted(cantonTestsPkg, daId).cause,
      )

      participant1.synchronizers.disconnect_local(daName)

      logger.info(s"Cannot remove package that is vetted, when disconnected from synchronizer")
      packageRemovalFails(participant1, cantonTestsPkg, PackagesVetted(cantonTestsPkg, daId).cause)

      participant1.packages.list().map(_.packageId) should contain(cantonTestsPkg)

      participant1.stop()
      participant1.start()

      logger.info(
        s"Cannot remove package that is vetted, after a restart, when disconnected"
      )
      packageRemovalFails(participant1, cantonTestsPkg, PackagesVetted(cantonTestsPkg, daId).cause)

      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      logger.info(
        s"Cannot remove package that is vetted, after a restart, when reconnected"
      )
      packageRemovalFails(participant1, cantonTestsPkg, PackagesVetted(cantonTestsPkg, daId).cause)

      removeVetting(participant1, daId, darPath)

      logger.info(s"Can remove package that is unused and unvetted")
      participant1.packages.remove(cantonTestsPkg)
    }

    "remove a package immediately with the force flag" taggedAs_ {
      ledgerIntegrity.setHappyCase(_)
    } in { implicit env =>
      import env.*

      setUp(participant1, sequencer1, daName, daId)

      participant1.packages.list().map(_.packageId) should contain(cantonTestsPkg)

      val cid = activeContractsForPackage(participant1, daName, cantonTestsPkg).loneElement
      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        new PackageInUse(cantonTestsPkg, cid, daId).cause,
      )

      participant1.packages.remove(cantonTestsPkg, force = true)

      participant1.packages.list().map(_.packageId) should not contain cantonTestsPkg
    }

    "crash if a vetted package has been removed with force" in { implicit env =>
      import env.*

      setUp(participant1, sequencer1, daName, daId)
      setUp(participant2, sequencer1, daName, daId)

      assertPackageUsable(participant1, participant2, daId)

      participant2.packages.remove(cantonTestsPkg, force = true)

      // Restart participant2 to also remove the package from in-memory caches
      participant2.stop()
      participant2.start()
      participant2.synchronizers.reconnect_all()

      // This should crash p2
      loggerFactory.assertLogsSeq(LevelAndAbove(Level.WARN))(
        {
          submitCommand(participant1, participant2, daId, optTimeout = None)
          eventually() {
            participant2.synchronizers
              .list_connected()
              .map(_.physicalSynchronizerId) should not contain daId
          }
        },
        inside(_) { case head :: _tail =>
          val exceptionMessage =
            s"""Unable to load package $cantonTestsPkg even though the package has been vetted: VettedPackage(packageId = ${cantonTestsPkg.show}, unbounded). Crashing...
                 |To recover from this error upload the package with id $cantonTestsPkg to the participant and then reconnect the participant to the synchronizer.""".stripMargin

          head.throwable.value.getMessage shouldBe exceptionMessage
          head.errorMessage shouldBe "An internal error has occurred."
        },
      )
    }

    "recover by uploading the removed package again" in { implicit env =>
      import env.*

      participant2.dars.upload(CantonTestsPath, vetAllPackages = false)
      participant2.synchronizers.reconnect_all()
      assertPackageUsable(participant1, participant2, daId)
    }

    "not be able to vet a removed package" in { implicit env =>
      import env.*

      setUp(participant1, sequencer1, daName, daId)
      archiveContracts(participant1, daName)

      val p1VettedPackages = participant1.topology.vetted_packages
        .list(Some(daId), filterParticipant = participant1.filterString)
        .loneElement
        .item
        .packages

      // Unvet all packages for p1
      participant1.topology.vetted_packages.propose(participant1, Seq.empty, store = daId)
      participant1.packages.synchronize_vetting()

      // Remove the package at p1
      participant1.packages.remove(cantonTestsPkg)

      // Unable to vet packages again at p1
      assertThrowsAndLogsCommandFailures(
        participant1.topology.vetted_packages.propose(participant1, p1VettedPackages, store = daId),
        _.shouldBeCommandFailure(
          CannotVetDueToMissingPackages,
          "Package vetting failed due to packages not existing on the local node",
        ),
      )
    }

    "unvet main package before removing a DAR" in { implicit env =>
      import env.*

      setUp(participant1, sequencer1, daName, daId)
      archiveContracts(participant1, daName)

      participant1.dars.remove(cantonTestsPkg, synchronizeVetting = false)

      // Check that the package is unvetted immediately after dar removal
      // Firstly, check the current snapshot approximation:
      val snapshot = participant1.underlying.value.sync.syncCrypto.ips
        .tryForSynchronizer(daId)
        .currentSnapshotApproximation
        .futureValueUS
      snapshot
        .vettedPackages(participant1)
        .futureValueUS
        .map(_.packageId) should not contain cantonTestsPkg

      // Secondly, check the topology store.
      vettedPackages(participant1) should not contain cantonTestsPkg
    }

    "unvet main package on disconnected synchronizers before removing a DAR" in { implicit env =>
      import env.*

      setUp(participant1, sequencer1, daName, daId)
      archiveContracts(participant1, daName)

      participant1.synchronizers.disconnect_all()

      assertThrowsAndLogsCommandFailures(
        participant1.dars.remove(cantonTestsPkg),
        _.shouldBeCommandFailure(
          PackageRemovalErrorCode,
          show"Package $cantonTestsPkg is currently vetted on the following synchronizers: $daId",
        ),
      )
      participant1.dars.list().map(_.mainPackageId) should contain(cantonTestsPkg)

      // Now check that the dar can be removed, if the participant is connected.
      participant1.synchronizers.reconnect_all()
      participant1.dars.remove(cantonTestsPkg)

      vettedPackages(participant1) should not contain cantonTestsPkg
    }

    "unvet dependent packages before removing a DAR" in { implicit env =>
      import env.*

      // Using participant2 for this test, as participant1 already has the CantonExamples DAR uploaded,
      // which would confuse this test.
      setUp(participant2, sequencer1, daName, daId)
      archiveContracts(participant2, daName)

      val dar = participant2.dars.get_contents(cantonTestsPkg)
      val allPackageIds = dar.packages.map(_.packageId).map(PackageId.assertFromString).toSet
      val dependencies = allPackageIds - cantonTestsPkg

      // Unvet the main package to try bypass automatic unvetting on removal.
      // Vet all dependent packages to test if they get unvetted prior to DAR removal.
      participant2.topology.vetted_packages.propose_delta(
        participant2,
        adds = dependencies.map(VettedPackage(_, None, None)).toSeq,
        removes = Seq(cantonTestsPkg),
        store = daId,
      )

      // This should:
      // - remove pkg as well as all dependent packages that are not referenced by some other DAR.
      // - unvet packages prior to removal
      participant2.dars.remove(cantonTestsPkg)

      val packagesInStore = participant2.packages.list().map(_.packageId).toSet
      // Check if any dependency has been removed. If not, this test would succeed vacuously.
      val dependenciesRemovedFromStore = dependencies.filterNot(packagesInStore)
      dependenciesRemovedFromStore should not be empty

      // Check that all vetted packages are also in the store.
      vettedPackages(participant2).filterNot(packagesInStore) shouldBe empty
    }

    "remove packages that do not exist" taggedAs_ {
      ledgerIntegrity.setHappyCase(_)
    } in { implicit env =>
      import env.*
      setUp(participant1, sequencer1, daName, daId)

      participant1.packages.remove("this-is-not-a-real-package-id")
    }

    "disable a package twice" taggedAs_ {
      ledgerIntegrity.setHappyCase(_)
    } in { implicit env =>
      import env.*
      setUp(participant1, sequencer1, daName, daId)

      checkPackageOnlyRemovedWhenUnusedUnvetted(participant1, cantonTestsPkg, daId)()
      logger.info(s"Remove the package a second time")
      participant1.packages.remove(cantonTestsPkg)
    }

    "not crash a participant by passing an invalid package id" taggedAs SecurityTest(
      property = Availability,
      asset = "participant node",
      attack = Attack(
        actor = "a participant operator",
        threat = "crash the participant by passing an invalid package id",
        mitigation = "reject the request",
      ),
    ) in { implicit env =>
      import env.*
      setUp(participant1, sequencer1, daName, daId)

      assertThrowsAndLogsCommandFailures(
        participant1.packages.remove("invalid-package-id-\uD83C\uDF3C\uD83C\uDF38❀✿\uD83C\uDF37"),
        entry => entry.commandFailureMessage should include("INVALID_ARGUMENT"),
      )
    }

    "remove only specific packages" taggedAs_ {
      ledgerIntegrity.setHappyCase(_)
    } in { implicit env =>
      import env.*

      setUp(participant1, sequencer1, daName, daId)
      uploadDarAndVerifyVettingOnSynchronizer(
        participant1,
        PerformanceTestPath,
        performanceTestPkg,
        daId,
      )

      val adminParty = participant1.id.adminParty

      val cmd = new Asset(
        adminParty.toProtoPrimitive,
        adminParty.toProtoPrimitive,
        "PackageRemovalTestAsset",
        "",
        adminParty.toProtoPrimitive,
        adminParty.toProtoPrimitive,
      ).create.commands.asScala.toSeq

      participant1.ledger_api.javaapi.commands.submit(
        Seq(adminParty),
        cmd,
        Some(daId),
      )

      checkPackageOnlyRemovedWhenUnusedUnvetted(participant1, cantonTestsPkg, daId)()

      checkPackageOnlyRemovedWhenUnusedUnvetted(
        participant1,
        performanceTestPkg,
        daId,
        darPath = PerformanceTestPath,
      )(cid =>
        new Asset.ContractId(cid)
          .exerciseArchive()
          .commands
          .loneElement
      )
    }

    "remove packages only once unused on all synchronizers" taggedAs SecurityTest(
      property = Integrity,
      asset = "participant node",
      attack = Attack(
        actor = "participant operator",
        threat =
          "corrupt the participant state by removing a package referred by an active contract",
        mitigation = "reject the request for removing the package",
      ),
    ) in { implicit env =>
      import env.*
      setUp(participant1, sequencer1, daName, daId)
      setUp(participant1, sequencer2, acmeName, acmeId)

      logger.info(s"Cannot remove package that is used and vetted")

      val cidDa = activeContractsForPackage(participant1, daName, cantonTestsPkg).loneElement
      val cidAcme = activeContractsForPackage(participant1, acmeName, cantonTestsPkg).loneElement

      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        new PackageInUse(cantonTestsPkg, cidDa, daId).cause,
      )

      archiveContract(participant1, cidDa)()

      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        new PackageInUse(cantonTestsPkg, cidAcme, acmeId).cause,
      )

      archiveContract(participant1, cidAcme)()

      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        PackagesVetted(cantonTestsPkg, Set(daId, acmeId)).cause,
      )

      removeVetting(participant1, daId)
      removeVetting(participant1, acmeId)

      logger.info(s"Can remove package that is unused and unvetted")
      participant1.packages.remove(cantonTestsPkg)

    }

    "remove packages for contracts that have been reassigned" taggedAs_ {
      ledgerIntegrity.setHappyCase(_)
    } in { implicit env =>
      import env.*

      setUp(participant1, sequencer1, daName, daId)
      participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
      uploadDarAndVerifyVettingOnSynchronizer(
        participant1,
        PerformanceTestPath,
        performanceTestPkg,
        daId,
        acmeId,
      )

      // Ensure that the latest time proof known to the reassigning participant
      // is more recent than the vetting, since the vetting status is checked at
      // the time proof, rather than at the latest known status
      // TODO(i13200) The following line can be removed once the ticket is closed
      participant1.testing.fetch_synchronizer_time(acmeId)

      val activeContractsForPkg = participant1.testing.acs_search(
        daName,
        filterPackage = cantonTestsPkg,
        exactId = "",
        filterTemplate = "",
      )

      val manyId = activeContractsForPkg.headOption.value.contractId

      val pkg2 = PackageId.assertFromString(Asset.PACKAGE_ID)
      val adminParty = participant1.id.adminParty

      val cmd = new Asset(
        adminParty.toProtoPrimitive,
        adminParty.toProtoPrimitive,
        "PackageRemovalTestAsset",
        "",
        adminParty.toProtoPrimitive,
        adminParty.toProtoPrimitive,
      ).create.commands.asScala.toSeq

      val asset = JavaDecodeUtil
        .decodeAllCreated(Asset.COMPANION)(
          participant1.ledger_api.javaapi.commands.submit(
            Seq(adminParty),
            cmd,
            Some(daId),
          )
        )
        .headOption
        .value

      val assetId = asset.id.toLf

      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        new PackageInUse(cantonTestsPkg, manyId, daId).cause,
      )
      packageRemovalFails(participant1, pkg2, new PackageInUse(pkg2, assetId, daId).cause)

      participant1.ledger_api.commands.submit_reassign(
        participant1.adminParty,
        Seq(assetId),
        source = synchronizer1Id,
        target = synchronizer2Id,
      )

      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        new PackageInUse(cantonTestsPkg, manyId, daId).cause,
      )
      packageRemovalFails(participant1, pkg2, new PackageInUse(pkg2, assetId, acmeId).cause)

      val archiveCmd =
        asset.id.exerciseArchive().commands.loneElement

      participant1.ledger_api.javaapi.commands
        .submit(actAs = Seq(participant1.adminParty), commands = Seq(archiveCmd))

      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        new PackageInUse(cantonTestsPkg, manyId, daId).cause,
      )
      packageRemovalFails(participant1, pkg2, PackagesVetted(pkg2, Set(daId, acmeId)).cause)

      removeVetting(participant1, daId, darPath = PerformanceTestPath)
      removeVetting(participant1, acmeId, darPath = PerformanceTestPath)

      participant1.packages.remove(pkg2)

      packageRemovalFails(
        participant1,
        cantonTestsPkg,
        new PackageInUse(cantonTestsPkg, manyId, daId).cause,
      )

      archiveContract(participant1, manyId)()

      packageRemovalFails(participant1, cantonTestsPkg, PackagesVetted(cantonTestsPkg, daId).cause)

      removeVetting(participant1, daId)
      removeVetting(participant1, acmeId)

      participant1.packages.remove(cantonTestsPkg)

    }

  }

  private def checkPackageOnlyRemovedWhenUnusedUnvetted(
      participant: => LocalParticipantReference,
      pkg: PackageId,
      synchronizerId: SynchronizerId,
      darPath: String = CantonTestsPath,
  )(
      mkArchiveCmd: String => Command = cid =>
        new Many.ContractId(cid)
          .exerciseArchive()
          .commands
          .loneElement
  )(implicit env: TestConsoleEnvironment): Unit = {
    logger.info(s"Cannot remove package that is used and vetted")

    val cid = activeContractsForPackage(participant, env.daName, pkg).loneElement

    packageRemovalFails(participant, pkg, new PackageInUse(pkg, cid, env.daId).cause)

    archiveContract(participant, cid)(mkArchiveCmd)

    logger.info(s"Cannot remove package that is vetted")
    packageRemovalFails(participant, pkg, PackagesVetted(pkg, env.daId).cause)

    removeVetting(participant, synchronizerId, darPath)

    logger.info(s"Can remove package that is unused and unvetted")
    participant.packages.remove(pkg)
  }

  private def removeVetting(
      participant1: => LocalParticipantReference,
      synchronizerId: SynchronizerId,
      darPath: String = CantonTestsPath,
  ): Unit = {
    val archive = DarParser
      .readArchiveFromFile(new File(darPath))
      .getOrElse(fail("cannot read examples"))

    val pkgs = Seq(DamlPackageStore.readPackageId(archive.main))

    participant1.topology.vetted_packages.propose_delta(
      participant1.id,
      removes = pkgs,
      store = synchronizerId,
    )

    participant1.packages.synchronize_vetting()
  }

  private def activeContractsForPackage(
      participant: LocalParticipantReference,
      synchronizerAlias: SynchronizerAlias,
      pkg: String,
  ): Seq[LfContractId] =
    participant.testing
      .acs_search(
        synchronizerAlias,
        filterPackage = pkg,
      )
      .map(_.contractId)

  private def archiveContract(
      participant1: => LocalParticipantReference,
      cid: ContractId,
  )(
      mkArchiveCmd: String => Command = cid =>
        new Many.ContractId(cid)
          .exerciseArchive()
          .commands
          .loneElement
  ) = {

    val cmd: Command = mkArchiveCmd(cid.coid)
    participant1.ledger_api.javaapi.commands.submit(Seq(participant1.id.adminParty), Seq(cmd))
  }

  private def packageRemovalFails(
      participant1: => LocalParticipantReference,
      pkg: String,
      message: String,
  ) =
    assertThrowsAndLogsCommandFailures(
      participant1.packages.remove(pkg),
      entry =>
        entry.shouldBeCommandFailure(
          com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.code,
          message,
        ),
    )

  private def setUp(
      participant: => LocalParticipantReference,
      sequencerConnection: SequencerReference,
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
  ): (String, DarDescription) = {
    clue("connecting to synchronizer") {
      participant.synchronizers.connect_local(sequencerConnection, alias = synchronizerAlias)
    }
    val darHex =
      uploadDarAndVerifyVettingOnSynchronizer(
        participant,
        CantonTestsPath,
        cantonTestsPkg,
        synchronizerId,
      )

    // Archive contracts left over from previous test cases.
    archiveContracts(participant, synchronizerAlias)

    assertPackageUsable(participant, participant, synchronizerId)
    val darDescriptor = descriptorForHexHash(participant, darHex)
    darHex -> darDescriptor

  }

  private def archiveContracts(
      participant: LocalParticipantReference,
      synchronizerAlias: SynchronizerAlias,
  ): Unit =
    for (
      contract <- participant.testing
        .acs_search(
          synchronizerAlias,
          filterPackage = cantonTestsPkg,
        )
      if contract.metadata.signatories equals Set(participant.adminParty.toLf)
    ) {
      archiveContract(participant, contract.contractId)()
    }

  private def uploadDarAndVerifyVettingOnSynchronizer(
      participant: LocalParticipantReference,
      darPath: String,
      pkg: PackageId,
      synchronizerIds: SynchronizerId*
  ): String =
    synchronizerIds
      .map { synchronizerId =>
        val darHex = clue(s"uploading and vetting tests on $synchronizerId") {
          participant.dars.upload(darPath, synchronizerId = synchronizerId)
        }
        eventually() {
          participant.topology.vetted_packages
            .list(
              store = synchronizerId,
              filterParticipant = participant.id.filterString,
            )
            .loneElement
            .item
            .packages
            .map(_.packageId) should contain(pkg)
        }
        darHex
      }
      .headOption
      .value

  def descriptorForHexHash(
      participant: ParticipantReference,
      hexString: String,
  ): DarDescription = {
    val dd =
      participant.dars.list().find(_.mainPackageId == hexString).valueOrFail("Must be there")
    DarDescription(
      mainPackageId = DarMainPackageId.tryCreate(dd.mainPackageId),
      description = String255.tryCreate(dd.description),
      name = String255.tryCreate(dd.name),
      version = String255.tryCreate(dd.version),
    )
  }

}

// Do not run this test with in-memory storage, as it restarts participant nodes.
class PackageRemovalIntegrationTestPostgres extends PackageRemovalIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
}
