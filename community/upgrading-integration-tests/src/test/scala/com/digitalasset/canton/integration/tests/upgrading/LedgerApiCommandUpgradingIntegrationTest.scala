// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.commands.Command.toJavaProto
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data.codegen.{Contract, ContractCompanion}
import com.daml.ledger.javaapi.data.{Unit as _, *}
import com.digitalasset.canton.BaseTest.UnsupportedExternalPartyTest.MultiRootNodeSubmission
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.damltests.upgrade.v1.java as v1
import com.digitalasset.canton.damltests.upgrade.v2.java as v2
import com.digitalasset.canton.damltests.upgradeck.v1.java as v1ck
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.transaction.ContractInstanceCoder
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*
import scala.util.chaining.*

sealed abstract class LedgerApiCommandUpgradingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private val byPackageNameIdentifier: Identifier =
    Identifier.fromJavaProto(v1.upgrade.Upgrading.TEMPLATE_ID.toProto)

  // The keyed template lives in its own package (UpgradeCK), so it needs its own
  // package-name-scoped identifier for the ExerciseByKey resolution test.
  private val byPackageNameIdentifierCK: Identifier =
    Identifier.fromJavaProto(v1ck.upgradeck.UpgradingCK.TEMPLATE_ID.toProto)

  private def party(name: String)(implicit env: TestConsoleEnvironment): Party =
    env.participant1.parties.list(name).headOption.valueOrFail("where is " + name).party

  private var alice1: Party = _
  private var bob1: Party = _
  private var alice3: Party = _
  private var bob3: Party = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1.withSetup { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant2.synchronizers.connect_local(sequencer1, alias = daName)
      participant3.synchronizers.connect_local(sequencer1, alias = daName)

      alice1 = participant1.parties.testing.enable("alice1")
      bob1 = participant1.parties.testing.enable("bob1")

      // Participant 1 and 2 have both versions

      participant1.dars.upload(UpgradingBaseTest.UpgradeV1)
      participant1.dars.upload(UpgradingBaseTest.UpgradeV2)

      participant2.dars.upload(UpgradingBaseTest.UpgradeV1)
      participant2.dars.upload(UpgradingBaseTest.UpgradeV2)

      // Participant 3 initially has just V1

      alice3 = participant3.parties.testing.enable("alice3")
      bob3 = participant3.parties.testing.enable("bob3")
      participant3.dars.upload(UpgradingBaseTest.UpgradeV1)
    }

  "The Ledger API" when {
    "commands are submitted with a package-name-scoped template id" should {
      "resolve to the available package id" in { implicit env =>
        val templateCon =
          new v1.upgrade.Upgrading(alice3.toProtoPrimitive, alice3.toProtoPrimitive, 0)
        checkAllCommandTypes[v1.upgrade.Upgrading, v1.upgrade.Upgrading.Contract](
          templateCon = templateCon,
          exercise = _.id.exerciseChangeOwner(bob3.toProtoPrimitive).commands.asScala.toSeq,
          createAndExercise =
            templateCon.createAnd.exerciseChangeOwner(bob3.toProtoPrimitive).commands.asScala.toSeq,
          queryingParty = alice3,
          participantOverride = Some(env.participant3),
        )(v1.upgrade.Upgrading.COMPANION)
      }

      "use the newest uploaded package" in { implicit env =>
        // Upload the upgraded template version
        env.participant3.dars.upload(UpgradingBaseTest.UpgradeV2)

        val templateCon =
          new v1.upgrade.Upgrading(alice3.toProtoPrimitive, alice3.toProtoPrimitive, 0)
        checkAllCommandTypes[v1.upgrade.Upgrading, v2.upgrade.Upgrading.Contract](
          templateCon = templateCon,
          exercise = _.id.exerciseChangeOwner(bob3.toProtoPrimitive).commands.asScala.toSeq,
          createAndExercise =
            templateCon.createAnd.exerciseChangeOwner(bob3.toProtoPrimitive).commands.asScala.toSeq,
          queryingParty = alice3,
          participantOverride = Some(env.participant3),
        )(v2.upgrade.Upgrading.COMPANION)
      }

      "override with user package preference" in { implicit env =>
        // Upload the upgraded template version

        val alice = alice1
        val bob = bob1

        val templateCon =
          new v1.upgrade.Upgrading(alice.toProtoPrimitive, alice.toProtoPrimitive, 0)
        checkAllCommandTypes[v1.upgrade.Upgrading, v1.upgrade.Upgrading.Contract](
          templateCon = templateCon,
          exercise = _.id.exerciseChangeOwner(bob.toProtoPrimitive).commands.asScala.toSeq,
          createAndExercise =
            templateCon.createAnd.exerciseChangeOwner(bob.toProtoPrimitive).commands.asScala.toSeq,
          queryingParty = alice,
          userPackagePreference =
            Some(Ref.PackageId.assertFromString(v1.upgrade.Upgrading.PACKAGE_ID)),
        )(v1.upgrade.Upgrading.COMPANION)
      }
    }

    "commands are submitted by key with a package-name-scoped template id" should {
      // Contract keys require Daml-LF >= 2.3 and protocol version >= 3.5, so this uses the keyed
      // UpgradingCK template, which lives in its own LF 2.3 UpgradeCK package (the shared Upgrade
      // package stays on LF 2.1). The UpgradeCK DARs are uploaded here rather than in the shared
      // setup so environments below the required protocol version are unaffected.
      "resolve ExerciseByKey to the newest uploaded package" onlyRunWithOrGreaterThan
        ProtocolVersion.v35 in { implicit env =>
          import env.*

          participant1.dars.upload(UpgradingBaseTest.UpgradeCKV1)
          participant1.dars.upload(UpgradingBaseTest.UpgradeCKV2)

          val alice = party("alice1")
          val bob = party("bob1")

          val ckPackageName = byPackageNameIdentifierCK.packageId

          // Create by specifying the package name; resolves to the newest uploaded package (V2).
          new v1ck.upgradeck.UpgradingCK(
            alice.toProtoPrimitive,
            alice.toProtoPrimitive,
            0,
          ).create.commands.asScala.toSeq
            .map(_.withPackageName(ckPackageName))
            .pipe(
              participant1.ledger_api.javaapi.commands.submit(Seq(alice), _)
            )
            .discard

          // ExerciseByKey on the previously created contract, again by package name: the key is
          // (issuer, field) == (alice, 0).
          v1ck.upgradeck.UpgradingCK
            .byKey(new v1ck.da.types.Tuple2(alice.toProtoPrimitive, java.lang.Long.valueOf(0L)))
            .exerciseChangeOwnerCK(bob.toProtoPrimitive)
            .commands
            .asScala
            .toSeq
            .map(_.withPackageName(ckPackageName))
            .pipe(
              participant1.ledger_api.javaapi.commands.submit(Seq(alice), _)
            )
            .discard
        }
    }

    "upgrading a disclosed contract" should {
      "work" in { implicit env =>
        val alice =
          env.participant1.parties
            .enable(
              "discloser_upgrade",
              synchronizeParticipants = Seq(env.participant2),
            )
        val bob =
          env.participant2.parties
            .enable(
              "disclosee_upgrade",
              synchronizeParticipants = Seq(env.participant1),
            )

        testExplicitDisclosureUpDowngrading(
          discloser = alice,
          disclosee = bob,
          sourceTemplate =
            new v1.upgrade.Upgrading(alice.toProtoPrimitive, alice.toProtoPrimitive, 0),
          sourceTemplateId = v1.upgrade.Upgrading.TEMPLATE_ID,
          exerciseFetchOnTargetVersion = new v2.upgrade.Upgrading.ContractId(_)
            .exerciseUpgrading_Fetch(bob.toProtoPrimitive)
            .commands()
            .overridePackageId(v2.upgrade.Upgrading.PACKAGE_ID),
        )
      }
    }

    "downgrading a disclosed contract" should {
      "work" in { implicit env =>
        val alice =
          env.participant1.parties
            .enable(
              "discloser_downgrade",
              synchronizeParticipants = Seq(env.participant2),
            )
        val bob =
          env.participant2.parties
            .enable(
              "disclosee_downgrade",
              synchronizeParticipants = Seq(env.participant1),
            )

        testExplicitDisclosureUpDowngrading(
          discloser = alice,
          disclosee = bob,
          sourceTemplate = new v2.upgrade.Upgrading(
            alice.toProtoPrimitive,
            alice.toProtoPrimitive,
            0,
            java.util.Optional.empty(),
          ),
          sourceTemplateId = v2.upgrade.Upgrading.TEMPLATE_ID,
          exerciseFetchOnTargetVersion = new v1.upgrade.Upgrading.ContractId(_)
            .exerciseUpgrading_Fetch(bob.toProtoPrimitive)
            .commands(),
        )
      }
    }

    "upgrading a disclosed contract " should {

      "fail on upgrade verification failure" in { implicit env =>
        val alice = env.participant1.parties
          .enable(
            "discloser_upgrade_failure",
            synchronizeParticipants = Seq(env.participant2),
          )
        val bob = env.participant2.parties
          .enable(
            "discloser_upgrade_failure",
            synchronizeParticipants = Seq(env.participant1),
          )

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          testExplicitDisclosureUpDowngrading(
            discloser = alice,
            disclosee = bob,
            sourceTemplate =
              new v1.upgrade.Upgrading(alice.toProtoPrimitive, alice.toProtoPrimitive, 0),
            sourceTemplateId = v1.upgrade.Upgrading.TEMPLATE_ID,
            exerciseFetchOnTargetVersion = new v2.upgrade.Upgrading.ContractId(_)
              .exerciseUpgrading_Fetch(bob.toProtoPrimitive)
              .commands(),
            mutateDisclosedContract = (disclosedContract: DisclosedContract) =>
              new DisclosedContract(
                ContractInstanceCoder
                  .decodeFatContractInstance(disclosedContract.createdEventBlob)
                  .valueOrFail("unexpected decode failure")
                  .setAuthenticationData(Bytes.assertFromString("abcdef"))
                  .pipe(ContractInstanceCoder.encodeFatContractInstance)
                  .valueOrFail("encode failed"),
                disclosedContract.synchronizerId.get(),
                disclosedContract.templateId,
                disclosedContract.contractId,
              ),
          ),
          _.commandFailureMessage should
            (include(s"Request failed for participant2") and
              include("INVALID_ARGUMENT/INTERPRETATION_UPGRADE_ERROR_AUTHENTICATION_FAILED") and
              include("failed to authenticate contract")),
        )
      }
    }
  }

  private def testExplicitDisclosureUpDowngrading(
      discloser: Party,
      disclosee: Party,
      sourceTemplate: Template,
      sourceTemplateId: data.Identifier,
      exerciseFetchOnTargetVersion: String => util.List[data.Command],
      mutateDisclosedContract: DisclosedContract => DisclosedContract = identity,
  )(implicit env: FixtureParam): Unit = {
    import env.*

    participant1.ledger_api.javaapi.commands.submit(
      Seq(discloser),
      sourceTemplate
        .create()
        .commands()
        .overridePackageId(sourceTemplateId.getPackageId)
        .asScala
        .toSeq,
    )

    val txs = participant1.ledger_api.javaapi.updates.transactions_with_tx_format(
      new TransactionFormat(
        new EventFormat(
          Collections.singletonMap(
            discloser.toProtoPrimitive,
            new CumulativeFilter(
              Collections.emptyMap[data.Identifier, Filter.Interface](),
              Collections.singletonMap[data.Identifier, Filter.Template](
                sourceTemplateId,
                Filter.Template.INCLUDE_CREATED_EVENT_BLOB,
              ),
              None.toJava,
            ),
          ),
          None.toJava,
          false,
        ),
        TransactionShape.ACS_DELTA,
      ),
      1,
    )

    val disclosedContract =
      JavaDecodeUtil.decodeDisclosedContracts(txs.headOption.value.getTransaction.get).loneElement

    participant2.ledger_api.javaapi.commands
      .submit(
        Seq(disclosee),
        exerciseFetchOnTargetVersion(disclosedContract.contractId.toScala.value).asScala.toSeq,
        disclosedContracts = Seq(mutateDisclosedContract(disclosedContract)),
      )
      .discard
  }

  private def checkAllCommandTypes[I <: Template, TCOut <: Contract[?, ?]](
      templateCon: I,
      exercise: TCOut => Seq[javaapi.data.Command],
      createAndExercise: Seq[javaapi.data.Command],
      queryingParty: Party,
      userPackagePreference: Option[Ref.PackageId] = None,
      participantOverride: Option[LocalParticipantReference] = None,
  )(tc: ContractCompanion[TCOut, ?, ?])(implicit env: FixtureParam): Assertion = {
    val participant = participantOverride.getOrElse(env.participant1)

    // Create by specifying the package name
    val createUpgrading_byPackageName =
      templateCon.create.commands.asScala.toSeq
        .map(_.withPackageName)
        .pipe(
          participant.ledger_api.javaapi.commands.submit(
            Seq(queryingParty),
            _,
            userPackageSelectionPreference = userPackagePreference.toList,
          )
        )
        .pipe(JavaDecodeUtil.decodeAllCreated(tc))
        .pipe(inside(_) { case Seq(contract) => contract })

    // Exercise command on the previously created contract.
    // Use ledger-effects shape so this works for both local and external parties: an external
    // party auto-routes to the ISS submit path, which always returns ledger-effects transactions
    // (ExercisedEvent instead of ArchivedEvent), so the consuming exercise must be read as an ExercisedEvent.
    exercise(createUpgrading_byPackageName)
      .map(_.withPackageName)
      .pipe(
        participant.ledger_api.javaapi.commands.submit(
          Seq(queryingParty),
          _,
          userPackageSelectionPreference = userPackagePreference.toList,
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        )
      )
      .pipe(JavaDecodeUtil.decodeAllArchivedLedgerEffectsEvents(tc))
      .pipe(inside(_) { case Seq(cId) => cId shouldBe createUpgrading_byPackageName.id })

    // CreateAndExercise command
    if (onlyLocalParty(MultiRootNodeSubmission)) {
      createAndExercise
        .map(_.withPackageName)
        .pipe(
          participant.ledger_api.javaapi.commands.submit(
            Seq(queryingParty),
            _,
            userPackageSelectionPreference = userPackagePreference.toList,
            transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
          )
        )
        .pipe { tx =>
          inside(JavaDecodeUtil.decodeAllCreated(tc)(tx)) { contracts =>
            val cid = JavaDecodeUtil.decodeAllArchivedLedgerEffectsEvents(tc)(tx).loneElement

            contracts.map(_.id) should contain(cid)
          }
        }
    } else succeed
  }

  private implicit class CommandWithoutPackageId(commandJava: javaapi.data.Command) {
    def withPackageName: javaapi.data.Command = withPackageName(byPackageNameIdentifier.packageId)

    def withPackageName(packageName: String): javaapi.data.Command = {
      val command = Command.fromJavaProto(commandJava.toProtoCommand)
      val res = command.command match {
        case Command.Command.Empty => command
        case c: Command.Command.Create =>
          command.copy(command =
            c.focus(_.value.templateId).modify(_.map(_.copy(packageId = packageName)))
          )
        case c: Command.Command.Exercise =>
          command.copy(command =
            c.focus(_.value.templateId).modify(_.map(_.copy(packageId = packageName)))
          )
        case c: Command.Command.ExerciseByKey =>
          command.copy(command =
            c.focus(_.value.templateId).modify(_.map(_.copy(packageId = packageName)))
          )
        case c: Command.Command.CreateAndExercise =>
          command.copy(command =
            c.focus(_.value.templateId).modify(_.map(_.copy(packageId = packageName)))
          )
      }
      javaapi.data.Command.fromProtoCommand(toJavaProto(res))
    }
  }
}

final class ReferenceLedgerApiCommandUpgradingIntegrationTestPostgres
    extends LedgerApiCommandUpgradingIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
