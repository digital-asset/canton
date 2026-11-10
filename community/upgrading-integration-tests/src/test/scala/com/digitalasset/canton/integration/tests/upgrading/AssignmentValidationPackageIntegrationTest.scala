// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.damltests.upgrade.v1.java.upgrade.Quote as QuoteV1
import com.digitalasset.canton.damltests.upgrade.v2.java.upgrade.Quote as QuoteV2
import com.digitalasset.canton.integration.util.{
  EntitySyntax,
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
  PartiesAllocator,
  UpdateFormatHelpers,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPackageId, config, protocol}
import com.digitalasset.daml.lf.data.Ref.IdString

import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

class AssignmentValidationPackageIntegrationTest
    extends CommunityIntegrationTest
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers
    with SharedEnvironment
    with EntitySyntax {

  private val aliceName = "Alice"
  private val bobName = "Bob"

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1_S1M1
      .addConfigTransform(
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag
      )
      .withSetup { implicit env =>
        import env.*

        // P2 is only connected to acme
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant2.synchronizers.connect_local(sequencer2, alias = acmeName)

        // P2 only has V2 of the package loaded
        participant1.dars.upload(
          UpgradingBaseTest.UpgradeV1,
          synchronizerId = Some(synchronizer1Id),
        )
        participant1.dars.upload(
          UpgradingBaseTest.UpgradeV2,
          synchronizerId = Some(synchronizer1Id),
        )

        participant1.dars.upload(
          UpgradingBaseTest.UpgradeV1,
          synchronizerId = Some(synchronizer2Id),
        )
        participant1.dars.upload(
          UpgradingBaseTest.UpgradeV2,
          synchronizerId = Some(synchronizer2Id),
        )

        participant2.dars.upload(
          UpgradingBaseTest.UpgradeV2,
          synchronizerId = Some(synchronizer2Id),
        )

        PartiesAllocator(Set(participant1, participant2))(
          newParties = Seq(
            aliceName -> participant1,
            bobName -> participant1,
          ),
          targetTopology = Map(
            aliceName -> Map(
              daId -> (PositiveInt.one, Set(
                (participant1.id, ParticipantPermission.Submission)
              )),
              acmeId -> (PositiveInt.one, Set(
                (participant1.id, ParticipantPermission.Submission),
                (participant2.id, ParticipantPermission.Submission),
              )),
            ),
            bobName -> Map(
              daId -> (PositiveInt.one, Set(
                (participant1.id, ParticipantPermission.Submission)
              )),
              acmeId -> (PositiveInt.one, Set(
                (participant1.id, ParticipantPermission.Submission)
              )),
            ),
          ),
        )
      }

  private val v1PackageId: IdString.PackageId = LfPackageId.assertFromString(QuoteV1.PACKAGE_ID)
  private val v2PackageId: IdString.PackageId = LfPackageId.assertFromString(QuoteV2.PACKAGE_ID)

  "create contract on P1 using V1 package" in { implicit env =>
    import env.*

    val alice = aliceName.toPartyId(participant1)

    val quote =
      JavaDecodeUtil
        .decodeAllCreated(QuoteV1.COMPANION)(
          participant1.ledger_api.javaapi.commands.submit(
            actAs = Seq(alice),
            commands = new QuoteV1(
              List(alice.toProtoPrimitive).asJava,
              List(alice.toProtoPrimitive).asJava,
              "DA",
              100,
            ).create.commands.asScala.toSeq,
            synchronizerId = Some(synchronizer1Id),
            userPackageSelectionPreference = Seq(v1PackageId), // Force V1
          )
        )
        .loneElement

    // Local query
    val p1Event = participant1.ledger_api.state.acs.of_party(alice).loneElement.event

    // Field should be enriched
    p1Event.getCreateArguments.fields
      .map(_.label) shouldBe Seq("issuer", "observer", "stockName", "value")

    p1Event.contractId shouldBe quote.id.contractId

    // Contract should not exist on P2
    participant2.ledger_api.state.acs.of_party(alice) shouldBe Seq.empty

  }

  private def aliceQuote()(implicit env: FixtureParam): (PartyId, protocol.LfContractId) = {
    val alice = aliceName.toPartyId(env.participant1)
    val quoteCid = LfContractId.assertFromString(
      env.participant1.ledger_api.state.acs.of_party(alice).loneElement.event.contractId
    )
    (alice, quoteCid)
  }

  "Disallow package overrides prior to PV37" onlyRunWithOrLessThan ProtocolVersion.v36 in {
    implicit env =>
      import env.*

      val (alice, quoteCid) = aliceQuote()

      def assertInvalidArgument(
          overrideSourceValidationPackageIds: Map[LfContractId, LfPackageId] = Map.empty,
          overrideTargetValidationPackageIds: Map[LfContractId, LfPackageId] = Map.empty,
      ): Unit =
        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.commands.submit_reassign(
            submitter = alice,
            contractIds = Seq(quoteCid),
            source = daId,
            target = acmeId,
            overrideSourceValidationPackageIds = overrideSourceValidationPackageIds,
            overrideTargetValidationPackageIds = overrideTargetValidationPackageIds,
          ),
          entry => {
            entry.shouldBeCantonErrorCode(RequestValidationErrors.InvalidArgument)
            entry.message should include(
              "Can only use validation package overrides when both source and target synchronizers are running protocol version 37 or later"
            )
          },
        )

      assertInvalidArgument(overrideSourceValidationPackageIds = Map(quoteCid -> v2PackageId))
      assertInvalidArgument(overrideTargetValidationPackageIds = Map(quoteCid -> v2PackageId))

  }

  "Disable package choosing prior PV37" in { implicit env =>
    import env.*

    val bob = bobName.toPartyId(participant1)

    val quote =
      JavaDecodeUtil
        .decodeAllCreated(QuoteV1.COMPANION)(
          participant1.ledger_api.javaapi.commands.submit(
            actAs = Seq(bob),
            commands = new QuoteV1(
              List(bob.toProtoPrimitive).asJava,
              List(bob.toProtoPrimitive).asJava,
              "DA",
              101,
            ).create.commands.asScala.toSeq,
            synchronizerId = Some(synchronizer1Id),
            userPackageSelectionPreference = Seq(v1PackageId),
          )
        )
        .loneElement

    val quoteCid = LfContractId.assertFromString(quote.id.contractId)

    // The chosen validation package will be V2 if running >= PV37
    val expected =
      if (testedProtocolVersion >= ProtocolVersion.v37) QuoteV2.PACKAGE_ID else QuoteV1.PACKAGE_ID

    participant1.ledger_api.commands.submit_reassign(
      submitter = bob,
      contractIds = Seq(quoteCid),
      source = daId,
      target = acmeId,
    )

    participant1.ledger_api.state.acs
      .of_party(bob)
      .loneElement
      .event
      .representativePackageId shouldBe expected

    participant1.ledger_api.commands.submit_reassign(
      submitter = bob,
      contractIds = Seq(quoteCid),
      source = acmeId,
      target = daId,
    )

    participant1.ledger_api.state.acs
      .of_party(bob)
      .loneElement
      .event
      .representativePackageId shouldBe expected

  }

  "Fail if the target validation package is not vetted on the target topology" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val (alice, quoteCid) = aliceQuote()

      assertThrowsAndLogsCommandFailures(
        participant1.ledger_api.commands.submit_reassign(
          submitter = alice,
          contractIds = Seq(quoteCid),
          source = daId,
          target = acmeId,
          overrideTargetValidationPackageIds = Map(quoteCid -> v1PackageId),
        ),
        entry => {
          entry.shouldBeCantonErrorCode(RequestValidationErrors.InvalidArgument)
          entry.message should include regex
            s"(?s)Cannot unassign contracts.*on synchronizer `synchronizer2::.*Participant PAR::participant2::.*has not vetted"
        },
      )
  }

  "Assign the contract to a synchronizer that only has V2 of the package" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val (alice, quoteCid) = aliceQuote()

      val p1Event = participant1.ledger_api.state.acs.of_party(alice).loneElement.event

      // Reassign contract to acme
      participant1.ledger_api.commands.submit_reassign(
        submitter = alice,
        contractIds = Seq(quoteCid),
        source = daId,
        target = acmeId,
        overrideTargetValidationPackageIds = Map(quoteCid -> v2PackageId),
      )

      // Now available on P2
      val p2Event = participant2.ledger_api.state.acs.of_party(alice).loneElement.event

      // The template ids should both be V1
      p1Event.getTemplateId.packageId shouldBe QuoteV1.COMPANION.PACKAGE_ID
      p2Event.getTemplateId.packageId shouldBe QuoteV1.COMPANION.PACKAGE_ID

      // But the representative packages are (V1 and V2)
      p1Event.representativePackageId shouldBe QuoteV1.PACKAGE_ID
      p2Event.representativePackageId shouldBe QuoteV2.PACKAGE_ID

      // But the records have been typed against different packages (V1 and V2)
      p1Event.getCreateArguments.getRecordId.packageId shouldBe QuoteV1.PACKAGE_ID
      p2Event.getCreateArguments.getRecordId.packageId shouldBe QuoteV2.PACKAGE_ID

      // Other than type info the values are the same
      p2Event.getCreateArguments.fields shouldBe p1Event.getCreateArguments.fields
  }

  "Assign the contract from a synchronizer that only has V2 of the package" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val (alice, quoteCid) = aliceQuote()

      participant1.ledger_api.commands.submit_reassign(
        submitter = alice,
        contractIds = Seq(quoteCid),
        source = acmeId,
        target = daId,
        overrideSourceValidationPackageIds = Map(quoteCid -> v2PackageId),
      )

  }

  "Assign the contract to a synchronizer that only has V2 of the package by default" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val (alice, quoteCid) = aliceQuote()

      participant1.ledger_api.commands.submit_reassign(
        submitter = alice,
        contractIds = Seq(quoteCid),
        source = daId,
        target = acmeId,
      )

  }

  "Assign the contract from a synchronizer that only has V2 of the package by default" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val (alice, quoteCid) = aliceQuote()

      participant1.ledger_api.commands.submit_reassign(
        submitter = alice,
        contractIds = Seq(quoteCid),
        source = acmeId,
        target = daId,
      )

  }

  "Asynchronously assign the contract from a synchronizer that only has V2 of the package" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val (alice, quoteCid) = aliceQuote()

      // From da to acme

      val (daUnassignment, _) = unassign(
        cid = quoteCid,
        source = daId,
        target = acmeId,
        submittingParty = alice.toLf,
        overrideTargetValidationPackageIds = Map(quoteCid -> v2PackageId),
      )

      assign(
        reassignmentId = daUnassignment.reassignmentId,
        source = daId,
        target = acmeId,
        submittingParty = alice.toLf,
      )

      // From acme to da

      val (acmeUnassignment, _) = unassign(
        cid = quoteCid,
        source = acmeId,
        target = daId,
        submittingParty = alice.toLf,
        overrideSourceValidationPackageIds = Map(quoteCid -> v2PackageId),
      )

      assign(
        reassignmentId = acmeUnassignment.reassignmentId,
        source = acmeId,
        target = daId,
        submittingParty = alice.toLf,
      )

  }

  "Using java-api assign the contract from a synchronizer that only has V2 of the package" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val (alice, quoteCid) = aliceQuote()

      // From da to acme

      val daUnassignment = participant1.ledger_api.javaapi.commands
        .submit_unassign(
          submitter = alice,
          contractIds = Seq(quoteCid),
          source = daId,
          target = acmeId,
          overrideTargetValidationPackageIds = Map(quoteCid -> v2PackageId),
        )
        .getEvents
        .loneElement
        .toProtoEvent
        .getUnassigned

      assign(
        reassignmentId = daUnassignment.getReassignmentId,
        source = daId,
        target = acmeId,
        submittingParty = alice.toLf,
      )

      // From acme to da

      val acmeUnassignment = participant1.ledger_api.javaapi.commands
        .submit_unassign(
          submitter = alice,
          contractIds = Seq(quoteCid),
          source = acmeId,
          target = daId,
          overrideSourceValidationPackageIds = Map(quoteCid -> v2PackageId),
        )
        .getEvents
        .loneElement
        .toProtoEvent
        .getUnassigned

      assign(
        reassignmentId = acmeUnassignment.getReassignmentId,
        source = acmeId,
        target = daId,
        submittingParty = alice.toLf,
      )

  }

  "Auto reassignment package selection" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val (alice, quoteCid) = aliceQuote()

      val updateFormat = UpdateFormatHelpers.getUpdateFormat(
        Set(alice),
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        includeReassignments = true,
      )

      val command = new QuoteV2.ContractId(quoteCid.coid)
        .exerciseQ_Fetch(alice.toProtoPrimitive)
        .commands()
        .loneElement

      val offset1 = participant1.ledger_api.state.end()

      // Exercise on acme (causes reassignment from da)
      participant1.ledger_api.javaapi.commands
        .submit(
          actAs = Seq(alice),
          commands = Seq(command),
          userPackageSelectionPreference = Seq.empty,
          synchronizerId = Some(acmeId),
        )

      inside(
        participant1.ledger_api.updates.updates(
          updateFormat,
          completeAfter = 3,
          beginOffsetExclusive = offset1,
          timeout = config.NonNegativeDuration.ofSeconds(5),
        )
      ) {
        case Seq(
              _: UpdateService.UnassignedWrapper,
              aw: UpdateService.AssignedWrapper,
              tw: UpdateService.TransactionWrapper,
            ) => {
          // V2 is usd as only this is available on acme
          tw.transaction.events.loneElement.getExercised.getTemplateId.packageId shouldBe QuoteV2.PACKAGE_ID

          // Both V1 or V2 could be used for unassignment validation, V2 will be chosen due to the higher package version (see unit test)

          // The package used in the TX is also used as the target validation package
          aw.events.loneElement.getCreatedEvent.representativePackageId shouldBe QuoteV2.PACKAGE_ID

        }
      }

      val offset2 = participant1.ledger_api.state.end()

      // Exercise on da (causes reassignment from acme)
      participant1.ledger_api.javaapi.commands
        .submit(
          actAs = Seq(alice),
          commands = Seq(command),
          // The target da with validate using V1 but V2 will be used for unassigning on the source synchronizer
          userPackageSelectionPreference = Seq(v1PackageId),
          synchronizerId = Some(daId),
        )

      inside(
        participant1.ledger_api.updates.updates(
          updateFormat,
          completeAfter = 3,
          beginOffsetExclusive = offset2,
          timeout = config.NonNegativeDuration.ofSeconds(5),
        )
      ) {
        case Seq(
              _: UpdateService.UnassignedWrapper,
              aw: UpdateService.AssignedWrapper,
              tw: UpdateService.TransactionWrapper,
            ) => {
          // V1 is used in the transaction due to user package selection preference
          tw.transaction.events.loneElement.getExercised.getTemplateId.packageId shouldBe QuoteV1.PACKAGE_ID

          // Only V2 is available on acme so must have been used for the unassignment

          // The package used in the TX is also used as the target validation package
          aw.events.loneElement.getCreatedEvent.representativePackageId shouldBe QuoteV1.PACKAGE_ID
        }
      }

  }

}
