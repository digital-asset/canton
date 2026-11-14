// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.limits

import com.daml.ledger.api.v2.transaction.Transaction
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, InstanceReference}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.PackageService.DarMainPackageId
import com.digitalasset.canton.participant.protocol.TransactionProcessor
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.util.SetupPackageVetting
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.value.Value.ContractId
import monocle.macros.syntax.lens.*

import java.nio.file.Path

class TransactionViewLimitsIT
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {

  val darPath: Path = Path.of(TransactionLimitsPath)
  val TransactionLimitTestsPkgId: LfPackageId =
    DarMainPackageId.tryFromDarPath(darPath).toLfPackageId

  private var party1, party2: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Config
      .withNetworkBootstrap { implicit env =>
        import env.*

        val defaultSsp = EnvironmentDefinition.defaultStaticSynchronizerParameters

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
            overrideStaticSynchronizerParameters =
              Option.when(testedProtocolVersion >= ProtocolVersion.v36)(
                defaultSsp
                  .focus(_.synchronizerLimits.transactionProtocolLimits.maxTransactionRootViews)
                  .replace(PositiveInt.one)
                  .focus(_.synchronizerLimits.transactionProtocolLimits.maxTransactionSubViews)
                  .replace(PositiveInt.one)
              ),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*

        participants.local.foreach { participant =>
          participant.synchronizers.connect_local(sequencer1, alias = daName)
        }

        PartiesAllocator(Set(participant1, participant2))(
          newParties = Seq(
            "party1" -> participant1,
            "party2" -> participant2,
          ),
          targetTopology = Map(
            "party1" -> Map(
              daId -> (PositiveInt.one, Set(participant1.id -> Submission))
            ),
            "party2" -> Map(
              daId -> (PositiveInt.one, Set(
                participant1.id -> Observation,
                participant2.id -> Submission,
              ))
            ),
          ),
        )

        party1 = "party1".toPartyId(participant1)
        party2 = "party2".toPartyId(participant2)

        // Upload the test DAR to all participants and vet its packages
        SetupPackageVetting(
          Set(s"$darPath"),
          targetTopology = Map(
            daId -> participants.all
              .map(
                _ -> VettedPackage
                  .unbounded(
                    Seq(TransactionLimitTestsPkgId)
                  )
                  .toSet
              )
              .toMap
          ),
        )
      }

  "Transaction view limits should be enabled for submission" in { implicit env =>
    import env.*

    val createCmd1 = ledger_api_utils.create(
      TransactionLimitTestsPkgId,
      "TransactionLimitTests",
      "T",
      Map("owner" -> party1, "watcher" -> party1, "value" -> 0),
    )
    val cid =
      getFirstContractId(participant1.ledger_api.commands.submit(Seq(party1), Seq(createCmd1)))
    val twoViewCmd = ledger_api_utils.exercise(
      TransactionLimitTestsPkgId,
      "TransactionLimitTests",
      "T",
      "TwoViews",
      Map("party" -> party2),
      cid.coid,
    )
    val singleRootAndViewCmd = ledger_api_utils.exercise(
      TransactionLimitTestsPkgId,
      "TransactionLimitTests",
      "T",
      "SingleRootAndView",
      Map.empty,
      cid.coid,
    )

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.ledger_api.commands.submit(Seq(party1), Seq(twoViewCmd)),
      entry => {
        entry.shouldBeCantonErrorCode(TransactionProcessor.SubmissionErrors.MalformedRequest)
        entry.errorMessage should include("Request failed for participant1")
        entry.errorMessage should include(
          "TransactionViewLimitError(Number of subviews for a transaction exceeded 1)"
        )
      },
    )

    val events =
      participant1.ledger_api.commands.submit(Seq(party1), Seq(singleRootAndViewCmd)).events

    inside(events) {
      case Seq(create) if create.event.isCreated =>
        val fields = create.getCreated.getCreateArguments.fields
        inside(fields.find(_.label == "value").flatMap(_.value)) { case Some(value) =>
          value.getInt64 shouldBe 42
        }
    }
  }

  private def getFirstContractId(tx: Transaction): ContractId =
    ContractId.assertFromString(tx.events.map(_.getCreated.contractId).loneElement)
}
