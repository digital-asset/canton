// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import cats.syntax.either.*
import com.daml.ledger.javaapi.data.DisclosedContract
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription
import com.digitalasset.canton.data.{
  ActionDescription,
  GenTransactionTree,
  MerkleSeq,
  MerkleTree,
  TransactionView,
  ViewParticipantData,
}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.ConfigTransforms.disableUpgradeValidation
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.UnsupportedContractId
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.LocalRejectError.ConsistencyRejections.InactiveContracts
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects
import com.digitalasset.canton.protocol.{
  AuthenticatedContractIdVersionV12,
  ContractInstance,
  ExampleContractFactory,
  GenContractInstance,
  InputContract,
}
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.{ContractInstanceCoder, FatContractInstance}
import com.digitalasset.daml.lf.value.Value.ContractId
import org.slf4j.event.Level

import java.util.Optional

class InvalidContractIdSuffixIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private var alice: Party = _
  private var participant: LocalParticipantReference = _
  private var maliciousNode: MaliciousParticipantNode = _
  private var pureCrypto: CryptoPureApi = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(disableUpgradeValidation)
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(CantonExamplesPath)
        alice = participant1.parties.testing.enable("Alice")
        pureCrypto = participant1.crypto.pureCrypto
        participant = participant1
        maliciousNode = MaliciousParticipantNode(
          participant1,
          daId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )
      }

  type FCIAt = FatContractInstance { type CreatedAtTime = ContractInstance#InstCreatedAtTime }

  private def createAndMutate(
      idMutation: ContractId.V1 => ContractId.V1
  ): (GenContractInstance, GenContractInstance, DisclosedContract) = {

    val createTx = participant.ledger_api.javaapi.commands.submit(
      Seq(alice),
      Seq(IouSyntax.testIou(alice, alice).create.commands.loneElement),
      includeCreatedEventBlob = true,
    )

    val inDC: DisclosedContract = JavaDecodeUtil.decodeDisclosedContracts(createTx).loneElement
    val inBlob = inDC.createdEventBlob
    val inCI = ContractInstance.decodeWithCreatedAt(inBlob).value
    val inFCI: FCIAt = inCI.inst.asInstanceOf[FCIAt]
    val inCidV1 = ContractId.V1.assertFromString(inFCI.contractId.coid)

    val mutCID = idMutation(inCidV1)
    val mutFCI: FCIAt = inFCI.mapCid(_ => mutCID).asInstanceOf[FCIAt]
    val mutBlob = ContractInstanceCoder.encodeFatContractInstance(mutFCI).value
    val mutDC = new DisclosedContract(
      mutBlob,
      inDC.synchronizerId.get(),
      inDC.templateId,
      Optional.of(mutCID.coid: String),
    )
    val mutCI = ContractInstance
      .createWithSerialization(mutFCI, inCI.metadata, mutBlob)
      .asInstanceOf[ContractInstance]

    (inCI, mutCI, mutDC)
  }

  private def submitCreateAndCall(idMutation: ContractId.V1 => ContractId.V1): Unit = {

    val (_, _, mutDC) = createAndMutate(idMutation: ContractId.V1 => ContractId.V1)

    val iouCid = new Iou.ContractId(mutDC.contractId.get())

    participant.ledger_api.javaapi.commands.submit(
      actAs = Seq(alice),
      commands = Seq(iouCid.exerciseCall().commands().loneElement),
      disclosedContracts = Seq(mutDC),
    )
  }

  private def maliciousCreateAndCall(idMutation: ContractId.V1 => ContractId.V1): Unit = {

    val (inCI, mutCI, _) = createAndMutate(idMutation: ContractId.V1 => ContractId.V1)

    val iouCid = new Iou.ContractId(inCI.contractId.coid)

    val commandsWithMetadata = CommandsWithMetadata(
      actAs = Seq(alice),
      commands = Seq(
        com.daml.ledger.api.v2.commands.Command
          .fromJavaProto(iouCid.exerciseCall().commands().loneElement.toProtoCommand)
      ),
      disclosedContracts = Map(inCI.contractId -> inCI),
    )

    val addInputContractMutation =
      ViewParticipantData.Optics.coreInputsUnsafe.modify(
        _ ++ Map(mutCI.contractId -> InputContract(mutCI, consumed = true))
      )

    val vpdActionDescriptionMutation = ViewParticipantData.Optics.actionDescriptionUnsafe
      .andThen(ActionDescription.Optics.exercise)
      .andThen(ExerciseActionDescription.Optics.inputContractIdUnsafe)
      .replace(mutCI.contractId)

    val removeInputContractMutation =
      ViewParticipantData.Optics.coreInputsUnsafe.replace(
        Map(mutCI.contractId -> InputContract(mutCI, consumed = true))
      )

    val vpdMutation =
      addInputContractMutation andThen vpdActionDescriptionMutation andThen removeInputContractMutation

    val treeMutation = GenTransactionTree.Optics.rootViewsUnsafe
      .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
      .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
      .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
      .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
      .modify(vpdMutation)

    maliciousNode
      .submitCommand(
        commandsWithMetadata,
        transactionTreeInterceptor = treeMutation,
      )
      .futureValueUS
      .map(_ => ()) shouldBe Either.unit
  }

  "identity mutation is accepted" in { _ =>
    submitCreateAndCall(identity)
  }

  "changing discriminator results in availability failure" in { _ =>
    // Fabricated contract ID is not active
    assertThrowsAndLogsCommandFailures(
      submitCreateAndCall(inCID =>
        ContractId.V1.build(ExampleContractFactory.lfHash(), inCID.suffix).value
      ),
      _.commandFailureMessage should include(InactiveContracts.id),
    )
  }

  def mutateSuffix(inCID: ContractId.V1): ContractId.V1 = {

    // Should be a v12 CantonContractIdVersion
    assert(inCID.suffix.startsWith(AuthenticatedContractIdVersionV12.versionPrefixBytes))

    // Change to v99
    val bytes: Array[Byte] = inCID.suffix.toByteArray
    bytes.update(0, 0xca.toByte) // No-op
    bytes.update(1, 0x99.toByte) // Fictional
    ContractId.V1.build(inCID.discriminator, Bytes.fromByteArray(bytes)).value
  }

  "invalid canton-contract-id-version submission" in { _ =>
    assertThrowsAndLogsCommandFailures(
      submitCreateAndCall(mutateSuffix),
      _.commandFailureMessage should include(UnsupportedContractId.id),
    )
  }

  "invalid canton-contract-id-version confirmation" in { _ =>
    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
      maliciousCreateAndCall(mutateSuffix),
      LogEntry.assertLogSeq(
        Seq(
          (
            e => {
              e.loggerName should startWith(
                "com.digitalasset.canton.participant.protocol.TransactionProcessor:"
              )
              e.warningMessage should include regex s"(?s)FailedToDeserialize.*DefaultDeserializationError.*Invalid disclosed contract id: Malformed contract ID: Suffix"
            },
            "processor",
          ),
          (
            e => {
              e.loggerName should startWith(
                "com.digitalasset.canton.participant.protocol.TransactionProcessingSteps:"
              )
              e.warningMessage should include regex s"(?s)${MalformedRejects.Payloads.id}.*FailedToDeserialize.*DefaultDeserializationError.*Invalid disclosed contract id: Malformed contract ID: Suffix"
            },
            "processor-steps",
          ),
        )
      ),
    )
  }

  "recover after failures" in { _ =>
    submitCreateAndCall(identity)
  }

}
