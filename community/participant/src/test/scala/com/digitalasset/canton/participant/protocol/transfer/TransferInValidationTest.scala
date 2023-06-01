// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.implicits.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.{CantonTimestamp, FullTransferInTree, TransferSubmitterMetadata}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.participant.protocol.transfer.TransferInValidation.*
import com.digitalasset.canton.participant.store.TransferStoreTest.transactionId1
import com.digitalasset.canton.protocol.ExampleTransactionFactory.submitterParticipant
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.{Future, Promise}

class TransferInValidationTest extends AsyncWordSpec with BaseTest {
  private val sourceDomain = SourceDomainId(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::source"))
  )
  private val sourceMediator = MediatorId(
    UniqueIdentifier.tryFromProtoPrimitive("mediator::source")
  )
  private val targetDomain = TargetDomainId(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
  )
  private val targetMediator = MediatorId(
    UniqueIdentifier.tryFromProtoPrimitive("mediator::target")
  )

  private val party1: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party1::party")
  ).toLf
  private val party2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party2::party")
  ).toLf

  private val participant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("bothdomains::participant")
  )
  private def submitterInfo(submitter: LfPartyId): TransferSubmitterMetadata = {
    TransferSubmitterMetadata(
      submitter,
      LedgerApplicationId.assertFromString("tests"),
      participant.toLf,
      LedgerCommandId.assertFromString("transfer-in-validation-command-id"),
      submissionId = None,
      workflowId = None,
    )
  }

  private val identityFactory = TestingTopology()
    .withDomains(sourceDomain.unwrap)
    .withReversedTopology(
      Map(submitterParticipant -> Map(party1 -> ParticipantPermission.Submission))
    )
    .withSimpleParticipants(participant) // required such that `participant` gets a signing key
    .build(loggerFactory)

  private val cryptoSnapshot =
    identityFactory
      .forOwnerAndDomain(submitterParticipant, sourceDomain.unwrap)
      .currentSnapshotApproximation

  private val pureCrypto = TestingIdentityFactory.pureCrypto()

  private val seedGenerator = new SeedGenerator(pureCrypto)

  private val transferInValidation =
    testInstance(targetDomain, Set(party1), Set(party1), cryptoSnapshot, None)

  "validateTransferInRequest" should {
    val contractId = ExampleTransactionFactory.suffixedId(10, 0)
    val contract = ExampleTransactionFactory.asSerializable(
      contractId,
      contractInstance = ExampleTransactionFactory.contractInstance(),
    )
    val transferOutResult =
      TransferResultHelpers.transferOutResult(
        sourceDomain,
        cryptoSnapshot,
        submitterParticipant,
      )
    val inRequest =
      makeFullTransferInTree(
        party1,
        Set(party1),
        contract,
        transactionId1,
        targetDomain,
        targetMediator,
        transferOutResult,
      )

    "succeed without errors in the basic case" in {
      for {
        result <- valueOrFail(
          transferInValidation
            .validateTransferInRequest(
              CantonTimestamp.Epoch,
              inRequest,
              None,
              cryptoSnapshot,
              transferringParticipant = false,
            )
        )("validation of transfer in request failed")
      } yield {
        result shouldBe None
      }
    }

    val transferId = TransferId(sourceDomain, CantonTimestamp.Epoch)
    val transferOutRequest = TransferOutRequest(
      submitterInfo(party1),
      Set(party1, party2), // Party 2 is a stakeholder and therefore a receiving party
      Set.empty,
      contractId,
      contract.rawContractInstance.contractInstance.unversioned.template,
      transferId.sourceDomain,
      SourceProtocolVersion(testedProtocolVersion),
      MediatorRef(sourceMediator),
      targetDomain,
      TargetProtocolVersion(testedProtocolVersion),
      TimeProofTestUtil.mkTimeProof(timestamp = CantonTimestamp.Epoch, targetDomain = targetDomain),
      TransferCounter.Genesis,
    )
    val uuid = new UUID(3L, 4L)
    val seed = seedGenerator.generateSaltSeed()
    val fullTransferOutTree =
      transferOutRequest.toFullTransferOutTree(
        pureCrypto,
        pureCrypto,
        seed,
        uuid,
      )
    val transferData =
      TransferData(
        SourceProtocolVersion(testedProtocolVersion),
        CantonTimestamp.Epoch,
        RequestCounter(1),
        valueOrFail(fullTransferOutTree)("Failed to create fullTransferOutTree"),
        CantonTimestamp.Epoch,
        contract,
        TransferCounter.Genesis,
        transactionId1,
        Some(transferOutResult),
        None,
        None,
      )

    "succeed without errors when transfer data is valid" in {
      for {
        result <- valueOrFail(
          transferInValidation
            .validateTransferInRequest(
              CantonTimestamp.Epoch,
              inRequest,
              Some(transferData),
              cryptoSnapshot,
              transferringParticipant = false,
            )
        )("validation of transfer in request failed")
      } yield {
        result match {
          case Some(TransferInValidationResult(confirmingParties)) =>
            assert(confirmingParties == Set(party1))
          case _ => fail()
        }
      }
    }

    "wait for the topology state to be available " in {
      val promise: Promise[Unit] = Promise()
      val transferInProcessingSteps2 =
        testInstance(
          targetDomain,
          Set(party1),
          Set(party1),
          cryptoSnapshot,
          Some(promise.future), // Topology state is not available
        )

      val inValidated = transferInProcessingSteps2
        .validateTransferInRequest(
          CantonTimestamp.Epoch,
          inRequest,
          Some(transferData),
          cryptoSnapshot,
          transferringParticipant = false,
        )
        .value

      always() {
        inValidated.isCompleted shouldBe false
      }

      promise.completeWith(Future.unit)
      for {
        _ <- inValidated
      } yield { succeed }
    }
  }

  private def testInstance(
      domainId: TargetDomainId,
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      snapshotOverride: DomainSnapshotSyncCryptoApi,
      awaitTimestampOverride: Option[Future[Unit]],
  ): TransferInValidation = {
    val damle = DAMLeTestInstance(participant, signatories, stakeholders)(loggerFactory)

    new TransferInValidation(
      domainId,
      submitterParticipant,
      damle,
      TestTransferCoordination.apply(
        Set(),
        CantonTimestamp.Epoch,
        Some(snapshotOverride),
        Some(awaitTimestampOverride),
        loggerFactory,
      ),
      TargetProtocolVersion(testedProtocolVersion),
      loggerFactory = loggerFactory,
    )
  }

  private def makeFullTransferInTree(
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      targetDomain: TargetDomainId,
      targetMediator: MediatorId,
      transferOutResult: DeliveredTransferOutResult,
      uuid: UUID = new UUID(4L, 5L),
  ): FullTransferInTree = {
    val seed = seedGenerator.generateSaltSeed()
    valueOrFail(
      TransferInProcessingSteps.makeFullTransferInTree(
        pureCrypto,
        seed,
        submitterInfo(submitter),
        stakeholders,
        contract,
        TransferCounter.Genesis,
        creatingTransactionId,
        targetDomain,
        MediatorRef(targetMediator),
        transferOutResult,
        uuid,
        SourceProtocolVersion(testedProtocolVersion),
        TargetProtocolVersion(testedProtocolVersion),
      )
    )("Failed to create FullTransferInTree")
  }

}
