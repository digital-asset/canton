// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.implicits.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewType.TransferInViewType
import com.digitalasset.canton.data.{CantonTimestamp, FullTransferInTree, TransferSubmitterMetadata}
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  SeedGenerator,
}
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
  private val sourceDomain = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::source"))
  private val sourceMediator = MediatorId(
    UniqueIdentifier.tryFromProtoPrimitive("mediator::source")
  )
  private val targetDomain = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
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
      None,
    )
  }

  private val workflowId: Option[LfWorkflowId] = None

  private val identityFactory = TestingTopology()
    .withDomains(sourceDomain)
    .withReversedTopology(
      Map(submitterParticipant -> Map(party1 -> ParticipantPermission.Submission))
    )
    .withParticipants(participant) // required such that `participant` gets a signing key
    .build(loggerFactory)

  private val cryptoSnapshot =
    identityFactory
      .forOwnerAndDomain(submitterParticipant, sourceDomain)
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
      workflowId,
      contractId,
      transferId.sourceDomain,
      SourceProtocolVersion(testedProtocolVersion),
      sourceMediator,
      targetDomain,
      TargetProtocolVersion(testedProtocolVersion),
      TimeProofTestUtil.mkTimeProof(timestamp = CantonTimestamp.Epoch, domainId = targetDomain),
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
        fullTransferOutTree,
        CantonTimestamp.Epoch,
        contract,
        transactionId1,
        Some(transferOutResult),
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
      domainId: DomainId,
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
      causalityTracking = true,
      TargetProtocolVersion(testedProtocolVersion),
      loggerFactory = loggerFactory,
    )
  }

  private def makeFullTransferInTree(
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      targetDomain: DomainId,
      targetMediator: MediatorId,
      transferOutResult: DeliveredTransferOutResult,
      uuid: UUID = new UUID(4L, 5L),
  ): FullTransferInTree = {
    val seed = seedGenerator.generateSaltSeed()
    TransferInProcessingSteps.makeFullTransferInTree(
      pureCrypto,
      seed,
      submitterInfo(submitter),
      workflowId,
      stakeholders,
      contract,
      creatingTransactionId,
      targetDomain,
      targetMediator,
      transferOutResult,
      uuid,
      SourceProtocolVersion(testedProtocolVersion),
      TargetProtocolVersion(testedProtocolVersion),
    )
  }

  private def encryptFullTransferInTree(
      tree: FullTransferInTree
  ): Future[EncryptedViewMessage[TransferInViewType]] =
    EncryptedViewMessageFactory
      .create(TransferInViewType)(tree, cryptoSnapshot, testedProtocolVersion)
      .fold(
        error => throw new IllegalArgumentException(s"Cannot encrypt transfer-in request: $error"),
        Predef.identity,
      )
}
