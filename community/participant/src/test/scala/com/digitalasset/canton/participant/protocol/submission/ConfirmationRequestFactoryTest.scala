// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.functor._
import com.daml.ledger.participant.state.v2.SubmitterInfo
import com.digitalasset.canton._
import com.digitalasset.canton.config.LoggingConfig
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data._
import com.digitalasset.canton.participant.protocol.submission.ConfirmationRequestFactory._
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.{
  UnableToDetermineKey,
  UnableToDetermineParticipant,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  ContractLookupError,
  SerializableContractOfId,
  TransactionTreeConversionError,
}
import com.digitalasset.canton.protocol.ExampleTransactionFactory._
import com.digitalasset.canton.protocol.WellFormedTransaction.{WithSuffixes, WithoutSuffixes}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.protocol.messages.{
  ConfirmationRequest,
  EncryptedView,
  EncryptedViewMessageV0,
  InformeeMessage,
}
import com.digitalasset.canton.sequencing.protocol.OpenEnvelope
import com.digitalasset.canton.topology.transaction.ParticipantPermission._
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ConfirmationRequestFactoryTest extends AsyncWordSpec with BaseTest with HasExecutorService {

  // Parties
  val observerParticipant1: ParticipantId = ParticipantId("observerParticipant1")
  val observerParticipant2: ParticipantId = ParticipantId("observerParticipant2")

  // General dummy parameters
  val domain: DomainId = DefaultTestIdentities.domainId
  val applicationId: ApplicationId = DefaultDamlValues.applicationId()
  val commandId: CommandId = DefaultDamlValues.commandId()
  val mediator: MediatorId = DefaultTestIdentities.mediator
  val ledgerTime: CantonTimestamp = CantonTimestamp.Epoch
  val submissionTime: CantonTimestamp = ledgerTime.plusMillis(7)
  val workflowId: Option[WorkflowId] = Some(
    WorkflowId.assertFromString("workflowIdConfirmationRequestFactoryTest")
  )
  val ledgerConfiguration: LedgerConfiguration = DefaultDamlValues.ledgerConfiguration
  val submitterInfo: SubmitterInfo = DefaultDamlValues.submitterInfo(submitters)

  // Crypto snapshots
  def createCryptoSnapshot(
      partyToParticipant: Map[ParticipantId, Seq[LfPartyId]],
      permission: ParticipantPermission = Submission,
      keyPurposes: Set[KeyPurpose] = KeyPurpose.all,
  ): DomainSnapshotSyncCryptoApi = {

    val map = partyToParticipant.fmap(parties => parties.map(_ -> permission).toMap)
    TestingTopology()
      .withReversedTopology(map)
      .withDomains(domain)
      .withKeyPurposes(keyPurposes)
      .build(loggerFactory)
      .forOwnerAndDomain(submitterParticipant, domain)
      .currentSnapshotApproximation
  }

  val defaultTopology: Map[ParticipantId, Seq[LfPartyId]] = Map(
    submitterParticipant -> Seq(submitter, signatory),
    observerParticipant1 -> Seq(observer),
    observerParticipant2 -> Seq(observer),
  )

  // Collaborators

  // This is a def (and not a val), as the crypto api has the next symmetric key as internal state
  // Therefore, it would not make sense to reuse an instance.
  def newCryptoSnapshot: DomainSnapshotSyncCryptoApi = createCryptoSnapshot(defaultTopology)

  val privateCryptoApi: DomainSnapshotSyncCryptoApi =
    TestingTopology()
      .withParticipants(submitterParticipant)
      .build()
      .forOwnerAndDomain(submitterParticipant, domain)
      .currentSnapshotApproximation
  val randomOps: RandomOps = new SymbolicPureCrypto

  val transactionUuid: UUID = new UUID(10L, 20L)

  val seedGenerator: SeedGenerator =
    new SeedGenerator(randomOps) {
      override def generateUuid(): UUID = transactionUuid
    }

  // Device under test
  def confirmationRequestFactory(
      transactionTreeFactoryResult: Either[TransactionTreeConversionError, GenTransactionTree]
  ): ConfirmationRequestFactory = {

    val transactionTreeFactory: TransactionTreeFactory = new TransactionTreeFactory {
      override def createTransactionTree(
          transaction: WellFormedTransaction[WithoutSuffixes],
          submitterInfo: SubmitterInfo,
          _confirmationPolicy: ConfirmationPolicy,
          _workflowId: Option[WorkflowId],
          _mediatorId: MediatorId,
          transactionSeed: SaltSeed,
          transactionUuid: UUID,
          _topologySnapshot: TopologySnapshot,
          _contractOfId: SerializableContractOfId,
      )(implicit
          traceContext: TraceContext
      ): EitherT[Future, TransactionTreeConversionError, GenTransactionTree] = {
        val actAs = submitterInfo.actAs.toSet
        if (actAs != Set(ExampleTransactionFactory.submitter))
          fail(
            s"Wrong submitters ${actAs.mkString(", ")}. Expected ${ExampleTransactionFactory.submitter}"
          )
        if (transaction.metadata.ledgerTime != ConfirmationRequestFactoryTest.this.ledgerTime)
          fail(s"""Wrong ledger time ${transaction.metadata.ledgerTime}.
                  | Expected ${ConfirmationRequestFactoryTest.this.ledgerTime}""".stripMargin)
        if (transactionUuid != ConfirmationRequestFactoryTest.this.transactionUuid)
          fail(
            s"Wrong transaction UUID $transactionUuid. Expected ${ConfirmationRequestFactoryTest.this.transactionUuid}"
          )
        transactionTreeFactoryResult.toEitherT
      }

      override def tryReconstruct(
          subaction: WellFormedTransaction[WithoutSuffixes],
          rootPosition: ViewPosition,
          confirmationPolicy: ConfirmationPolicy,
          mediatorId: MediatorId,
          salts: Iterable[Salt],
          transactionUuid: UUID,
          topologySnapshot: TopologySnapshot,
          contractOfId: SerializableContractOfId,
          _rbContext: RollbackContext,
      )(implicit traceContext: TraceContext): EitherT[
        Future,
        TransactionTreeConversionError,
        (TransactionView, WellFormedTransaction[WithSuffixes]),
      ] = ???

      override def saltsFromView(view: TransactionViewTree): Iterable[Salt] = ???
    }

    new ConfirmationRequestFactory(submitterParticipant, domain, LoggingConfig(), loggerFactory)(
      transactionTreeFactory,
      seedGenerator,
    )
  }

  private val contractInstanceOfId = { id: LfContractId =>
    EitherT(
      Future.successful[Either[ContractLookupError, SerializableContract]](
        Left(ContractLookupError(id, "Error in test: argument should not be used"))
      )
    )
  }
  // This isn't used because the transaction tree factory is mocked

  // Input factory
  val transactionFactory: ExampleTransactionFactory =
    new ExampleTransactionFactory()(
      confirmationPolicy = ConfirmationPolicy.Signatory,
      ledgerTime = ledgerTime,
    )

  // Expected output factory
  def expectedConfirmationRequest(
      example: ExampleTransaction,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
  ): ConfirmationRequest = {
    val cryptoPureApi = cryptoSnapshot.pureCrypto

    val expectedTransactionViewMessages = example.transactionViewTreesWithWitnesses.map {
      case (tree, witnesses) =>
        val signature =
          if (tree.isTopLevel) {
            Some(
              Await
                .result(privateCryptoApi.sign(tree.transactionId.unwrap).value, 10.seconds)
                .valueOr(err => fail(err.toString))
            )
          } else None

        val keySeed = tree.viewPosition.position.foldRight(testKeySeed) { case (pos, seed) =>
          ProtocolCryptoApi
            .hkdf(cryptoPureApi, ProtocolVersion.latestForTest)(
              seed,
              cryptoPureApi.defaultSymmetricKeyScheme.keySizeInBytes,
              HkdfInfo.subview(pos),
            )
            .valueOr(e => throw new IllegalStateException(s"Failed to derive key: $e"))
        }

        val viewEncryptionScheme = cryptoPureApi.defaultSymmetricKeyScheme
        val symmetricKeyRandomness = ProtocolCryptoApi
          .hkdf(cryptoPureApi, ProtocolVersion.latestForTest)(
            keySeed,
            viewEncryptionScheme.keySizeInBytes,
            HkdfInfo.ViewKey,
          )
          .valueOr(e => fail(s"Failed to derive key: $e"))

        val symmetricKey = cryptoPureApi
          .createSymmetricKey(symmetricKeyRandomness, viewEncryptionScheme)
          .valueOrFail("failed to create symmetric key from randomness")

        val participants = tree.informees
          .map(_.party)
          .map(cryptoSnapshot.ipsSnapshot.activeParticipantsOf(_).futureValue)
          .flatMap(_.keySet)

        val encryptedView = EncryptedView
          .compressed(
            cryptoPureApi,
            symmetricKey,
            TransactionViewType,
            ProtocolVersion.latestForTest,
          )(
            LightTransactionViewTree.fromTransactionViewTree(tree)
          )
          .valueOr(err => fail(s"Failed to encrypt view tree: $err"))

        val ec: ExecutionContext = executorService
        val recipients = witnesses
          .toRecipients(cryptoSnapshot.ipsSnapshot)(ec)
          .value
          .futureValue
          .value

        val createdRandomnessMap = randomnessMap(keySeed, participants, cryptoPureApi)

        OpenEnvelope(
          EncryptedViewMessageV0(
            signature,
            tree.viewHash,
            createdRandomnessMap.fmap(_.encrypted),
            encryptedView,
            transactionFactory.domainId,
          ),
          recipients,
        )
    }

    ConfirmationRequest(
      InformeeMessage(example.fullInformeeTree),
      expectedTransactionViewMessages,
    )
  }

  val testKeySeed = randomOps.generateSecureRandomness(0)

  def randomnessMap(
      randomness: SecureRandomness,
      informeeParticipants: Set[ParticipantId],
      cryptoPureApi: CryptoPureApi,
  ): Map[ParticipantId, AsymmetricEncrypted[SecureRandomness]] = {

    val randomnessPairs = for {
      participant <- informeeParticipants
      publicKey = newCryptoSnapshot.ipsSnapshot
        .encryptionKey(participant)
        .futureValue
        .getOrElse(fail("The defaultIdentitySnapshot really should have at least one key."))
    } yield participant -> cryptoPureApi
      .encryptWith(randomness, publicKey, ProtocolVersion.latestForTest)
      .valueOr(err => fail(err.toString))

    randomnessPairs.toMap
  }

  "A ConfirmationRequestFactory" when {
    "everything is ok" can {

      forEvery(transactionFactory.standardHappyCases) { example =>
        s"create a confirmation request for: $example" in {

          val factory = confirmationRequestFactory(Right(example.transactionTree))

          factory
            .createConfirmationRequest(
              example.wellFormedUnsuffixedTransaction,
              ConfirmationPolicy.Vip,
              submitterInfo,
              ledgerTime,
              workflowId,
              mediator,
              newCryptoSnapshot,
              contractInstanceOfId,
              Some(testKeySeed),
              ProtocolVersion.latestForTest,
            )
            .value
            .map(res =>
              res should equal(Right(expectedConfirmationRequest(example, newCryptoSnapshot)))
            )
        }
      }
    }

    val singleFetch = transactionFactory.SingleFetch()

    "submitter node does not represent submitter" must {

      val emptyCryptoSnapshot = createCryptoSnapshot(Map.empty)

      "be rejected" in {
        val factory = confirmationRequestFactory(Right(singleFetch.transactionTree))

        factory
          .createConfirmationRequest(
            singleFetch.wellFormedUnsuffixedTransaction,
            ConfirmationPolicy.Vip,
            submitterInfo,
            ledgerTime,
            workflowId,
            mediator,
            emptyCryptoSnapshot,
            contractInstanceOfId,
            Some(testKeySeed),
            ProtocolVersion.latestForTest,
          )
          .value
          .map(
            _ should equal(
              Left(
                ParticipantAuthorizationError(
                  s"$submitterParticipant does not host $submitter or is not active."
                )
              )
            )
          )
      }
    }

    "submitter node is not allowed to submit transactions" must {

      val confirmationOnlyCryptoSnapshot =
        createCryptoSnapshot(defaultTopology, permission = Confirmation)

      "be rejected" in {
        val factory = confirmationRequestFactory(Right(singleFetch.transactionTree))

        factory
          .createConfirmationRequest(
            singleFetch.wellFormedUnsuffixedTransaction,
            ConfirmationPolicy.Vip,
            submitterInfo,
            ledgerTime,
            workflowId,
            mediator,
            confirmationOnlyCryptoSnapshot,
            contractInstanceOfId,
            Some(testKeySeed),
            ProtocolVersion.latestForTest,
          )
          .value
          .map(
            _ should equal(
              Left(
                ParticipantAuthorizationError(
                  s"$submitterParticipant is not authorized to submit transactions for $submitter."
                )
              )
            )
          )
      }
    }

    "transactionTreeFactory fails" must {
      "be rejected" in {
        val error = ContractLookupError(ExampleTransactionFactory.suffixedId(-1, -1), "foo")
        val factory = confirmationRequestFactory(Left(error))

        factory
          .createConfirmationRequest(
            singleFetch.wellFormedUnsuffixedTransaction,
            ConfirmationPolicy.Vip,
            submitterInfo,
            ledgerTime,
            workflowId,
            mediator,
            newCryptoSnapshot,
            contractInstanceOfId,
            Some(testKeySeed),
            ProtocolVersion.latestForTest,
          )
          .value
          .map(_ should equal(Left(TransactionTreeFactoryError(error))))
      }
    }

    // Note lack of test for ill-authorized transaction as authorization check is performed by LF upon ledger api submission as of Daml 1.6.0

    "informee participant cannot be found" must {

      val submitterOnlyCryptoSnapshot =
        createCryptoSnapshot(Map(submitterParticipant -> Seq(submitter)))

      "be rejected" in {
        val factory = confirmationRequestFactory(Right(singleFetch.transactionTree))

        factory
          .createConfirmationRequest(
            singleFetch.wellFormedUnsuffixedTransaction,
            ConfirmationPolicy.Vip,
            submitterInfo,
            ledgerTime,
            workflowId,
            mediator,
            submitterOnlyCryptoSnapshot,
            contractInstanceOfId,
            Some(testKeySeed),
            ProtocolVersion.latestForTest,
          )
          .value
          .map(
            _ should equal(
              Left(
                EncryptedViewMessageCreationError(
                  UnableToDetermineParticipant(Set(observer), submitterOnlyCryptoSnapshot.domainId)
                )
              )
            )
          )
      }
    }

    "participants have no public keys" must {

      val noKeyCryptoSnapshot = createCryptoSnapshot(defaultTopology, keyPurposes = Set.empty)

      "be rejected" in {
        val factory = confirmationRequestFactory(Right(singleFetch.transactionTree))

        factory
          .createConfirmationRequest(
            singleFetch.wellFormedUnsuffixedTransaction,
            ConfirmationPolicy.Vip,
            submitterInfo,
            ledgerTime,
            workflowId,
            mediator,
            noKeyCryptoSnapshot,
            contractInstanceOfId,
            Some(testKeySeed),
            ProtocolVersion.latestForTest,
          )
          .value
          .map(_ should matchPattern {
            case Left(EncryptedViewMessageCreationError(UnableToDetermineKey(_, _, _))) =>
          })
      }
    }
  }
}
