// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{
  CacheConfig,
  CryptoConfig,
  LoggingConfig,
  SessionEncryptionKeyCacheConfig,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.jce.JceCrypto
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner.SigningTimestampOverrides
import com.digitalasset.canton.crypto.store.memory.{
  InMemoryCryptoPrivateStore,
  InMemoryCryptoPublicStore,
}
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullTransactionViewTree,
  GenTransactionTree,
  LightTransactionViewTree,
  TransactionView,
  ViewPosition,
}
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.ProcessingSteps.DecryptedViews
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  ContractInstanceOfId,
  TransactionTreeConversionError,
}
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  SeedGenerator,
  TransactionConfirmationRequestFactory,
  TransactionTreeFactory,
}
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  extra,
  extraParticipant,
  observer,
  observerParticipant,
  signatory,
  signatoryParticipant,
  submitter,
  submittingParticipant,
}
import com.digitalasset.canton.protocol.SynchronizerParameters.WithValidity
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithAbsoluteSuffixes,
  WithoutSuffixes,
}
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageUtils.Optics.viewHashOrHashesLens
import com.digitalasset.canton.protocol.{
  CantonContractIdVersion,
  ContractIdAbsolutizer,
  DynamicSynchronizerParameters,
  ExampleTransactionFactory,
  RollbackContext,
  ViewHash,
  WellFormedTransaction,
}
import com.digitalasset.canton.sequencing.protocol.{
  MediatorGroupRecipient,
  MemberRecipient,
  OpenEnvelope,
  Recipients,
  WithRecipients,
}
import com.digitalasset.canton.store.{
  SessionKeyStoreWithInMemoryCache,
  SessionKeyStoreWithNoEviction,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import com.digitalasset.canton.topology.{ParticipantId, TestingIdentityFactory, TestingTopology}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTestWordSpec,
  HasExecutionContext,
  LfGlobalKeyMapping,
  WorkflowId,
}
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*

import java.util.UUID

class ViewMessageDecrypterTest extends BaseTestWordSpec with HasExecutionContext {

  private class Env(
      interceptEncryptedViewKeys: Seq[AsymmetricEncrypted[SecureRandomness]] => Seq[
        AsymmetricEncrypted[SecureRandomness]
      ] = identity,
      interceptSubviewKeyRandomness: Seq[Seq[SecureRandomness]] => Seq[Seq[SecureRandomness]] =
        identity,
      interceptEncryptedViewMessages: Seq[
        EncryptedViewMessage[TransactionViewType.type]
      ] => Seq[
        EncryptedViewMessage[TransactionViewType.type]
      ] = identity,
      interceptFullTree: Seq[FullTransactionViewTree] => Seq[FullTransactionViewTree] = identity,
  ) {

    val participantId: ParticipantId = ParticipantId("participant")

    val jceCrypto: Crypto = {
      val config = CryptoConfig()
      JceCrypto
        .create(
          config,
          CryptoSchemes.tryFromConfig(config),
          SessionEncryptionKeyCacheConfig(),
          CacheConfig(PositiveNumeric.tryCreate(1)),
          new InMemoryCryptoPrivateStore(testedReleaseProtocolVersion, loggerFactory),
          new InMemoryCryptoPublicStore(loggerFactory),
          timeouts,
          loggerFactory,
        )
        .value
    }

    val pureCrypto: CryptoPureApi = jceCrypto.pureCrypto

    val topologyMap = Map(
      submittingParticipant -> Map(submitter -> Submission),
      signatoryParticipant -> Map(signatory -> Submission),
      observerParticipant -> Map(observer -> Observation),
      extraParticipant -> Map(extra -> Observation),
    )

    val snapshot: SynchronizerSnapshotSyncCryptoApi = {
      val identityFactory: TestingIdentityFactory = new TestingIdentityFactory(
        TestingTopology(participants = Map(participantId -> ParticipantAttributes(Submission)))
          .withReversedTopology(topologyMap),
        jceCrypto,
        loggerFactory,
        List(
          WithValidity(
            CantonTimestamp.MinValue,
            None,
            DynamicSynchronizerParameters.defaultValues(testedProtocolVersion),
          )
        ),
      )

      identityFactory
        .forOwnerAndSynchronizer(participantId)
        .currentSnapshotApproximation
        .futureValueUS
    }

    val parent: Int = 0
    val child: Int = 1
    val allViewIndices: Seq[Int] = Seq(parent, child)

    val fullTree: Seq[FullTransactionViewTree] = {
      val exampleTransactionFactory: ExampleTransactionFactory = new ExampleTransactionFactory(
        pureCrypto
      )()

      val fullTree =
        exampleTransactionFactory.MultipleRootsAndSimpleViewNesting.transactionViewTrees.drop(1)
      fullTree(parent).subviewHashes.loneElement shouldBe fullTree(child).viewHash

      interceptFullTree(fullTree)
    }

    def mkRandomness(): SecureRandomness =
      pureCrypto.generateSecureRandomness(pureCrypto.defaultSymmetricKeyScheme.keySizeInBytes)

    val randomness: Seq[SecureRandomness] = Seq(mkRandomness(), mkRandomness())
    val subviewKeyRandomness: Seq[Seq[SecureRandomness]] = interceptSubviewKeyRandomness(
      Seq(Seq(randomness(child)), Seq.empty)
    )

    def mkViewKeyData(
        viewKeyRandomness: SecureRandomness
    ): (SymmetricKey, Seq[AsymmetricEncrypted[SecureRandomness]]) = {
      val viewKey: SymmetricKey = pureCrypto.createSymmetricKey(viewKeyRandomness).value
      val encryptedViewKeys: Seq[AsymmetricEncrypted[SecureRandomness]] =
        snapshot
          .encryptFor(viewKeyRandomness, Seq(participantId))
          .futureValueUS
          .value
          .values
          .toSeq
      val modifiedEncryptedViewKeys = interceptEncryptedViewKeys(encryptedViewKeys)
      (viewKey, modifiedEncryptedViewKeys)
    }

    val viewKeyData: Seq[(SymmetricKey, Seq[AsymmetricEncrypted[SecureRandomness]])] =
      randomness.map(mkViewKeyData)

    val lightTree: Seq[LightTransactionViewTree] = allViewIndices.map(i =>
      LightTransactionViewTree
        .fromTransactionViewTree(fullTree(i), subviewKeyRandomness(i), testedProtocolVersion)
        .value
    )

    val encryptedViewMessage: Seq[EncryptedViewMessage[TransactionViewType.type]] =
      interceptEncryptedViewMessages(
        allViewIndices.map { i =>
          EncryptedViewMessageFactory
            .encryptView(TransactionViewType)(
              lightTree(i),
              viewKeyData(i),
              snapshot,
              None,
              testedProtocolVersion,
            )
            .futureValueUS
            .value
        }
      )

    val recipients: Recipients = Recipients.cc(participantId)
    val allEnvelopes: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType.type]]]] =
      NonEmpty
        .from(encryptedViewMessage.map(OpenEnvelope(_, recipients)(testedProtocolVersion)))
        .value
    val onlyChildEnvelopes
        : NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType.type]]]] =
      NonEmpty(Seq, allEnvelopes(child))

    val decrypter: ViewMessageDecrypter = new ViewMessageDecrypter(
      participantId,
      testedProtocolVersion,
      new SessionKeyStoreWithNoEviction(),
      snapshot,
      futureSupervisor,
      loggerFactory,
    )

    /** Dataset used for a scenario with multiple messages per envelope.
      *
      * Uses the envelopes from the actual envelopes from the confirmation request
      */
    object MultipleMessagesPerEnvelope {

      private val randomOps: RandomOps = new SymbolicPureCrypto()

      private val transactionUuid: UUID = new UUID(10L, 20L)

      private val seedGenerator: SeedGenerator =
        new SeedGenerator(randomOps) {
          override def generateUuid(): UUID = transactionUuid
        }

      private val ledgerTime: CantonTimestamp = CantonTimestamp.Epoch

      private val transactionFactory: ExampleTransactionFactory =
        new ExampleTransactionFactory()(ledgerTime = ledgerTime)

      private def confirmationRequestFactory(
          transactionTreeFactoryResult: Either[TransactionTreeConversionError, GenTransactionTree]
      ): TransactionConfirmationRequestFactory = {

        val transactionTreeFactory: TransactionTreeFactory = new TransactionTreeFactory {
          override def cantonContractIdVersion: CantonContractIdVersion =
            transactionFactory.cantonContractIdVersion

          override def createTransactionTree(
              transaction: WellFormedTransaction[WithoutSuffixes],
              submitterInfo: SubmitterInfo,
              _workflowId: Option[WorkflowId],
              _mediator: MediatorGroupRecipient,
              transactionSeed: SaltSeed,
              transactionUuid: UUID,
              _topologySnapshot: TopologySnapshot,
              _contractOfId: ContractInstanceOfId,
              _keyResolver: LfGlobalKeyMapping,
              _maxSequencingTime: CantonTimestamp,
              validatePackageVettings: Boolean,
          )(implicit
              traceContext: TraceContext
          ): EitherT[FutureUnlessShutdown, TransactionTreeConversionError, GenTransactionTree] =
            transactionTreeFactoryResult.toEitherT

          override def tryReconstruct(
              subaction: WellFormedTransaction[WithoutSuffixes],
              rootPosition: ViewPosition,
              mediator: MediatorGroupRecipient,
              submittingParticipantO: Option[ParticipantId],
              salts: Iterable[Salt],
              transactionUuid: UUID,
              topologySnapshot: TopologySnapshot,
              contractOfId: ContractInstanceOfId,
              _rbContext: RollbackContext,
              _keyResolver: LfGlobalKeyMapping,
              _absolutizer: ContractIdAbsolutizer,
          )(implicit traceContext: TraceContext): EitherT[
            FutureUnlessShutdown,
            TransactionTreeConversionError,
            (TransactionView, WellFormedTransaction[WithAbsoluteSuffixes]),
          ] = ???

          override def saltsFromView(view: TransactionView): Iterable[Salt] = ???
        }

        // we force view requests to be handled sequentially, which makes results deterministic and easier to compare
        // in the end.
        new TransactionConfirmationRequestFactory(
          submittingParticipant,
          LoggingConfig(),
          loggerFactory,
          parallel = false,
        )(
          transactionTreeFactory,
          seedGenerator,
        )(executorService)
      }

      val exampleTransactionFactory: ExampleTransactionFactory = new ExampleTransactionFactory(
        pureCrypto
      )()

      val example = exampleTransactionFactory.ViewInterleavings
      val factory = confirmationRequestFactory(Right(example.transactionTree))

      val sessionKeyStore = new SessionKeyStoreWithInMemoryCache(
        SessionEncryptionKeyCacheConfig(),
        timeouts,
        loggerFactory,
      )

      val maxSequencingTime: CantonTimestamp = ledgerTime.plusSeconds(10)

      val confirmationRequest = factory
        .createConfirmationRequest(
          transactionTree = example.transactionTree,
          cryptoSnapshot = snapshot,
          signingTimestampOverrides = Some(
            SigningTimestampOverrides(
              wallClock.now,
              Some(maxSequencingTime),
            )
          ),
          sessionKeyStore = sessionKeyStore,
          protocolVersion = testedProtocolVersion,
        )
        .futureValueUS
        .value

      val decrypterForObserver: ViewMessageDecrypter = new ViewMessageDecrypter(
        observerParticipant,
        testedProtocolVersion,
        sessionKeyStore,
        snapshot,
        futureSupervisor,
        loggerFactory,
      )
    }
  }

  "ViewMessageDecrypter" can {

    "successfully decrypt all view messages from envelopes with multiple views" in {
      val env = new Env()
      import env.*

      import MultipleMessagesPerEnvelope.{confirmationRequest, decrypterForObserver, example}

      val envelopes = confirmationRequest.viewEnvelopes.filter(
        _.recipients.allRecipients.contains(MemberRecipient(observerParticipant))
      )

      if (testedProtocolVersion >= ProtocolVersion.v35) {
        envelopes.length shouldBe 2
      } else {
        envelopes.length shouldBe 4
      }

      val decryptionResult = decrypterForObserver
        .decryptViews(
          NonEmptyUtil.fromUnsafe(envelopes)
        )
        .futureValueUS
        .value

      decryptionResult.decryptionErrors shouldBe empty
      decryptionResult.views.length shouldBe 4

      val decryptedViewGenTrees = decryptionResult.views.map(_._1.unwrap.tree)

      decryptedViewGenTrees should contain theSameElementsAs Seq(
        example.transactionViewTree0.tree,
        example.transactionViewTree100.tree,
        example.transactionViewTree110.tree,
        example.transactionViewTree2.tree,
      )
    }

    "successfully decrypt all view messages" in {

      val env = new Env()
      import env.*

      val decryptedViews = decrypter.decryptViews(allEnvelopes).futureValueUS.value
      inside(decryptedViews) { case DecryptedViews(views, decryptionErrors) =>
        forEvery(views.zipWithIndex) {
          case ((WithRecipients(view, actualRecipients), optSignature), i) =>
            view shouldBe lightTree(i)
            actualRecipients shouldBe recipients
            optSignature shouldBe encryptedViewMessage(
              i
            ).submittingParticipantSignature
        }
        views should have size allViewIndices.size.toLong

        decryptionErrors shouldBe empty
      }
    }

    "fail on decryption errors" in {
      // Note: it would be desirable to filter out envelopes with decryption errors instead of failing.

      val env = new Env(
        interceptEncryptedViewKeys = _.map(encryptedKey =>
          encryptedKey
            .focus(_.ciphertext)
            .replace(ByteString.fromHex("DEADBEEFDEADBEEFDEADBEEFDEADBEEF"))
        )
      )
      import env.*

      loggerFactory.assertInternalErrorAsyncUS[IllegalArgumentException](
        decrypter.decryptViews(onlyChildEnvelopes).value,
        _.getMessage should startWith(
          s"Can't decrypt the randomness of the message with hash(es) ${encryptedViewMessage(child).viewHashes} where I'm allegedly an informee. " +
            s"SyncCryptoDecryptError(\n  FailedToDecrypt(\n    org.bouncycastle.jcajce.provider.util.BadBlockException"
        ),
      )
    }.futureValueUS

    "fail on with missing view keys" in {
      // Note: It would be desirable to filter out envelopes that use unknown keys (according to the topology state)

      val env = new Env(
        interceptEncryptedViewKeys = _.map(encryptedKey =>
          encryptedKey
            .focus(_.encryptedFor)
            .replace(Fingerprint.tryFromString("Nudelsuppe"))
        )
      )
      import env.*

      loggerFactory.assertInternalErrorAsyncUS[IllegalArgumentException](
        decrypter.decryptViews(onlyChildEnvelopes).value,
        _.getMessage shouldBe s"Can't decrypt the randomness of the message with hash(es) ${encryptedViewMessage(child).viewHashes} where I'm allegedly an informee. " +
          s"MissingParticipantKey(PAR::participant::default)",
      )
    }.futureValueUS

    "crash on missing private keys" in {
      // Note: If the private key is missing, the participant needs to crash to avoid a ledger fork.
      //  The operator needs to upload the missing key and reconnect to the synchronizer.

      val env = new Env()
      import env.*

      val (_, encryptedViewKeys) = viewKeyData(child)
      // Remove the private key from the store
      val fingerprint = encryptedViewKeys.loneElement.encryptedFor
      jceCrypto.cryptoPrivateStore
        .existsPrivateKey(fingerprint, KeyPurpose.Encryption)
        .futureValueUS shouldBe Right(true)
      jceCrypto.cryptoPrivateStore.removePrivateKey(fingerprint).futureValueUS.value

      loggerFactory
        .assertInternalErrorAsyncUS[IllegalArgumentException](
          decrypter.decryptViews(onlyChildEnvelopes).value,
          _.getMessage shouldBe s"Can't decrypt the randomness of the message with hash(es) ${encryptedViewMessage(child).viewHashes} where I'm allegedly an informee. " +
            s"PrivateKeyStoreVerificationError(FailedToReadKey(keyId = $fingerprint, reason = matching private key does not exist))",
        )
        .futureValueUS
    }

    "fail if the randomness of an EncryptedViewMessage does not match the randomness in the parent tree" in {
      // Note: It is desirable to keep the child view and discard the parent view in this case.

      val dummyRandomness = SecureRandomness
        .fromByteString(16)(ByteString.fromHex("DEADBEEFDEADBEEFDEADBEEFDEADBEEF"))
        .value
      val env = new Env(interceptSubviewKeyRandomness = _ => Seq(Seq(dummyRandomness), Seq.empty))
      import env.*

      loggerFactory
        .assertInternalErrorAsyncUS[IllegalArgumentException](
          decrypter.decryptViews(allEnvelopes).value,
          _.getMessage shouldBe s"View ${encryptedViewMessage(child).viewHashes.head1} has different encryption keys associated with it. " +
            s"(Previous: Some(Success(Outcome(${randomness(child)}))), new: $dummyRandomness)",
        )
        .futureValueUS
    }

    "fail if different encrypted view messages contain the same view with different randomnesses" in {
      // Note: it would be desirable to keep the messages instead.

      val env = new Env(
        interceptFullTree = trees => Seq(trees(1), trees(1)),
        interceptSubviewKeyRandomness = _ => Seq(Seq.empty, Seq.empty),
      )
      import env.*

      loggerFactory.assertInternalErrorAsyncUS[IllegalArgumentException](
        decrypter.decryptViews(allEnvelopes).value,
        { x =>
          val randomnesses = (randomness(parent), randomness(child))
          Seq(randomnesses, randomnesses.swap).map { case (r1, r2) =>
            s"View ${encryptedViewMessage(child).viewHashes.head1} has different encryption keys associated with it. " +
              s"(Previous: Some(Success(Outcome($r1))), new: $r2)"
          } should contain(x.getMessage)
        },
      )
    }.futureValueUS

    "successfully decrypt even if the view hash of an EncryptedViewMessage does not match the view hash of the contained tree" in {
      // Note: It is desirable to discard the envelope instead.

      val dummyViewHash = ViewHash(
        Hash.digest(
          HashPurpose.MerkleTreeInnerNode,
          ByteString.fromHex("DEADBEEF"),
          HashAlgorithm.Sha256,
        )
      )

      val env = new Env(
        interceptEncryptedViewMessages = _.map { message =>
          viewHashOrHashesLens[TransactionViewType].replace(dummyViewHash)(message)
        }
      )
      import env.*

      val decryptedViews = decrypter.decryptViews(onlyChildEnvelopes).futureValueUS.value

      inside(decryptedViews) { case DecryptedViews(views, decryptionErrors) =>
        val (WithRecipients(view, outputRecipients), optSignature) = views.loneElement
        view shouldBe lightTree(child)
        outputRecipients shouldBe recipients
        optSignature shouldBe encryptedViewMessage(
          child
        ).submittingParticipantSignature
        decryptionErrors shouldBe empty
      }
    }
  }
}
