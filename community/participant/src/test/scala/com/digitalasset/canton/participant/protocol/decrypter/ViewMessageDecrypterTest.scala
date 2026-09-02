// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveNumeric}
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
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.metrics.CommonMockMetrics
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
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.protocol.SynchronizerParameters.WithValidity
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithAbsoluteSuffixes,
  WithoutSuffixes,
}
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageUtils.Optics.viewHashOrHashesLens
import com.digitalasset.canton.protocol.messages.{
  EncryptedMultipleViewsMessage,
  EncryptedViewMessage,
  TransactionConfirmationRequest,
}
import com.digitalasset.canton.sequencing.protocol.*
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
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, WorkflowId}
import com.digitalasset.nonempty.{NonEmpty, NonEmptyUtil}
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.util.UUID

trait ViewMessageDecrypterTest extends BaseTestWordSpec with HasExecutionContext {

  protected class Env(
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

    private val participantId: ParticipantId = ParticipantId("participant")

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
          CommonMockMetrics.cryptoMetrics,
          timeouts,
          loggerFactory,
        )
        .value
    }

    private val pureCrypto: CryptoPureApi = jceCrypto.pureCrypto

    private val topologyMap = Map(
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

    private val fullTree: Seq[FullTransactionViewTree] = {
      val exampleTransactionFactory: ExampleTransactionFactory = new ExampleTransactionFactory(
        pureCrypto
      )()

      val fullTree =
        exampleTransactionFactory.MultipleRootsAndSimpleViewNesting.transactionViewTrees.drop(1)
      fullTree(parent).subviewHashes.loneElement shouldBe fullTree(child).viewHash

      interceptFullTree(fullTree)
    }

    private def mkRandomness(): SecureRandomness =
      pureCrypto.generateSecureRandomness(pureCrypto.defaultSymmetricKeyScheme.keySizeInBytes)

    val randomness: Seq[SecureRandomness] = Seq(mkRandomness(), mkRandomness())
    private val subviewKeyRandomness: Seq[Seq[SecureRandomness]] = interceptSubviewKeyRandomness(
      Seq(Seq(randomness(child)), Seq.empty)
    )

    private def mkViewKeyData(
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

    val defaultSynchronizerLimits: SynchronizerLimits =
      SynchronizerLimits.defaultFor(testedProtocolVersion)

    val viewKeyData: Seq[(SymmetricKey, Seq[AsymmetricEncrypted[SecureRandomness]])] =
      randomness.map(mkViewKeyData)

    var lightTree: Seq[LightTransactionViewTree] =
      allViewIndices.map(i =>
        LightTransactionViewTree
          .fromTransactionViewTreeUsingViewHashReference(
            fullTree(i),
            subviewKeyRandomness(i),
            testedProtocolVersion,
          )
          .value
      )

    val encryptedViewMessage: Seq[EncryptedViewMessage[TransactionViewType.type]] =
      interceptEncryptedViewMessages(
        if (testedProtocolVersion < ProtocolVersion.transparency)
          allViewIndices.map { i =>
            EncryptedViewMessageFactory
              .encryptView(TransactionViewType)(
                lightTree(i),
                viewKeyData(i),
                Signature.noSignature,
                snapshot,
                testedProtocolVersion,
              )
              .futureValueUS
              .value
          }
        else {
          val childLvt = lightTree(child)
          val childEnc = EncryptedViewMessageFactory
            .encryptView(TransactionViewType)(
              childLvt,
              viewKeyData(child),
              Signature.noSignature,
              snapshot,
              testedProtocolVersion,
            )
            .futureValueUS
            .value
            .asInstanceOf[EncryptedMultipleViewsMessage[TransactionViewType.type]]

          val ciphertextId = childEnc.encryptedViews.computeCiphertextId(pureCrypto)

          val parentOldLvt = lightTree(parent)
          val parentLvt = LightTransactionViewTree.tryCreate(
            parentOldLvt.tree,
            parentOldLvt.subviewReferencesAndKeys.map(subviewReferenceAndKey =>
              subviewReferenceAndKey.copy(subviewReference =
                ByCiphertextId(ciphertextId, NonNegativeInt.zero)
              )
            ),
            testedProtocolVersion,
          )

          // Update the light tree to reflect the new parent-child transactions views that are linked
          // by the ciphertext ID instead of the view hash.
          lightTree = Seq(parentLvt, childLvt)

          val parentEnc = EncryptedViewMessageFactory
            .encryptView(TransactionViewType)(
              parentLvt,
              viewKeyData(parent),
              Signature.noSignature,
              snapshot,
              testedProtocolVersion,
            )
            .futureValueUS
            .value

          Seq(parentEnc, childEnc)
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

    val decrypter: ViewMessageDecrypter = ViewMessageDecrypter(
      participantId,
      new SessionKeyStoreWithNoEviction(),
      testedProtocolVersion,
      futureSupervisor,
      loggerFactory,
    )

    def checkDecryptedViews(
        decryptedViews: DecryptedViews[LightTransactionViewTree],
        nbrViews: Long = allViewIndices.size.toLong,
    ): Assertion =
      inside(decryptedViews) { case DecryptedViews(views, decryptionErrors) =>
        views.foreach { decryptedView =>
          lightTree should contain(decryptedView.view.unwrap)
          decryptedView.view.recipients shouldBe recipients
          encryptedViewMessage.map(_.submittingParticipantSignature) should contain(
            decryptedView.signatureO
          )
        }
        views should have size nbrViews
        decryptionErrors shouldBe empty
      }

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
      private val factory = confirmationRequestFactory(Right(example.transactionTree))

      private val sessionKeyStore = new SessionKeyStoreWithInMemoryCache(
        SessionEncryptionKeyCacheConfig(),
        timeouts,
        loggerFactory,
      )

      private val maxSequencingTime: CantonTimestamp = ledgerTime.plusSeconds(10)

      val confirmationRequest: TransactionConfirmationRequest = factory
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

      val decrypterForObserver: ViewMessageDecrypter = ViewMessageDecrypter(
        observerParticipant,
        sessionKeyStore,
        testedProtocolVersion,
        futureSupervisor,
        loggerFactory,
      )
    }
  }

  protected def reportRandomnessMismatch(env: Env, dummyRandomness: SecureRandomness): Unit

  def viewMessageDecrypterTest(): Unit = {
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
          NonEmptyUtil.fromUnsafe(envelopes),
          snapshot,
          defaultSynchronizerLimits,
        )
        .futureValueUS
        .value

      decryptionResult.decryptionErrors shouldBe empty
      decryptionResult.views.length shouldBe 4

      val decryptedViewGenTrees = decryptionResult.views.map(_.view.unwrap.tree)

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

      val decryptedViews = decrypter
        .decryptViews(allEnvelopes, snapshot, defaultSynchronizerLimits)
        .futureValueUS
        .value

      env.checkDecryptedViews(decryptedViews)
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
        decrypter.decryptViews(onlyChildEnvelopes, snapshot, defaultSynchronizerLimits).value,
        _.getMessage should startWith(
          s"Can't decrypt the randomness of the message with hash(es) ${encryptedViewMessage(child).viewHashes} where I'm allegedly an informee. " +
            s"SyncCryptoDecryptError(\n  FailedToDecrypt(\n    org.bouncycastle.jcajce.provider.util.BadBlockException"
        ),
      )
    }.futureValueUS

    "fail on missing view keys" in {
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
        decrypter.decryptViews(onlyChildEnvelopes, snapshot, defaultSynchronizerLimits).value,
        _.getMessage shouldBe s"Can't decrypt the randomness of the message with hash(es) ${encryptedViewMessage(child).viewHashes} where I'm allegedly an informee. " +
          s"MissingParticipantKey(PAR::participant::default)",
      )
    }.futureValueUS

    "crash on missing private keys" in {
      // Note: If the private key is missing, the participant needs to crash to avoid a ledger fork.
      // The operator needs to upload the missing key and reconnect to the synchronizer.

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
          decrypter.decryptViews(onlyChildEnvelopes, snapshot, defaultSynchronizerLimits).value,
          _.getMessage shouldBe s"Can't decrypt the randomness of the message with hash(es) ${encryptedViewMessage(child).viewHashes} where I'm allegedly an informee. " +
            s"PrivateKeyStoreVerificationError(FailedToReadKey(keyId = $fingerprint, reason = matching private key does not exist))",
        )
        .futureValueUS
    }

    "report if the randomness of an EncryptedViewMessage does not match the randomness in the parent tree" in {
      // Note: It is desirable to keep the child view and discard the parent view in this case.
      val dummyRandomness = SecureRandomness
        .fromByteString(16)(ByteString.fromHex("DEADBEEFDEADBEEFDEADBEEFDEADBEEF"))
        .value

      // We intercept the subview key randomness listed in the parent view and replace it with dummy randomness
      // that fails when used to decrypt the child view.
      val env = new Env(interceptSubviewKeyRandomness = _ => Seq(Seq(dummyRandomness), Seq.empty))

      reportRandomnessMismatch(env, dummyRandomness)
    }

    "fail if different encrypted view messages contain the same view with different randomnesses" in {
      // Note: it would be desirable to keep the messages instead.

      val env = new Env(
        // We override the interceptFullTree method to capture the full tree and return a duplicate
        // of the same view (child view). Within `Env`, we use two different randomness values (i.e. keys)
        // for each index in the full view tree list.
        interceptFullTree = trees => Seq(trees(1), trees(1)),
        // Make sure the two views have no children.
        interceptSubviewKeyRandomness = _ => Seq(Seq.empty, Seq.empty),
      )
      import env.*

      loggerFactory.assertInternalErrorAsyncUS[IllegalArgumentException](
        decrypter.decryptViews(allEnvelopes, snapshot, defaultSynchronizerLimits).value,
        _.getMessage should include("has different encryption keys associated with it"),
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

      val decryptedViews = decrypter
        .decryptViews(onlyChildEnvelopes, snapshot, defaultSynchronizerLimits)
        .futureValueUS
        .value

      env.checkDecryptedViews(decryptedViews, 1.toLong)
    }
  }
}
