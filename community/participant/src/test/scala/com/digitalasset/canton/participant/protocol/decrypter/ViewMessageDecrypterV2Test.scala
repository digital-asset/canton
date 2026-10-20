// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{
  AsymmetricEncrypted,
  CryptoPureApi,
  Encrypted,
  Fingerprint,
  SecureRandomness,
  Signature,
  SymmetricKey,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.{
  ByCiphertextId,
  FullTransactionViewTree,
  LightTransactionViewTree,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.messages.{EncryptedMultipleViews, EncryptedViewMessage}
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, ViewHash}
import com.digitalasset.canton.sequencing.protocol.{MemberRecipient, OpenEnvelope, Recipients}
import com.digitalasset.canton.topology.client.{ForwardingTopologySnapshot, TopologySnapshotLoader}
import com.digitalasset.canton.topology.{KeyCollection, Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MaxBytesToDecompress
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.{NonEmpty, NonEmptyUtil}
import com.google.protobuf.ByteString
import monocle.Monocle.toAppliedFocusOps

import scala.concurrent.ExecutionContext

class ViewMessageDecrypterV2Test extends BaseTestWordSpec with ViewMessageDecrypterTest {

  private def mkRandomness(pureCrypto: CryptoPureApi): SecureRandomness =
    pureCrypto.generateSecureRandomness(pureCrypto.defaultSymmetricKeyScheme.keySizeInBytes)

  private def mkViewKeyData(
      viewKeyRandomness: SecureRandomness,
      pureCrypto: CryptoPureApi,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
  ): (SymmetricKey, Seq[AsymmetricEncrypted[SecureRandomness]]) = {
    val viewKey = pureCrypto
      .createSymmetricKey(viewKeyRandomness)
      .valueOrFail(
        "failed to derive symmetric view key"
      )
    val encryptedViewKeys = snapshot
      // although views can have multiple participants in the view tree,
      // we only encrypt for a single participant since we only want to check if the decrypter
      // correctly propagates failures from descendants to ancestors.
      .encryptFor(
        viewKeyRandomness,
        Seq(ParticipantId("participant")),
        deterministicEncryption =
          snapshot.crypto.staticSynchronizerParameters.enableTransparencyChecks,
      )
      .futureValueUS
      .valueOrFail("failed to encrypt view randomness")
      .values
      .toSeq
    (viewKey, encryptedViewKeys)
  }

  private def encrypt(
      lightViewTree: LightTransactionViewTree,
      randomness: SecureRandomness,
      pureCrypto: CryptoPureApi,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
  ): EncryptedViewMessage[TransactionViewType.type] =
    EncryptedViewMessageFactory
      .encryptView(TransactionViewType)(
        lightViewTree,
        mkViewKeyData(randomness, pureCrypto, snapshot),
        Signature.noSignature,
        snapshot,
        testedProtocolVersion,
      )
      .futureValueUS
      .valueOrFail("failed to encrypt test view")

  private def buildEncryptedLeafViewMetadata(
      viewTree: FullTransactionViewTree,
      pureCrypto: CryptoPureApi,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
  ): ViewMetadata = {
    val randomness = mkRandomness(pureCrypto)
    val lvt = LightTransactionViewTree
      .fromTransactionViewTreeUsingCiphertextIdReference(
        viewTree,
        Seq.empty,
        Map.empty,
        testedProtocolVersion,
      )
      .valueOrFail("failed to create sibling light view")
    val enc = encrypt(lvt, randomness, pureCrypto, snapshot)
    val ciphertextId = enc.encryptedViews.computeCiphertextId(pureCrypto)
    ViewMetadata(
      viewTree.viewHash,
      randomness,
      enc,
      ByCiphertextId(ciphertextId, NonNegativeInt.zero),
    )
  }

  private case class ViewMetadata(
      viewHash: ViewHash,
      secureRandomness: SecureRandomness,
      ciphertext: EncryptedViewMessage[TransactionViewType.type],
      ciphertextId: ByCiphertextId,
  )

  private def buildNestedViewEnvelopes(
      parent1ViewTree: FullTransactionViewTree,
      child10ViewTreeOrMetadata: Either[
        FullTransactionViewTree,
        ViewMetadata,
      ],
      child11ViewTree: FullTransactionViewTree,
      child110ViewTreeOrMetadata: Either[
        FullTransactionViewTree,
        ViewMetadata,
      ],
      recipients: Recipients,
      pureCrypto: CryptoPureApi,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
  ) = {

    val parent1Randomness = mkRandomness(pureCrypto)
    val child11Randomness = mkRandomness(pureCrypto)

    val ViewMetadata(child10ViewHash, child10Randomness, child10Enc, child10CiphertextId) =
      child10ViewTreeOrMetadata.fold(
        child10ViewTree => buildEncryptedLeafViewMetadata(child10ViewTree, pureCrypto, snapshot),
        identity,
      )

    val ViewMetadata(
      grandchild110ViewHash,
      grandchild110Randomness,
      grandchild110Enc,
      grandchild110CiphertextId,
    ) =
      child110ViewTreeOrMetadata.fold(
        child110ViewTree => buildEncryptedLeafViewMetadata(child110ViewTree, pureCrypto, snapshot),
        identity,
      )

    val child11Lvt = LightTransactionViewTree
      .fromTransactionViewTreeUsingCiphertextIdReference(
        child11ViewTree,
        Seq(grandchild110Randomness),
        Map(grandchild110ViewHash -> grandchild110CiphertextId),
        testedProtocolVersion,
      )
      .valueOrFail("failed to create cached child light view")
    val child11Enc = encrypt(child11Lvt, child11Randomness, pureCrypto, snapshot)
    val child11CiphertextId = child11Enc.encryptedViews.computeCiphertextId(pureCrypto)

    val parentLvt = LightTransactionViewTree
      .fromTransactionViewTreeUsingCiphertextIdReference(
        parent1ViewTree,
        Seq(child10Randomness, child11Randomness),
        Map(
          child10ViewHash -> child10CiphertextId,
          child11ViewTree.viewHash -> ByCiphertextId(
            child11CiphertextId,
            NonNegativeInt.zero,
          ),
        ),
        testedProtocolVersion,
      )
      .valueOrFail("failed to create parent light view")
    val parentEnc = encrypt(parentLvt, parent1Randomness, pureCrypto, snapshot)

    NonEmptyUtil.fromUnsafe(
      Seq(parentEnc, child10Enc, child11Enc, grandchild110Enc).map(
        OpenEnvelope(_, recipients)(testedProtocolVersion)
      )
    )
  }

  private def genericDecrypterTests(enableTransparency: Boolean): Unit = {
    behave like viewMessageDecrypterTest(enableTransparency = enableTransparency)

    // In V1, we used the listed view hashes to identify the view to decrypt and assumed that each
    // view hash was unique. In V2, we use the ciphertext ID to identify the view to decrypt,
    // which can be computed from the ciphertext itself. This allows us to successfully decrypt
    // even if different view messages list the same or incorrect view hashes.
    "successfully decrypt even if different view messages list the same view hash" in {
      val env = new Env(
        interceptEncryptedViewMessages = { encryptedViewMessages =>
          val sharedViewHash = encryptedViewMessages.head.viewHashes.head1
          // Force all encrypted view messages to use the same view hash
          // to verify that decryption does not rely on view-hash uniqueness.
          encryptedViewMessages.map {
            _.copy(viewHashes = NonEmptyUtil.fromUnsafe(Seq(sharedViewHash)))
          }
        },
        enableTransparency = enableTransparency,
      )

      val decryptedViews = env.decrypter
        .decryptViews(env.allEnvelopes, env.snapshot, env.defaultSynchronizerLimits)
        .futureValueUS
        .value

      env.checkDecryptedViews(decryptedViews)
    }

    "successfully decrypt if the same envelope is duplicated" in {
      val env = new Env(enableTransparency = enableTransparency)
      import env.*

      val decryptedViews = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        decrypter
          .decryptViews(
            onlyChildEnvelopes ++ onlyChildEnvelopes,
            snapshot,
            defaultSynchronizerLimits,
          )
          .futureValueUS
          .value,
        entries =>
          LogEntry.assertLogSeq(
            Seq(
              (
                entry => {
                  entry.shouldBeCantonErrorCode(SyncServiceAlarm)
                  entry.warningMessage should include("Discarding duplicate envelope")
                },
                "Message should contain a warning about discarding duplicate envelope",
              )
            )
          )(entries),
      )
      env.checkDecryptedViews(decryptedViews, nbrViews = 1)
    }

    "fail if different envelopes share the same ciphertext" in {
      val env = new Env(enableTransparency = enableTransparency)
      import env.*

      val originalChildMessage = encryptedViewMessage(child)
      // Keep the same encrypted payload (same ciphertext ID), but change the envelope metadata.
      val differentRecipients = Recipients.cc(ParticipantId("another-participant"))
      val modifiedEnvelope =
        OpenEnvelope(originalChildMessage, differentRecipients)(testedProtocolVersion)

      loggerFactory
        .assertInternalError[IllegalArgumentException](
          decrypter
            .decryptViews(
              onlyChildEnvelopes ++ Seq(modifiedEnvelope),
              snapshot,
              defaultSynchronizerLimits,
            )
            .futureValueUS,
          _.getMessage should include("Different envelope with the same ciphertextID"),
        )
    }

    // Tree shape used below:
    //
    //          Node(1)
    //         /       \
    //     Node(10)  Node(11)
    //                  |
    //               Node(110)
    //
    // The test makes 110 invalid, so the failure should invalidate 11 and propagate to 1,
    // while sibling 10 still decrypts.
    "propagate descendant failures" in {
      implicit val traceContext: TraceContext = TraceContext.empty

      val env = new Env(enableTransparency = enableTransparency)
      import env.*

      val pureCrypto = jceCrypto.pureCrypto
      val example = new ExampleTransactionFactory(pureCrypto)().MultipleRootsAndViewNestings

      val grandchild110Randomness = mkRandomness(pureCrypto)
      val grandchildLvt = LightTransactionViewTree
        .fromTransactionViewTreeUsingCiphertextIdReference(
          example.transactionViewTree110,
          Seq.empty,
          Map.empty,
          testedProtocolVersion,
        )
        .valueOrFail("failed to create grandchild light view")
      val grandchild110InvalidEnc =
        encrypt(grandchildLvt, grandchild110Randomness, pureCrypto, snapshot)
          .focus(_.encryptedViews.viewTrees)
          .replace(
            Encrypted.fromByteString(ByteString.copyFromUtf8("invalid ciphertext"))
          )

      val allEnvelopes = buildNestedViewEnvelopes(
        example.transactionViewTree1,
        Left(example.transactionViewTree10),
        example.transactionViewTree11,
        Right(
          ViewMetadata(
            example.transactionViewTree110.viewHash,
            grandchild110Randomness,
            grandchild110InvalidEnc,
            ByCiphertextId(
              grandchild110InvalidEnc.encryptedViews.computeCiphertextId(
                pureCrypto
              ),
              NonNegativeInt.zero,
            ),
          )
        ),
        recipients,
        pureCrypto,
        snapshot,
      )

      val decryptedViews = decrypter
        .decryptViews(allEnvelopes, snapshot, defaultSynchronizerLimits)
        .futureValueUS
        .valueOrFail("failed transaction")

      val decryptedViewHashes = decryptedViews.views.map(_.view.unwrap.viewHash).toSet
      decryptedViewHashes shouldBe Set(example.transactionViewTree10.viewHash)
      decryptedViewHashes should not contain example.transactionViewTree110.viewHash
      decryptedViewHashes should not contain example.transactionViewTree11.viewHash
      decryptedViewHashes should not contain example.transactionViewTree1.viewHash

      val errors = decryptedViews.decryptionErrors.map(_.show)
      errors.exists(
        _.contains(
          "SymmetricDecryptError(FailedToDecrypt"
        )
      ) shouldBe true
      errors.exists(
        _.contains(
          s"Parent view ${example.transactionViewTree11.viewHash} is invalid because a subview is invalid"
        )
      ) shouldBe true
      errors.exists(
        _.contains(
          s"Parent view ${example.transactionViewTree1.viewHash} is invalid because a subview is invalid"
        )
      ) shouldBe true
    }

    // Tree shape used below:
    //
    //  Node(0)        Node(1)
    //                /       \
    //            Node(10)  Node(11)
    //                        |
    //                      Node(110)
    //
    // The test encrypts the views 0 and 110 into a single grouped view message.
    "decrypt grouped encrypted subviews message with multiple views" in {
      implicit val traceContext: TraceContext = TraceContext.empty

      val env = new Env(enableTransparency = false)
      import env.*

      val pureCrypto = jceCrypto.pureCrypto
      val example = new ExampleTransactionFactory(pureCrypto)().MultipleRootsAndViewNestings

      val grandchild110Lvt = LightTransactionViewTree
        .fromTransactionViewTreeUsingCiphertextIdReference(
          example.transactionViewTree110,
          Seq.empty,
          Map.empty,
          testedProtocolVersion,
        )
        .valueOrFail("failed to create grandchild light view")
      val rootOLvt = LightTransactionViewTree
        .fromTransactionViewTreeUsingCiphertextIdReference(
          example.transactionViewTree0,
          Seq.empty,
          Map.empty,
          testedProtocolVersion,
        )
        .valueOrFail("failed to create sibling grouped light view")

      val groupRandomness = mkRandomness(pureCrypto)
      val (groupKey, groupEncryptedRandomness) =
        mkViewKeyData(groupRandomness, pureCrypto, snapshot)
      val groupEncryptedViews =
        EncryptedMultipleViews
          .compressAndEncryptViews(
            pureCrypto,
            groupKey,
            TransactionViewType,
            testedProtocolVersion,
          )(
            NonEmpty(Seq, rootOLvt, grandchild110Lvt),
            MaxBytesToDecompress.HardcodedDefault,
          )
          .valueOrFail("failed to encrypt grouped views")
      val groupEncMessage = EncryptedViewMessage(
        groupEncryptedViews,
        NonEmpty(Seq, rootOLvt.viewHash, grandchild110Lvt.viewHash),
        NonEmptyUtil.fromUnsafe(groupEncryptedRandomness),
        grandchild110Lvt.psid,
        pureCrypto.defaultSymmetricKeyScheme,
        Some(Signature.noSignature),
        testedProtocolVersion,
      )
      val allEnvelopes = buildNestedViewEnvelopes(
        example.transactionViewTree1,
        Left(example.transactionViewTree10),
        example.transactionViewTree11,
        Right(
          ViewMetadata(
            example.transactionViewTree110.viewHash,
            groupRandomness,
            groupEncMessage,
            ByCiphertextId(
              groupEncMessage.encryptedViews.computeCiphertextId(pureCrypto),
              // the grouped view message contains two views
              // and the grandchild110 view is the second position, so its index is 1
              NonNegativeInt.one,
            ),
          )
        ),
        recipients,
        pureCrypto,
        snapshot,
      )

      val decryptedViews = decrypter
        .decryptViews(allEnvelopes, snapshot, defaultSynchronizerLimits)
        .futureValueUS
        .valueOrFail("failed transaction")

      val decryptedViewHashes = decryptedViews.views.map(_.view.unwrap.viewHash).toSet

      decryptedViewHashes shouldBe Set(
        example.transactionViewTree0.viewHash,
        example.transactionViewTree1.viewHash,
        example.transactionViewTree10.viewHash,
        example.transactionViewTree11.viewHash,
        example.transactionViewTree110.viewHash,
      )
      decryptedViews.decryptionErrors shouldBe empty
    }
  }

  "A ViewMessageDecrypter version 2 (using ciphertext ID)" must {
    if (testedProtocolVersion >= ProtocolVersion.transparencyAndCiphertextId) {

      "with transparency checks disabled" should {
        genericDecrypterTests(enableTransparency = false)

        "decrypt successfully with malformed extra entries if transparency checks are disabled" in {
          val bogusRecipientKey = Fingerprint.tryFromString("Nudelsuppe")
          val bogusCiphertext = ByteString.fromHex("DEADBEEFDEADBEEFDEADBEEFDEADBEEF")

          val env = new Env(
            interceptEncryptedViewKeys = viewKeys =>
              viewKeys ++ Seq(
                viewKeys.head.copy[SecureRandomness](encryptedFor = bogusRecipientKey),
                viewKeys.head.copy[SecureRandomness](ciphertext = bogusCiphertext),
              ),
            enableTransparency = false,
          )
          import env.*

          val decryptedViews =
            decrypter
              .decryptViews(onlyChildEnvelopes, snapshot, defaultSynchronizerLimits)
              .futureValueUS
              .valueOrFail("failed transaction")

          checkDecryptedViews(decryptedViews, nbrViews = 1L)
        }
      }

      "with transparency checks enabled" should {
        genericDecrypterTests(enableTransparency = true)

        "fail transparency checks if key is nonexistent " in {
          val bogusRecipientKey = Fingerprint.tryFromString("Nudelsuppe")

          val env = new Env(
            interceptEncryptedViewKeys = viewKeys =>
              viewKeys :+ viewKeys.head.copy[SecureRandomness](
                encryptedFor = bogusRecipientKey
              ),
            enableTransparency = true,
          )
          import env.*

          val decryptedViews =
            decrypter
              .decryptViews(onlyChildEnvelopes, snapshot, defaultSynchronizerLimits)
              .futureValueUS
              .valueOrFail("failed transaction")
          val errors = decryptedViews.decryptionErrors.map(_.show)
          val viewContext =
            s"for message with hash(es) ${onlyChildEnvelopes.head1.protocolMessage.viewHashes.head1}"

          decryptedViews.views shouldBe empty
          errors.exists(
            _.contains(
              s"No encryption key found for recipient key ID $bogusRecipientKey $viewContext"
            )
          ) shouldBe true
        }

        "fail transparency checks if ciphertext does not match" in {
          val bogusCiphertext = ByteString.fromHex("DEADBEEFDEADBEEFDEADBEEFDEADBEEF")

          val env = new Env(
            interceptEncryptedViewKeys = viewKeys =>
              viewKeys :+ viewKeys.head.copy[SecureRandomness](
                ciphertext = bogusCiphertext
              ),
            enableTransparency = true,
          )
          import env.*

          val decryptedViews =
            decrypter
              .decryptViews(onlyChildEnvelopes, snapshot, defaultSynchronizerLimits)
              .futureValueUS
              .valueOrFail("failed transaction")
          val errors = decryptedViews.decryptionErrors.map(_.show)
          val viewContext =
            s"for message with hash(es) ${onlyChildEnvelopes.head1.protocolMessage.viewHashes.head1}"

          decryptedViews.views shouldBe empty
          errors.exists(
            _.contains(
              s"Re-encrypted ciphertext does not match the view ciphertext for recipient key ID ${onlyChildEnvelopes.head1.protocolMessage.viewEncryptionKeyRandomness.head1.encryptedFor} $viewContext"
            )
          ) shouldBe true
        }

        /* We add another valid key for an existing informee participant and generate a new topology snapshot
         * that includes the additional key. We then add another valid encrypted randomness to the message
         * encrypted with the newly added key. Transparency checks will re-encrypt the view
         * randomness for both keys and accept the message since the ciphertexts match.
         */
        "pass transparency checks with multiple valid encrypted randomness entries" in {
          implicit val traceContext: TraceContext = TraceContext.empty

          val env = new Env(enableTransparency = true)
          import env.*

          val childEnvelope = onlyChildEnvelopes.head1

          val participantRecipient = childEnvelope.recipients.allRecipients.collectFirst {
            case MemberRecipient(member) => member
          }.value

          val extraEncryptionKey = jceCrypto
            .generateEncryptionKey()
            .futureValueUS
            .valueOrFail("failed to generate additional encryption key")

          val baseTopologySnapshot = snapshot.ipsSnapshot.asInstanceOf[TopologySnapshotLoader]

          // Create a new topology snapshot that includes an additional encryption key for the participant recipient
          val topologyWithExtraKey =
            new ForwardingTopologySnapshot(
              baseTopologySnapshot.timestamp,
              baseTopologySnapshot,
              loggerFactory,
            ) {
              private implicit val ec: ExecutionContext = executorService

              private def appendKeyIfRecipient(owner: Member, keys: KeyCollection) =
                if (owner == participantRecipient)
                  keys.copy(encryptionKeys = keys.encryptionKeys :+ extraEncryptionKey)
                else keys

              override def allKeys(owner: Member)(implicit
                  traceContext: TraceContext
              ): FutureUnlessShutdown[KeyCollection] =
                super.allKeys(owner).map(appendKeyIfRecipient(owner, _))(ec)

              override def allKeys(members: Seq[Member])(implicit
                  traceContext: TraceContext
              ): FutureUnlessShutdown[Map[Member, KeyCollection]] =
                super
                  .allKeys(members)
                  .map(_.map { case (member, keys) =>
                    member -> appendKeyIfRecipient(member, keys)
                  })(ec)
            }

          val snapshotWithExtraKey = new SynchronizerSnapshotSyncCryptoApi(
            snapshot.psid,
            topologyWithExtraKey,
            snapshot.crypto,
            snapshot.syncCryptoSigner,
            snapshot.syncCryptoVerifier,
            loggerFactory,
          )

          val childMessage = childEnvelope.protocolMessage
          val extraEncryptedRandomness = snapshotWithExtraKey.pureCrypto
            .encryptDeterministicWith(
              randomness(child),
              extraEncryptionKey,
              childMessage.viewEncryptionKeyRandomness.head1.encryptionAlgorithmSpec,
            )
            .valueOrFail("failed to encrypt child view randomness with the extra key")

          val childMessageWithAdditionalCorrectKey = childMessage.copy(
            viewEncryptionKeyRandomness =
              childMessage.viewEncryptionKeyRandomness :+ extraEncryptedRandomness
          )

          val envelopesWithAdditionalCorrectKey = NonEmptyUtil.fromUnsafe(
            Seq(childEnvelope.copy(protocolMessage = childMessageWithAdditionalCorrectKey))
          )

          val decryptedViews =
            decrypter
              .decryptViews(
                envelopesWithAdditionalCorrectKey,
                snapshotWithExtraKey,
                defaultSynchronizerLimits,
              )
              .futureValueUS
              .valueOrFail("failed transaction")

          checkDecryptedViews(decryptedViews, nbrViews = 1L)
        }

        // Tree shape used below:
        //
        //          Node(1)
        //         /       \
        //     Node(10)  Node(11)
        //                  |
        //               Node(110)
        //
        // The test makes 110 invalid (i.e. fail transparency checks with a bogus encryption key), so the failure
        // should invalidate 11 and propagate to 1.
        "propagate descendant transparency-check failures" in {
          implicit val traceContext: TraceContext = TraceContext.empty

          val env = new Env(enableTransparency = true)
          import env.*

          val pureCrypto = jceCrypto.pureCrypto
          val example = new ExampleTransactionFactory(pureCrypto)().MultipleRootsAndViewNestings

          val grandchild110Randomness = mkRandomness(pureCrypto)
          val grandchild110Lvt = LightTransactionViewTree
            .fromTransactionViewTreeUsingCiphertextIdReference(
              example.transactionViewTree110,
              Seq.empty,
              Map.empty,
              testedProtocolVersion,
            )
            .valueOrFail("failed to create grandchild light view")
          val grandchild110Enc =
            encrypt(grandchild110Lvt, grandchild110Randomness, pureCrypto, snapshot)
          val grandchild110EncWithInvalidTransparencyEntry = grandchild110Enc.copy(
            viewEncryptionKeyRandomness = grandchild110Enc.viewEncryptionKeyRandomness :+
              grandchild110Enc.viewEncryptionKeyRandomness.head1.copy[SecureRandomness](
                ciphertext = ByteString.fromHex("DEADBEEFDEADBEEFDEADBEEFDEADBEEF")
              )
          )

          val allEnvelopes = buildNestedViewEnvelopes(
            example.transactionViewTree1,
            Left(example.transactionViewTree10),
            example.transactionViewTree11,
            Right(
              ViewMetadata(
                example.transactionViewTree110.viewHash,
                grandchild110Randomness,
                grandchild110EncWithInvalidTransparencyEntry,
                ByCiphertextId(
                  grandchild110EncWithInvalidTransparencyEntry.encryptedViews.computeCiphertextId(
                    pureCrypto
                  ),
                  NonNegativeInt.zero,
                ),
              )
            ),
            recipients,
            pureCrypto,
            snapshot,
          )

          val decryptedViews = decrypter
            .decryptViews(allEnvelopes, snapshot, defaultSynchronizerLimits)
            .futureValueUS
            .valueOrFail("failed transaction")

          val decryptedViewHashes = decryptedViews.views.map(_.view.unwrap.viewHash).toSet
          decryptedViewHashes shouldBe Set(
            example.transactionViewTree10.viewHash
          )

          val errors = decryptedViews.decryptionErrors.map(_.show)
          val grandchild110ViewContext =
            s"for message with hash(es) ${example.transactionViewTree110.viewHash}"
          errors.exists(
            _.contains(
              s"TransparencyCheckError(\"Re-encrypted ciphertext does not match the view ciphertext for " +
                s"recipient key ID ${grandchild110EncWithInvalidTransparencyEntry.viewEncryptionKeyRandomness.head1.encryptedFor} $grandchild110ViewContext\")"
            )
          ) shouldBe true
          errors.exists(
            _.contains(
              s"Parent view ${example.transactionViewTree11.viewHash} is invalid because a subview is invalid"
            )
          ) shouldBe true
          errors.exists(
            _.contains(
              s"Parent view ${example.transactionViewTree1.viewHash} is invalid because a subview is invalid"
            )
          ) shouldBe true
          errors.exists(
            _.contains("SymmetricDecryptError(FailedToDecrypt")
          ) shouldBe false
        }
      }
    }
  }
}
