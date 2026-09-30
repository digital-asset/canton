// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{
  AsymmetricEncrypted,
  CryptoPureApi,
  Encrypted,
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
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.ExampleTransactionFactory
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, Recipients}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmptyUtil
import com.google.protobuf.ByteString
import monocle.Monocle.toAppliedFocusOps

class ViewMessageDecrypterV2Test extends BaseTestWordSpec with ViewMessageDecrypterTest {

  "A ViewMessageDecrypter version 2 (using ciphertext ID)" must {
    if (testedProtocolVersion >= ProtocolVersion.transparency) {
      behave like viewMessageDecrypterTest()

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
          }
        )

        val decryptedViews = env.decrypter
          .decryptViews(env.allEnvelopes, env.snapshot, env.defaultSynchronizerLimits)
          .futureValueUS
          .value

        env.checkDecryptedViews(decryptedViews)
      }

      "successfully decrypt if the same envelope is duplicated" in {
        val env = new Env()
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
        val env = new Env()
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

      def mkRandomness(pureCrypto: CryptoPureApi): SecureRandomness =
        pureCrypto.generateSecureRandomness(pureCrypto.defaultSymmetricKeyScheme.keySizeInBytes)

      def mkViewKeyData(
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
          .encryptFor(viewKeyRandomness, Seq(ParticipantId("participant")))
          .futureValueUS
          .valueOrFail("failed to encrypt view randomness")
          .values
          .toSeq
        (viewKey, encryptedViewKeys)
      }

      def encrypt(
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

      def buildNestedViewEnvelopes(
          parentViewTree: FullTransactionViewTree,
          child10ViewTree: FullTransactionViewTree,
          child11ViewTree: FullTransactionViewTree,
          grandchild110Randomness: SecureRandomness,
          grandchild110Enc: EncryptedViewMessage[TransactionViewType.type],
          recipients: Recipients,
          pureCrypto: CryptoPureApi,
          snapshot: SynchronizerSnapshotSyncCryptoApi,
      ) = {

        val parentRandomness = mkRandomness(pureCrypto)
        val child10Randomness = mkRandomness(pureCrypto)
        val child11Randomness = mkRandomness(pureCrypto)

        val grandchild110CiphertextId =
          grandchild110Enc.encryptedViews.computeCiphertextId(pureCrypto)

        val child10Lvt = LightTransactionViewTree
          .fromTransactionViewTreeUsingCiphertextIdReference(
            child10ViewTree,
            Seq.empty,
            Map.empty,
            testedProtocolVersion,
          )
          .valueOrFail("failed to create sibling light view")
        val child10Enc = encrypt(child10Lvt, child10Randomness, pureCrypto, snapshot)
        val child10CiphertextId = child10Enc.encryptedViews.computeCiphertextId(pureCrypto)

        val child11Lvt = LightTransactionViewTree
          .fromTransactionViewTreeUsingCiphertextIdReference(
            child11ViewTree,
            Seq(grandchild110Randomness),
            Map(
              grandchild110Enc.viewHashes.head1 -> ByCiphertextId(
                grandchild110CiphertextId,
                com.digitalasset.canton.config.RequireTypes.NonNegativeInt.zero,
              )
            ),
            testedProtocolVersion,
          )
          .valueOrFail("failed to create cached child light view")
        val child11Enc = encrypt(child11Lvt, child11Randomness, pureCrypto, snapshot)
        val child11CiphertextId = child11Enc.encryptedViews.computeCiphertextId(pureCrypto)

        val parentLvt = LightTransactionViewTree
          .fromTransactionViewTreeUsingCiphertextIdReference(
            parentViewTree,
            Seq(child10Randomness, child11Randomness),
            Map(
              child10ViewTree.viewHash -> ByCiphertextId(
                child10CiphertextId,
                NonNegativeInt.zero,
              ),
              child11ViewTree.viewHash -> ByCiphertextId(
                child11CiphertextId,
                NonNegativeInt.zero,
              ),
            ),
            testedProtocolVersion,
          )
          .valueOrFail("failed to create parent light view")
        val parentEnc = encrypt(parentLvt, parentRandomness, pureCrypto, snapshot)

        NonEmptyUtil.fromUnsafe(
          Seq(parentEnc, child10Enc, child11Enc, grandchild110Enc).map(
            OpenEnvelope(_, recipients)(testedProtocolVersion)
          )
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

        val env = new Env()
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
        val grandchildEnc = encrypt(grandchildLvt, grandchild110Randomness, pureCrypto, snapshot)

        val allEnvelopes = buildNestedViewEnvelopes(
          example.transactionViewTree1,
          example.transactionViewTree10,
          example.transactionViewTree11,
          grandchild110Randomness,
          grandchildEnc
            .focus(_.encryptedViews.viewTrees)
            .replace(
              Encrypted.fromByteString(ByteString.copyFromUtf8("invalid ciphertext"))
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
            "SymmetricDecryptError(FailedToDecrypt(javax.crypto.AEADBadTagException: data too short))"
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
    }
  }
}
