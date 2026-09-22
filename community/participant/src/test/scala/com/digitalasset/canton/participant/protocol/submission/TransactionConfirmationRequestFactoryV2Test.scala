// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  SecureRandomness,
  Signature,
  SymmetricKeyScheme,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.{
  ByCiphertextId,
  FullTransactionViewTree,
  LightTransactionViewTree,
}
import com.digitalasset.canton.protocol.messages.{EncryptedMultipleViews, EncryptedViewMessage}
import com.digitalasset.canton.protocol.{ExampleTransaction, ViewHash}
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.{NonEmpty, NonEmptyUtil}

import scala.collection.concurrent.TrieMap

class TransactionConfirmationRequestFactoryV2Test
    extends TransactionConfirmationRequestFactoryTest {

  /* We follow a different approach to encrypt views with ciphertext IDs. Both this approach and the
   * one used in the production implementation must yield the same result.
   *
   * The production implementation groups views with the same recipients by depth and can process
   * groups at the same depth in parallel.
   *
   * In this test implementation, we only care about deterministic results, so we process the
   * recipient groups sequentially from deepest to shallowest. For each group, we first build the
   * final `LightTransactionViewTree`s directly with `ByCiphertextId` references, using the
   * ciphertext IDs that were computed for previously processed child groups, and then encrypt the
   * resulting trees.
   */
  override protected def buildAndEncryptViews(
      example: ExampleTransaction,
      hashToKeyMap: Map[ViewHash, (Recipients, SecureRandomness)],
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
  ): Seq[(EncryptedViewMessage[TransactionViewType.type], Recipients)] = {
    val viewsWithRecipientsAndSignature = example.transactionViewTreesWithWitnesses
      .map { case (tree, _) =>
        val signature = Option.when(tree.isTopLevel)(SymbolicCrypto.emptySignature)
        val (recipients, _) = hashToKeyMap(tree.viewHash)
        (recipients, tree, signature)
      }
    encryptViews(
      cryptoSnapshot,
      viewsWithRecipientsAndSignature,
      hashToKeyMap,
    )
  }

  private def encryptViews(
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
      viewsWithRecipientsAndSignature: Seq[
        (Recipients, FullTransactionViewTree, Option[Signature])
      ],
      hashToKeyMap: Map[ViewHash, (Recipients, SecureRandomness)],
  ): Seq[(EncryptedViewMessage[TransactionViewType.type], Recipients)] = {
    val cryptoPureApi = cryptoSnapshot.pureCrypto
    val viewEncryptionScheme = cryptoPureApi.defaultSymmetricKeyScheme

    // Group views by recipients and sort them by depth and then by string representation
    // to ensure deterministic order of the views.
    val viewsWithRecipientsAndSignatureOrdered =
      viewsWithRecipientsAndSignature
        .groupBy { case (recipients, _, _) => recipients }
        .toSeq
        .sortBy { case (recipients, _) =>
          (recipients.trees.head1.depth, recipients.toString)
        }(
          Ordering.Tuple2(Ordering[Int].reverse, Ordering[String])
        )
        .map { case (recipients, views) =>
          recipients -> views.map { case (_, tree, signature) => (tree, signature) }
        }

    val byCiphertextIdMap: TrieMap[ViewHash, ByCiphertextId] = TrieMap.empty

    // We traverse the trees from the highest to lowest depth (leaf views first) encrypt them and
    // populate the map that allows to reference views by their ciphertext ID in the subview references
    // of their parent views.
    viewsWithRecipientsAndSignatureOrdered.foldLeft(
      Seq.empty[(EncryptedViewMessage[TransactionViewType.type], Recipients)]
    ) { case (state, (recipients, trees)) =>
      val (firstTree, signature) = trees.head

      val (_, sessionKeyRandomness) = hashToKeyMap(firstTree.viewHash)

      val sessionKey = cryptoPureApi
        .createSymmetricKey(sessionKeyRandomness, viewEncryptionScheme)
        .valueOrFail("fail to create symmetric key from randomness")

      val participants = firstTree.informees
        .map(cryptoSnapshot.ipsSnapshot.activeParticipantsOf(_).futureValueUS)
        .flatMap(_.keySet)

      val lightTreesWithCtIds = trees.map { case (tree, _) =>
        LightTransactionViewTree
          .fromTransactionViewTreeUsingCiphertextIdReference(
            tree,
            tree.subviewHashes.map(viewHash => hashToKeyMap(viewHash)._2),
            byCiphertextIdMap.toMap,
            testedProtocolVersion,
          )
          .valueOrFail("no ciphertext ID")
      }

      val encryptedViews = EncryptedMultipleViews
        .compressAndEncryptViews(
          cryptoPureApi,
          sessionKey,
          TransactionViewType,
          testedProtocolVersion,
        )(
          NonEmptyUtil.fromUnsafe(lightTreesWithCtIds),
          defaultMaxBytesToDecompress,
        )
        .valueOr(err => fail(s"fail to encrypt view tree: $err"))

      val ciphertextId = encryptedViews.computeCiphertextId(cryptoPureApi.pureCrypto)
      lightTreesWithCtIds.zipWithIndex.foreach { case (lightTree, i) =>
        byCiphertextIdMap.put(
          lightTree.viewHash,
          ByCiphertextId(ciphertextId, NonNegativeInt.tryCreate(i)),
        )
      }

      val randomnessMapNE = NonEmpty
        .from(randomnessMap(sessionKeyRandomness, participants, cryptoPureApi).values.toSeq)
        .valueOrFail("session key randomness map is empty")

      val messages = Seq(
        EncryptedViewMessage(
          encryptedViews = encryptedViews,
          viewHashes = NonEmptyUtil.fromUnsafe(trees.map(_._1.viewHash)),
          viewEncryptionKeyRandomness = randomnessMapNE,
          synchronizerId = transactionFactory.psid,
          viewEncryptionScheme = SymmetricKeyScheme.Aes128Gcm,
          submittingParticipantSignature = signature,
          protocolVersion = testedProtocolVersion,
        )
      )

      state ++ messages.map((_, recipients))
    }
  }

  "A ConfirmationRequestFactory version 2 (uses ciphertext IDs references)" must {
    if (testedProtocolVersion >= ProtocolVersion.transparency)
      behave like transactionConfirmationRequestFactoryTest()
  }

}
