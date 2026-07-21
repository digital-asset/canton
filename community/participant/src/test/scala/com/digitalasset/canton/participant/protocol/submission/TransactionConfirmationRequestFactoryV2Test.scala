// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{
  SecureRandomness,
  Signature,
  SymmetricKeyScheme,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.{
  ByCiphertextId,
  ByViewHash,
  LightTransactionViewTree,
  SubviewReferenceAndKey,
}
import com.digitalasset.canton.protocol.messages.{
  EncryptedMultipleViews,
  EncryptedMultipleViewsMessage,
  EncryptedViewMessage,
}
import com.digitalasset.canton.protocol.{ExampleTransaction, ViewHash}
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.nonempty.{NonEmpty, NonEmptyUtil}

import scala.collection.concurrent.TrieMap

class TransactionConfirmationRequestFactoryV2Test
    extends TransactionConfirmationRequestFactoryTest {

  /* We follow a different approach to encrypt views with ciphertext IDs. Both this approach and the
   * one used in the production implementation must yield the same result.
   *
   * The main difference is that, in the production implementation, we group views with the same
   * recipients by depth, allowing all groups of views within a given depth level to be processed in
   * parallel.
   *
   * In this test implementation, we do not do that because we do not care about the execution
   * strategy for groups at a given depth. Instead, after grouping views by recipients, we directly order the
   * resulting groups by depth and process them sequentially. For each group, we encrypt the views,
   * compute the corresponding ciphertext IDs, and replace the original `LightTransactionViewTree`,
   * which contains `ByViewHash` references, with a new `LightTransactionViewTree` containing
   * `ByCiphertextId` references as the ciphertext IDs become available.
   */
  override protected def encryptViews(
      example: ExampleTransaction,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
      lightTransactionTreeWithRecipients: Seq[
        (Recipients, (LightTransactionViewTree, Option[Signature]))
      ],
      hashToKeyMap: Map[ViewHash, (Recipients, SecureRandomness)],
  )(implicit
      traceContext: TraceContext
  ): Seq[(EncryptedViewMessage[TransactionViewType.type], Recipients)] = {
    val cryptoPureApi = cryptoSnapshot.pureCrypto
    val viewEncryptionScheme = cryptoPureApi.defaultSymmetricKeyScheme

    // Group views by recipients and sort them by depth and then by string representation
    // to ensure deterministic order of the views.
    val lightTransactionTreeWithRecipientsOrdered =
      lightTransactionTreeWithRecipients
        .groupBy { case (recipients, _) => recipients }
        .toSeq
        .sortBy { case (recipients, _) =>
          (recipients.trees.head1.depth, recipients.toString)
        }(
          Ordering.Tuple2(Ordering[Int].reverse, Ordering[String])
        )
        .map { case (recipients, views) => recipients -> views.map { case (_, view) => view } }

    val byCiphertextIdMap: TrieMap[ViewHash, ByCiphertextId] = TrieMap.empty

    // We traverse the trees from the highest to lowest depth (leaf views first) encrypt them and
    // populate the map that allows to reference views by their ciphertext ID in the subview references
    // of their parent views.
    lightTransactionTreeWithRecipientsOrdered.foldLeft(
      Seq.empty[(EncryptedViewMessage[TransactionViewType.type], Recipients)]
    ) { case (state, (recipients, lightTrees)) =>
      val (firstTree, signature) = lightTrees.head

      val (_, sessionKeyRandomness) = hashToKeyMap(firstTree.viewHash)

      val sessionKey = cryptoPureApi
        .createSymmetricKey(sessionKeyRandomness, viewEncryptionScheme)
        .valueOrFail("fail to create symmetric key from randomness")

      val participants = firstTree.informees
        .map(cryptoSnapshot.ipsSnapshot.activeParticipantsOf(_).futureValueUS)
        .flatMap(_.keySet)

      val lightTreesWithCtIds = lightTrees.map { case (lightTree, _) =>
        LightTransactionViewTree.tryCreate(
          lightTree.tree,
          lightTree.subviewReferencesAndKeys.map(vk =>
            SubviewReferenceAndKey(
              byCiphertextIdMap
                .get(vk.subviewReference match {
                  case ByViewHash(viewHash) => viewHash
                  case _: ByCiphertextId => fail("unexpected ByCiphertextId in subview reference")
                })
                .valueOrFail("no ciphertext ID"),
              vk.viewEncryptionKeyRandomness,
            )
          ),
          testedProtocolVersion,
        )
      }

      val encryptedViews = EncryptedMultipleViews
        .compressAndEncryptViews(
          cryptoPureApi,
          sessionKey,
          TransactionViewType,
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
        EncryptedMultipleViewsMessage(
          encryptedViews = encryptedViews,
          viewHashes = NonEmptyUtil.fromUnsafe(lightTrees.map(_._1.viewHash)),
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

  // TODO(#32393): Enable for PV`transparency`+ after implementing the new full view encryption/decryption flow.
  "A ConfirmationRequestFactory version 2 (uses ciphertext IDs references)" must {
    /*if (testedProtocolVersion >= ProtocolVersion.transparency)
      behave like transactionConfirmationRequestFactoryTest()*/
  }

}
