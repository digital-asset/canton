// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.syntax.either.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  SecureRandomness,
  Signature,
  SymmetricKeyScheme,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.LightTransactionViewTree
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.protocol.messages.{EncryptedMultipleViews, EncryptedViewMessage}
import com.digitalasset.canton.protocol.{ExampleTransaction, ViewHash}
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.{NonEmpty, NonEmptyUtil}

class TransactionConfirmationRequestFactoryV1Test
    extends TransactionConfirmationRequestFactoryTest {

  override protected def buildAndEncryptViews(
      example: ExampleTransaction,
      hashToKeyMap: Map[ViewHash, (Recipients, SecureRandomness)],
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
  ): Seq[(EncryptedViewMessage[TransactionViewType.type], Recipients)] = {
    val lightTransactionTreeWithRecipients = createLightTransactionTreesWithRecipients(
      example,
      hashToKeyMap,
    )
    encryptViews(cryptoSnapshot, lightTransactionTreeWithRecipients, hashToKeyMap)
  }

  private def createLightTransactionTreesWithRecipients(
      example: ExampleTransaction,
      hashToKeyMap: Map[ViewHash, (Recipients, SecureRandomness)],
  ): Seq[(Recipients, (LightTransactionViewTree, Option[Signature]))] =
    example.transactionViewTreesWithWitnesses
      .map { case (tree, _) =>
        val signature = Option.when(tree.isTopLevel)(SymbolicCrypto.emptySignature)

        val (recipients, _) = hashToKeyMap(tree.viewHash)

        (
          recipients,
          (
            LightTransactionViewTree
              .fromTransactionViewTreeUsingViewHashReference(
                tree,
                tree.subviewHashes.map(viewHash => hashToKeyMap(viewHash)._2),
                testedProtocolVersion,
              )
              .valueOrFail("fail to create light transaction view tree"),
            signature,
          ),
        )
      }

  /* We follow a different approach to encrypt views using view hashes. Both this approach and the
   * one used in the production implementation must yield the same result.
   *
   * The main difference lies in the order of operations. Unlike the production implementation,
   * where the view randomness map is generated upfront, and we simply call
   * `EncryptedViewMessageFactory.encryptGroupedViews(...)`, this test implementation
   * follows a different flow: we first encrypt the views, then generate the view randomness map,
   * and finally construct the `EncryptedViewMessage` instances manually.
   */
  private def encryptViews(
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
      lightTransactionTreeWithRecipients: Seq[
        (Recipients, (LightTransactionViewTree, Option[Signature]))
      ],
      hashToKeyMap: Map[ViewHash, (Recipients, SecureRandomness)],
  ): Seq[(EncryptedViewMessage[TransactionViewType.type], Recipients)] = {
    val cryptoPureApi = cryptoSnapshot.pureCrypto
    val viewEncryptionScheme = cryptoPureApi.defaultSymmetricKeyScheme

    val lightTreesByRecipientsE
        : Seq[(Recipients, Seq[(LightTransactionViewTree, Option[Signature])])] = {
      val groupedOrdered = lightTransactionTreeWithRecipients.groupMap(_._1)(_._2)
      val recipientsInOrder = lightTransactionTreeWithRecipients.map(_._1).distinct

      recipientsInOrder.flatMap { recipients =>
        groupedOrdered.get(recipients).map(recipients -> _)
      }
    }

    lightTreesByRecipientsE.flatMap { case (recipients, lightTrees) =>
      val (firstTree, signature) = lightTrees.head

      val (_, sessionKeyRandomness) = hashToKeyMap(firstTree.viewHash)

      val sessionKey = cryptoPureApi
        .createSymmetricKey(sessionKeyRandomness, viewEncryptionScheme)
        .valueOrFail("fail to create symmetric key from randomness")

      val participants = firstTree.informees
        .map(cryptoSnapshot.ipsSnapshot.activeParticipantsOf(_).futureValueUS)
        .flatMap(_.keySet)

      val encryptedViews = EncryptedMultipleViews
        .compressAndEncryptViews(
          cryptoPureApi,
          sessionKey,
          TransactionViewType,
          testedProtocolVersion,
        )(
          NonEmptyUtil.fromUnsafe(lightTrees.map(_._1)),
          defaultMaxBytesToDecompress,
        )
        .valueOr(err => fail(s"fail to encrypt view tree: $err"))

      val randomnessMapNE = NonEmpty
        .from(randomnessMap(sessionKeyRandomness, participants, cryptoPureApi).values.toSeq)
        .valueOrFail("session key randomness map is empty")

      val messages = Seq(
        EncryptedViewMessage(
          encryptedViews = encryptedViews,
          viewHashes = NonEmptyUtil.fromUnsafe(lightTrees.map(_._1.viewHash)),
          viewEncryptionKeyRandomness = randomnessMapNE,
          synchronizerId = transactionFactory.psid,
          viewEncryptionScheme = SymmetricKeyScheme.Aes128Gcm,
          submittingParticipantSignature = signature,
          protocolVersion = testedProtocolVersion,
        )
      )

      messages.map((_, recipients))
    }

  }

  "A ConfirmationRequestFactory version 1 (uses ViewHash-based references)" must {
    if (testedProtocolVersion < ProtocolVersion.transparency)
      behave like transactionConfirmationRequestFactoryTest()
  }

}
