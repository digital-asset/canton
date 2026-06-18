// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.syntax.either.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.{
  SecureRandomness,
  Signature,
  SymmetricKeyScheme,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.LightTransactionViewTree
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.protocol.messages.{
  EncryptedMultipleViews,
  EncryptedMultipleViewsMessage,
  EncryptedSingleViewMessage,
  EncryptedView,
  EncryptedViewMessage,
}
import com.digitalasset.canton.protocol.{ExampleTransaction, ViewHash}
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

class TransactionConfirmationRequestFactoryV1Test
    extends TransactionConfirmationRequestFactoryTest {

  /* We follow a different approach to encrypt views using view hashes. Both this approach and the
   * one used in the production implementation must yield the same result.
   *
   * The main difference lies in the order of operations. Unlike the production implementation,
   * where the view randomness map is generated upfront, and we simply call
   * `EncryptedViewMessageFactory.encryptGroupedViews(...)`, this test implementation
   * follows a different flow: we first encrypt the views, then generate the view randomness map,
   * and finally construct the `EncryptedViewMessage` instances manually.
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

    if (testedProtocolVersion >= ProtocolVersion.v35) {
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
          )(
            NonEmptyUtil.fromUnsafe(lightTrees.map(_._1)),
            defaultMaxBytesToDecompress,
          )
          .valueOr(err => fail(s"fail to encrypt view tree: $err"))

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

        messages.map((_, recipients))
      }
    } else {
      lightTransactionTreeWithRecipients.map { case (recipients, (ltvt, signatureO)) =>
        val (_, sessionKeyRandomness) = hashToKeyMap(ltvt.viewHash)

        val sessionKey = cryptoPureApi
          .createSymmetricKey(sessionKeyRandomness, viewEncryptionScheme)
          .valueOrFail("fail to create symmetric key from randomness")

        val participants = ltvt.informees
          .map(cryptoSnapshot.ipsSnapshot.activeParticipantsOf(_).futureValueUS)
          .flatMap(_.keySet)

        val randomnessMapNE = NonEmpty
          .from(randomnessMap(sessionKeyRandomness, participants, cryptoPureApi).values.toSeq)
          .valueOrFail("session key randomness map is empty")

        val encryptedView = EncryptedView
          .compressed(
            cryptoPureApi,
            sessionKey,
            TransactionViewType,
          )(
            ltvt,
            defaultMaxBytesToDecompress,
          )
          .valueOr(err => fail(s"fail to encrypt view tree: $err"))

        val message = EncryptedSingleViewMessage(
          signatureO,
          ltvt.viewHash,
          randomnessMapNE,
          encryptedView,
          transactionFactory.psid,
          SymmetricKeyScheme.Aes128Gcm,
          testedProtocolVersion,
        )

        (message, recipients)
      }
    }
  }

  "A ConfirmationRequestFactory version 1 (uses ViewHash-based references)" must {
    if (
      testedProtocolVersion <= ProtocolVersion.v35 || testedProtocolVersion == ProtocolVersion.dev
    )
      behave like transactionConfirmationRequestFactoryTest(useNewEncryptionAlgorithm = false)
  }

}
