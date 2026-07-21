// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.checked
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.{
  SecureRandomness,
  Signature,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.{
  ByCiphertextId,
  ByViewHash,
  LightTransactionViewTree,
  SubviewReferenceAndKey,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.DecryptedViews
import com.digitalasset.canton.participant.protocol.TransactionProcessor.TransactionProcessorError
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.ViewHash
import com.digitalasset.canton.protocol.messages.{
  EncryptedViewMessage,
  EncryptedViewMessageError,
  TransactionViewMessage,
}
import com.digitalasset.canton.sequencing.protocol.{MemberRecipient, OpenEnvelope, WithRecipients}
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext
import scala.util.Success

/** This class is responsible for decrypting the views contained in `EncryptedViewMessages`. It does
  * so in two steps:
  *
  * (1) Extract the randomness for each view hash from `EncryptedViewMessages`. This step is
  * necessary to be able to decrypt the `LightTransactionViewTrees` in the second step, but it can
  * be done in parallel for all messages in the batch, as the randomness is stored separately from
  * the view trees and can be extracted without decrypting the view trees.
  *
  * (2) Decrypt the `LightTransactionViewTrees` contained in `EncryptedViewMessages`. This step can
  * also be done in parallel for all messages in the batch. The decryption of
  * `LightTransactionViewTrees` can also yield the randomness for additional view hashes that are
  * listed in `LightTransactionViewTree.subviewReferencesAndKeys`, so we keep adding randomness to
  * the randomness map whenever it becomes available and make it available for the decryption of
  * other views that list the same hash.
  *
  * This implementation assumes that view hashes are UNIQUE across the batch.
  */
private[decrypter] class ViewMessageDecrypterImplV1(
    participantId: ParticipantId,
    sessionKeyStore: ConfirmationRequestSessionKeyStore,
    snapshot: SynchronizerSnapshotSyncCryptoApi,
    protocolVersion: ProtocolVersion,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val pureCrypto = snapshot.pureCrypto

  def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType]]]]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionProcessorError, DecryptedViews[
    LightTransactionViewTree
  ]] = {
    // To recover parallel processing to the largest possible extent, we'll associate a promise to each received
    // view. The promise gets fulfilled once the randomness of that view has been extracted,
    // either from EncryptedViewMessage.sessionKeys or from LightTransactionViewTree.subviewHashesAndKeys.

    val randomnessMap: Map[ViewHash, PromiseUnlessShutdown[SecureRandomness]] =
      batch
        .flatMap(_.protocolMessage.viewHashes)
        .map(viewHash =>
          viewHash -> PromiseUnlessShutdown.supervised[SecureRandomness](
            "secure-randomness",
            futureSupervisor,
          )
        )
        .forgetNE
        .toMap

    EitherT.right(for {
      // Extract randomness from EncryptedViewMessages
      _ <- MonadUtil.parTraverseWithLimit_(pureCrypto.encryptionParallelism)(
        batch.toSeq
      ) { encryptedViewMessageEnvelope =>
        // For multi-view messages, let's put the randomness for all the hashes
        val viewHashes = encryptedViewMessageEnvelope.protocolMessage.viewHashes
        extractRandomnessFromEnvelope(encryptedViewMessageEnvelope).map { randomnessO =>
          randomnessO.foreach { randomness =>
            viewHashes.foreach { viewHash =>
              storeRandomness(viewHash, randomness, checked(randomnessMap(viewHash)))
            }
          }
        }
      }

      // Decrypt LightTransactionViewTrees and keep adding randomness to randomnessMap whenever they become available.
      decryptionResult <- MonadUtil
        .parTraverseWithLimit(pureCrypto.encryptionParallelism)(
          batch.toSeq
        ) { encryptedViewMessageEnvelope =>
          // Transform single view to a list for a compatibility with the v35+ multi-view result
          decryptViewMessageEnvelope(randomnessMap, encryptedViewMessageEnvelope).map {
            case Left(error) => Seq(Left(error))
            case Right(views) =>
              views.forgetNE.map(view => Right(view))
          }
        }
        .map(_.flatten)

    } yield DecryptedViews.fromViewsWithSignature(decryptionResult))
  }

  private def extractRandomnessFromEnvelope(
      envelope: OpenEnvelope[TransactionViewMessage]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[SecureRandomness]] =
    if (envelope.recipients.leafRecipients.contains(MemberRecipient(participantId))) {
      val message = envelope.protocolMessage
      EncryptedViewMessage
        .decryptRandomness(
          snapshot,
          sessionKeyStore,
          message,
          participantId,
        )
        .valueOr { e =>
          ErrorUtil.internalError(
            new IllegalArgumentException(
              s"Can't decrypt the randomness of the message with hash(es) ${message.viewHashes} " +
                s"where I'm allegedly an informee. $e"
            )
          )
        }
        .map(Some(_))
    } else FutureUnlessShutdown.pure(None)

  private def decryptViewMessageEnvelope(
      randomnessMap: Map[ViewHash, PromiseUnlessShutdown[SecureRandomness]],
      encryptedViewEnvelope: OpenEnvelope[TransactionViewMessage],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Either[
    EncryptedViewMessageError,
    NonEmpty[Seq[(WithRecipients[LightTransactionViewTree], Option[Signature])]],
  ]] = {

    val encryptedViewMessage = encryptedViewEnvelope.protocolMessage
    for {
      // For the multiple views, we can take randomness from any hash from list, it should be the same
      // TODO(#31213): Handle multiple views with the same view hash during decryption
      randomness <- randomnessMap(encryptedViewMessage.viewHashes.head1).futureUS
      decryptionResult <- decryptMessageWithRandomness(
        randomnessMap,
        encryptedViewMessage,
        randomness,
      ).value
    } yield decryptionResult.map { case (viewTrees, signature) =>
      viewTrees.map { viewTree =>
        (WithRecipients(viewTree, encryptedViewEnvelope.recipients), signature)
      }
    }
  }

  private def decryptMessageWithRandomness(
      randomnessMap: Map[ViewHash, PromiseUnlessShutdown[SecureRandomness]],
      encryptedViewMessage: TransactionViewMessage,
      randomness: SecureRandomness,
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    EncryptedViewMessageError,
    (
        NonEmpty[Seq[LightTransactionViewTree]],
        Option[Signature],
    ),
  ] =
    for {
      lightTransactionMultiViewTree <- EncryptedViewMessage.decryptFor(
        snapshot,
        sessionKeyStore,
        encryptedViewMessage,
        participantId,
        Some(randomness),
      )(
        LightTransactionViewTree
          .fromByteString(
            (pureCrypto, EncryptedViewMessage.computeRandomnessLength(pureCrypto)),
            protocolVersion,
          )(_)
          .leftMap(err => DefaultDeserializationError(err.message))
      )

      viewTrees = lightTransactionMultiViewTree.viewTrees

      _ = viewTrees.forgetNE.foreach { viewTree =>
        viewTree.subviewReferencesAndKeys
          .foreach {
            case SubviewReferenceAndKey(ByViewHash(viewHash), subviewKey) =>
              randomnessMap.get(viewHash) match {
                case Some(promise) => storeRandomness(viewHash, subviewKey, promise)
                case None =>
                  // It is enough to alarm here.
                  // The view will be filtered out when attempting to construct a FullTransactionViewTree.
                  SyncServiceAlarm
                    .Warn(
                      s"View ${viewTree.viewHash} lists a subview with hash $viewHash, but I haven't received any views for this hash"
                    )
                    .report()
              }
            case SubviewReferenceAndKey(ByCiphertextId(_, _), _) =>
              ErrorUtil.invalidState(
                s"Invalid subview reference in view ${viewTree.viewHash}: expected a view hash, but got a ciphertext ID"
              )
          }
      }
    } yield (viewTrees, encryptedViewMessage.submittingParticipantSignature)

  private def storeRandomness(
      viewHash: ViewHash,
      randomness: SecureRandomness,
      promise: PromiseUnlessShutdown[SecureRandomness],
  )(implicit traceContext: TraceContext): Unit = {
    val isNew = promise.outcome(randomness)
    if (!isNew) {
      val previousValue = promise.unwrap.future.value
      if (!previousValue.contains(Success(Outcome(randomness)))) {
        ErrorUtil.internalError(
          new IllegalArgumentException(
            s"View $viewHash has different encryption keys associated with it. (Previous: $previousValue, new: $randomness)"
          )
        )
      }
    }
  }
}
