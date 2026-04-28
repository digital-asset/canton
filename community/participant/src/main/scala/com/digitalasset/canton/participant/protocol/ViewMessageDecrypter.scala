// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.checked
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.{
  SecureRandomness,
  Signature,
  SynchronizerCryptoPureApi,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.{LightTransactionViewTree, ViewHashAndKey}
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
import com.digitalasset.canton.sequencing.protocol.{
  MemberRecipient,
  OpenEnvelope,
  Recipients,
  WithRecipients,
}
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext
import scala.util.Success

class ViewMessageDecrypter(
    participantId: ParticipantId,
    protocolVersion: ProtocolVersion,
    sessionKeyStore: ConfirmationRequestSessionKeyStore,
    snapshot: SynchronizerSnapshotSyncCryptoApi,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private def pureCrypto: SynchronizerCryptoPureApi = snapshot.pureCrypto

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

    val messagesWithRecipients = batch.map { envelope =>
      envelope.protocolMessage -> envelope.recipients
    }

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
      _ <- messagesWithRecipients.toNEF.parTraverse { case (message, recipients) =>
        // For multi-view messages, let's put the randomness for all the hashes
        val viewHashes = message.viewHashes
        extractRandomnessFromEnvelope(message, recipients).map { randomnessO =>
          randomnessO.foreach { randomness =>
            viewHashes.foreach { viewHash =>
              storeRandomness(viewHash, randomness, checked(randomnessMap(viewHash)))
            }
          }
        }
      }

      // Decrypt LightTransactionViewTrees and keep adding randomness to randomnessMap whenever they become available.
      decryptionResult <- messagesWithRecipients.toNEF
        .parTraverse { case (message, recipients) =>
          // Transform single view to a list for a compatibility with the v35+ multi-view result
          decryptViews(randomnessMap, message, recipients).map {
            case Left(error) => Seq(Left(error))
            case Right(views) =>
              views.forgetNE.map(view => Right(view))
          }
        }
        .map(_.forgetNE.flatten)

    } yield DecryptedViews(decryptionResult))
  }

  private def extractRandomnessFromEnvelope(
      message: TransactionViewMessage,
      recipients: Recipients,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[SecureRandomness]] =
    if (recipients.leafRecipients.contains(MemberRecipient(participantId))) {
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

  private def decryptViews(
      randomnessMap: Map[ViewHash, PromiseUnlessShutdown[SecureRandomness]],
      message: TransactionViewMessage,
      recipients: Recipients,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Either[
    EncryptedViewMessageError,
    NonEmpty[Seq[(WithRecipients[LightTransactionViewTree], Option[Signature])]],
  ]] =
    for {
      // For the multiple views, we can take randomness from any hash from list, it should be the same
      // TODO(#31213): Handle multiple views with the same view hash during decryption
      randomness <- randomnessMap(message.viewHashes.head1).futureUS
      decryptionResult <- decryptViewsWithRandomness(
        randomnessMap,
        message,
        randomness,
      ).value
    } yield decryptionResult.map { case (viewTrees, signature) =>
      viewTrees.map { viewTree =>
        (WithRecipients(viewTree, recipients), signature)
      }
    }

  private def decryptViewsWithRandomness(
      randomnessMap: Map[ViewHash, PromiseUnlessShutdown[SecureRandomness]],
      viewMessage: TransactionViewMessage,
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
        viewMessage,
        participantId,
        Some(randomness),
      )(
        lightTransactionViewTreeDeserializer
      )

      viewTrees = lightTransactionMultiViewTree.viewTrees

      _ = viewTrees.forgetNE.foreach { viewTree =>
        viewTree.subviewHashesAndKeys
          .foreach { case ViewHashAndKey(subviewHash, subviewKey) =>
            randomnessMap.get(subviewHash) match {
              case Some(promise) => storeRandomness(subviewHash, subviewKey, promise)
              case None =>
                // It is enough to alarm here.
                // The view will be filtered out when attempting to construct a FullTransactionViewTree.
                SyncServiceAlarm
                  .Warn(
                    s"View ${viewTree.viewHash} lists a subview with hash $subviewHash, but I haven't received any views for this hash"
                  )
                  .report()
            }
          }
      }
    } yield (viewTrees, viewMessage.submittingParticipantSignature)

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

  private def lightTransactionViewTreeDeserializer(
      bytes: ByteString
  ): Either[DefaultDeserializationError, LightTransactionViewTree] =
    LightTransactionViewTree
      .fromByteString(
        (pureCrypto, EncryptedViewMessage.computeRandomnessLength(pureCrypto)),
        protocolVersion,
      )(
        bytes
      )
      .leftMap(err => DefaultDeserializationError(err.message))
}
