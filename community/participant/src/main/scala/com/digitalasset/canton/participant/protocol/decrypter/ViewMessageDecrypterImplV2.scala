// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import cats.Monoid
import cats.data.{Chain, EitherT}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{
  Hash,
  SecureRandomness,
  Signature,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.LightTransactionViewTree.SubviewReferenceAndKey
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.{
  ByCiphertextId,
  ByViewHash,
  LightTransactionViewTree,
  LightTransactionViewTreeDeserializationContext,
  ViewTree,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  DecryptedViewData,
  DecryptedViews,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessor.TransactionProcessorError
import com.digitalasset.canton.participant.protocol.decrypter.ViewMessageDecrypterImplV2.DecryptedViewsChained
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.SynchronizerLimits
import com.digitalasset.canton.protocol.messages.{
  EncryptedViewMessage,
  EncryptedViewMessageError,
  MultipleViewTrees,
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
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** Decrypts encrypted transaction views and recursively resolves all referenced subviews using
  * ciphertext-ID-based references introduced in PV`transparency`+.
  *
  * The decrypter:
  *   - decrypts view randomness for any view that can be decrypted with its own private key.
  *   - recursively decrypts referenced subviews
  *   - accumulates all successfully decrypted views and decryption errors
  *
  * Decryption results are accumulated independently of failures so that partial decryption progress
  * can still be returned even if some subviews fail to decrypt.
  */
private[decrypter] class ViewMessageDecrypterImplV2(
    participantId: ParticipantId,
    sessionKeyStore: ConfirmationRequestSessionKeyStore,
    snapshot: SynchronizerSnapshotSyncCryptoApi,
    protocolVersion: ProtocolVersion,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val pureCrypto = snapshot.pureCrypto

  /** Cache of in-flight or completed decryption attempts per ciphertext and decryption key.
    *
    * For a given ciphertext and decryption key pair, we store the corresponding decryption Future
    * together with a boolean indicating whether the decryption result is the first successful
    * result for that view.
    *
    * This allows:
    *   - avoiding duplicate concurrent decryption work for the same ciphertext and key
    *   - reusing or short-circuiting based on whether we have successfully decrypted the ciphertext
    *     with a given key
    *
    * Note: the Future is chained in such a way that we retry each decryption attempt with the same
    * key until the first that succeeds and subsequent futures simply return the latest decryption
    * result.
    */
  private[canton] val underDecryption: TrieMap[
    (Hash, SecureRandomness),
    FutureUnlessShutdown[
      Either[Chain[
        EncryptedViewMessageError
      ], (MultipleViewTrees[LightTransactionViewTree], Boolean)]
    ],
  ] = TrieMap.empty

  /** Stores encrypted view messages indexed by ciphertext ID.
    *
    * Throws an exception or crashes if multiple envelopes share the same ciphertext ID (e.g.,
    * differing encrypted randomness values or presence of signatures). This prevents
    * non-deterministic behavior where the system simply retains whichever envelope it processes
    * first.
    */
  private[canton] val ciphertextIdsMap: TrieMap[
    Hash,
    OpenEnvelope[EncryptedViewMessage[TransactionViewType]],
  ] = TrieMap.empty

  /** Remembers the first randomness observed for each ciphertext ID. A ciphertext ID must not be
    * associated with more than one randomness value.
    */
  private[canton] val ciphertextRandomnessMap: TrieMap[Hash, SecureRandomness] = TrieMap.empty

  private def decryptSubviewsAndMergeResults(
      parent: MultipleViewTrees[LightTransactionViewTree],
      parentCiphertextId: Hash,
      submittingParticipantSignature: Option[Signature],
      recipients: Recipients,
      synchronizerLimits: SynchronizerLimits,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[DecryptedViewsChained[LightTransactionViewTree]] = {
    val decryptedSubviews = parent.viewTrees.forgetNE.map { viewTree =>
      // For each decrypted view, recursively decrypt all referenced subviews.
      // Each view may reference multiple subviews via ciphertext IDs.
      MonadUtil.parTraverseWithLimit(pureCrypto.encryptionParallelism)(
        viewTree.subviewReferencesAndKeys
          .map {
            case SubviewReferenceAndKey(ByCiphertextId(ciphertextId, _), subviewKey) =>
              ciphertextIdsMap.get(ciphertextId) match {
                case Some(encryptedSubviewsEnvelope) =>
                  Right((ciphertextId, subviewKey, encryptedSubviewsEnvelope))
                case None =>
                  Left(
                    DecryptedViewsChained[LightTransactionViewTree](
                      Chain.empty,
                      Chain.one(
                        EncryptedViewMessageError.InvalidSubviewReferenceError(
                          s"Invalid subview reference in view ${viewTree.viewHash}: ciphertext ID $ciphertextId not found"
                        )
                      ),
                    )
                  )
              }
            // PV<transparency>+ invariant: subview references must be ciphertext-based.
            // View-hash-based references are considered invalid in this decryption mode.
            case SubviewReferenceAndKey(ByViewHash(_), _) =>
              ErrorUtil.invalidState(
                s"Invalid subview reference in view ${viewTree.viewHash}: expected a ciphertext ID, but got a view hash"
              )
          }
      ) {
        case Left(error) =>
          FutureUnlessShutdown.pure(error)
        case Right((ciphertextId, subviewKey, encryptedSubviewsEnvelope)) =>
          decryptMessageWithRandomness(
            encryptedSubviewsEnvelope.protocolMessage,
            encryptedSubviewsEnvelope.recipients,
            ciphertextId,
            subviewKey,
            synchronizerLimits,
          )
      }
    }

    // Combine recursively decrypted subviews with the views decrypted at the current level
    // into a single accumulated result containing all decrypted views and decryption errors.
    decryptedSubviews.sequence.map { ds =>
      val decryptedSubviewsSeq = ds.flatten
      decryptedSubviewsSeq
        .prepended(
          // Prepend the views decrypted at the current level before combining them
          // with all recursively decrypted subviews. If a child view fails to decrypt with the provided randomness,
          // then the parent view is considered invalid as well, and we report a decryption error.
          if (decryptedSubviewsSeq.exists(_.decryptionErrors.nonEmpty))
            DecryptedViewsChained[LightTransactionViewTree](
              Chain.empty,
              Chain.one(
                EncryptedViewMessageError.InvalidSubviewReferenceError(
                  s"Parent view ${parent.viewTrees.map(_.viewHash).mkString(", ")} is invalid " +
                    s"because a subview failed to decrypt"
                )
              ),
            )
          else
            DecryptedViewsChained[LightTransactionViewTree](
              Chain.fromSeq(
                parent.viewTrees.forgetNE.zipWithIndex.map { case (viewTree, index) =>
                  DecryptedViewData(
                    WithRecipients(viewTree, recipients),
                    Some(ByCiphertextId(parentCiphertextId, NonNegativeInt.tryCreate(index))),
                    submittingParticipantSignature,
                  )
                }
              ),
              Chain.empty,
            )
        )
        .combineAll
    }
  }

  private def decryptMessageWithRandomness(
      encryptedViewsMessage: EncryptedViewMessage[TransactionViewType],
      recipients: Recipients,
      ciphertextId: Hash,
      randomness: SecureRandomness,
      synchronizerLimits: SynchronizerLimits,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[DecryptedViewsChained[LightTransactionViewTree]] = {

    def decryptF: FutureUnlessShutdown[
      Either[Chain[
        EncryptedViewMessageError
      ], (MultipleViewTrees[LightTransactionViewTree], Boolean)]
    ] =
      EncryptedViewMessage
        .decryptFor(
          snapshot,
          sessionKeyStore,
          encryptedViewsMessage,
          participantId,
          Some(randomness),
        )(
          LightTransactionViewTree
            .fromByteString(
              LightTransactionViewTreeDeserializationContext(
                pureCrypto,
                EncryptedViewMessage.computeRandomnessLength(pureCrypto),
                synchronizerLimits,
              ),
              protocolVersion,
            )(_)
            .leftMap(err => DefaultDeserializationError(err.message))
        )
        .bimap(
          err => Chain.one(err),
          // `true` indicates this is the first successful result of the decryption for this view with this key
          decryptedView => (decryptedView, true),
        )
        .value

    val promise = PromiseUnlessShutdown.supervised[
      Either[
        Chain[EncryptedViewMessageError],
        (MultipleViewTrees[LightTransactionViewTree], Boolean),
      ]
    ](
      "view decryption",
      futureSupervisor,
    )

    // for now, we make sure that a given ciphertext ID is only associated with the one randomness value
    ciphertextRandomnessMap
      .updateWith(ciphertextId) {
        case Some(existingRandomness) if existingRandomness != randomness =>
          ErrorUtil.internalError(
            new IllegalArgumentException(
              s"Ciphertext ID $ciphertextId has multiple encryption keys associated with it"
            )
          )
        case Some(existingRandomness) => Some(existingRandomness)
        case None => Some(randomness)
      }
      .discard

    // we must make sure that only one decryption attempt for a given ciphertext and key is in-flight at any time,
    // so we synchronize the access to the cache
    val decryptionO = underDecryption
      .updateWith((ciphertextId, randomness)) {
        case Some(runningF) =>
          // there is already another attempt in-flight with the same key,
          // we chain our decryption attempt to it so that we only perform the decryption if the previous attempt fails
          Some(runningF.flatMap {
            case Left(errorChain) =>
              decryptF.map {
                _.left.map(err => errorChain ++ err)
              }
            // skip decryption
            case v @ Right(_) =>
              FutureUnlessShutdown.pure(v.map { case (encView, _) => (encView, false) })
          })
        case None =>
          // first attempt, we complete the promise and store the result in the cache
          promise.completeWithUS(decryptF).discard
          Some(promise.futureUS)
      }
      .sequence

    for {
      lightTransactionMultiViewTreeO <- decryptionO
      res <- lightTransactionMultiViewTreeO match {
        case Some(lightTransactionMultiViewTreeE) =>
          for {
            decryptedViews <- lightTransactionMultiViewTreeE match {
              case Right((lightTransactionMultiViewTree, true)) =>
                decryptSubviewsAndMergeResults(
                  lightTransactionMultiViewTree,
                  ciphertextId,
                  encryptedViewsMessage.submittingParticipantSignature,
                  recipients,
                  synchronizerLimits,
                )
              // skip decryption, already decrypted by another thread
              case Right((_, false)) =>
                FutureUnlessShutdown.pure(
                  DecryptedViewsChained[LightTransactionViewTree](Chain.empty, Chain.empty)
                )
              case Left(err) =>
                FutureUnlessShutdown.pure(
                  DecryptedViewsChained[LightTransactionViewTree](Chain.empty, err)
                )
            }
          } yield decryptedViews
        case None =>
          ErrorUtil.invalidState(
            s"Unexpected None from decryption lookup for ciphertextId $ciphertextId; updateWith should always produce a value."
          )
      }
    } yield res
  }

  def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType]]]],
      synchronizerLimits: SynchronizerLimits,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionProcessorError, DecryptedViews[
    LightTransactionViewTree
  ]] = {
    // hash ciphertexts to retrieve the corresponding ciphertext IDs
    batch.forgetNE.foreach { envelope =>
      val ciphertextId =
        envelope.protocolMessage.encryptedViews.computeCiphertextId(snapshot.pureCrypto)

      ciphertextIdsMap
        .updateWith(ciphertextId) {
          // this is not expected to happen, and we crash to avoid inconsistent behavior
          case Some(existingEnvelope) if existingEnvelope != envelope =>
            ErrorUtil.internalError(
              new IllegalArgumentException(
                s"Duplicate envelope with the same ciphertextID $ciphertextId for participant $participantId: " +
                  s"existing envelope $existingEnvelope, " +
                  s"new envelope $envelope"
              )
            )
          // this is not expected to happen, but we handle it gracefully by preserving the existing envelopes
          case Some(existingEnvelope) =>
            // TODO(#34769): Check if this is problematic and if we should remove both the original and duplicate views from the map
            // It is enough to alarm here. The duplicate envelopes are filtered out.
            SyncServiceAlarm
              .Warn(
                s"Discarding duplicate envelope with ciphertext ID $ciphertextId for participant $participantId"
              )
              .report()
            Some(existingEnvelope) // preserve existing
          case None => Some(envelope)
        }
        .discard
    }

    // if the participant is a leaf recipient (within the recipient tree), then it means that the randomness
    // is expected to be encrypted for this participant and can be directly decrypted with its private key.
    val decryptableEnvelopes =
      ciphertextIdsMap.toSeq.filter { case (_, envelope) =>
        envelope.recipients.leafRecipients.contains(MemberRecipient(participantId))
      }

    def checkNoDuplicates[A](items: Seq[A], mkErrorMessage: A => String): Unit = {
      val duplicates = items.diff(items.distinct).distinct

      if (duplicates.nonEmpty) {
        val duplicateMessages = duplicates.map(mkErrorMessage)
        ErrorUtil.internalError(
          new IllegalArgumentException(
            s"Duplicate item(s): ${duplicateMessages.mkString("; ")}"
          )
        )
      }
    }

    EitherT.right {
      for {
        res <- MonadUtil
          .parTraverseWithLimit(pureCrypto.encryptionParallelism)(decryptableEnvelopes) {
            case (ciphertextId, encryptedViewsEnvelope) =>
              val encryptedViewMessage = encryptedViewsEnvelope.protocolMessage
              for {
                randomness <- EncryptedViewMessage
                  .decryptRandomness(
                    snapshot,
                    sessionKeyStore,
                    encryptedViewMessage,
                    participantId,
                  )
                  // TODO(#15657): Depending on the error either crash or mark the message as invalid
                  .valueOr { e =>
                    ErrorUtil.internalError(
                      new IllegalArgumentException(
                        s"Can't decrypt the randomness of the message with hash(es) ${encryptedViewMessage.viewHashes} " +
                          s"where I'm allegedly an informee. $e"
                      )
                    )
                  }
                decryptedViews <- decryptMessageWithRandomness(
                  encryptedViewMessage,
                  encryptedViewsEnvelope.recipients,
                  ciphertextId,
                  randomness,
                  synchronizerLimits,
                )
              } yield decryptedViews
          }
          .map(_.combineAll)

        viewsList = res.views.toList
        // Each decrypted view must have a unique ciphertext ID. We already guarantee that
        // a ciphertextID is unique because if we encounter a duplicate ciphertextID associated with different
        // envelopes we crash. However, we also need to check that the decrypted views themselves are unique, because
        // it is possible that multiple envelopes with different ciphertexts decrypt to the same view, which should not
        // happen.
        _ = checkNoDuplicates[WithRecipients[LightTransactionViewTree]](
          viewsList.map(_.view),
          (duplicate: WithRecipients[LightTransactionViewTree]) =>
            s"The view ${duplicate.unwrap.viewHash} has multiple encryption keys associated with it",
        )
      } yield {
        DecryptedViews(res.views.toList, res.decryptionErrors.toList)
      }
    }
  }

}

private object ViewMessageDecrypterImplV2 {

  final case class DecryptedViewsChained[V <: ViewTree](
      views: Chain[DecryptedViewData[V]],
      decryptionErrors: Chain[EncryptedViewMessageError],
  )

  object DecryptedViewsChained {

    def empty[V <: ViewTree]: DecryptedViewsChained[V] =
      DecryptedViewsChained(Chain.empty, Chain.empty)

    implicit def monoid[V <: ViewTree]: Monoid[DecryptedViewsChained[V]] =
      new Monoid[DecryptedViewsChained[V]] {

        override def empty: DecryptedViewsChained[V] =
          DecryptedViewsChained.empty[V]

        override def combine(
            x: DecryptedViewsChained[V],
            y: DecryptedViewsChained[V],
        ): DecryptedViewsChained[V] =
          DecryptedViewsChained(
            x.views ++ y.views,
            x.decryptionErrors ++ y.decryptionErrors,
          )
      }
  }

}
