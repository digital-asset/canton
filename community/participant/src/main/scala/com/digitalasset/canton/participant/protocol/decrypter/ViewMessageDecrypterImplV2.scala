// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import cats.Monoid
import cats.data.{Chain, EitherT}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
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
  ViewPosition,
  ViewTree,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
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
  *
  * Note: An instance of this class must be instantiated once per decryption batch to ensure
  * internal lookup maps (i.e. `underDecryption`, `ciphertextIdsMap`) do not accumulate state
  * indefinitely.
  */
private[decrypter] class ViewMessageDecrypterImplV2(
    participantId: ParticipantId,
    sessionKeyStore: ConfirmationRequestSessionKeyStore,
    snapshot: SynchronizerSnapshotSyncCryptoApi,
    protocolVersion: ProtocolVersion,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val pureCrypto = snapshot.pureCrypto

  /** Cache of in-flight or completed decryption attempts per ciphertext and decryption key.
    *
    * For a given ciphertext and decryption key pair, we store the corresponding decryption Future.
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
  private[canton] val underDecryption: ConcurrentHashMap[
    (Hash, SecureRandomness),
    FutureUnlessShutdown[
      Either[Chain[
        EncryptedViewMessageError
      ], MultipleViewTrees[LightTransactionViewTree]]
    ],
  ] = new ConcurrentHashMap[
    (Hash, SecureRandomness),
    FutureUnlessShutdown[Either[Chain[
      EncryptedViewMessageError
    ], MultipleViewTrees[LightTransactionViewTree]]],
  ]()

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

  /** Remembers the first randomness value observed for each ciphertext ID.
    *
    * The sole purpose of this map is to ensure that a ciphertext ID is never associated with more
    * than one randomness value.
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
    val decryptedSubviews = parent.viewTrees.forgetNE.zipWithIndex.map { case (viewTree, index) =>
      val (invalidReferenceErrors, subviews) = viewTree.subviewReferencesAndKeys
        .partitionMap {
          case SubviewReferenceAndKey(ByCiphertextId(ciphertextId, _), subviewKey) =>
            ciphertextIdsMap.get(ciphertextId) match {
              case Some(encryptedSubviewsEnvelope) =>
                Right((ciphertextId, subviewKey, encryptedSubviewsEnvelope))
              case None =>
                Left(
                  EncryptedViewMessageError.InvalidSubviewReferenceError(
                    s"Invalid subview reference in view ${viewTree.viewHash}: ciphertext ID $ciphertextId not found"
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

      if (invalidReferenceErrors.isEmpty) {
        // For each decrypted view, recursively decrypt all referenced subviews.
        // Each view may reference multiple subviews via ciphertext IDs.
        MonadUtil
          .parTraverseWithLimit(pureCrypto.encryptionParallelism)(subviews) {
            case (ciphertextId, subviewKey, encryptedSubviewsEnvelope) =>
              decryptMessageWithRandomness(
                encryptedSubviewsEnvelope.protocolMessage,
                encryptedSubviewsEnvelope.recipients,
                ciphertextId,
                subviewKey,
                synchronizerLimits,
              )
          }
          .map { subviewResults =>
            val byCiphertextId =
              ByCiphertextId(parentCiphertextId, NonNegativeInt.tryCreate(index))
            // A parent view is only accumulated if all of its direct ciphertext-ID subview references exist.
            Seq(
              DecryptedViewsChained[LightTransactionViewTree](
                Chain.one(
                  DecryptedViewData(
                    WithRecipients(viewTree, recipients),
                    Some(byCiphertextId),
                    submittingParticipantSignature,
                  )
                ),
                Chain.empty,
              ),
              subviewResults.combineAll,
            ).combineAll
          }
      } else
        FutureUnlessShutdown.pure(
          DecryptedViewsChained[LightTransactionViewTree](
            views = Chain.empty,
            decryptionErrors = Chain.fromSeq(invalidReferenceErrors),
          )
        )
    }

    // Combine recursively decrypted subviews with the views decrypted at the current level
    // into a single accumulated result containing all decrypted views and decryption errors.
    decryptedSubviews.sequence.map(_.combineAll)
  }

  /** Decrypts an encrypted message using a provided randomness key, deduplicating concurrent
    * decryption attempts for the same (ciphertextId, randomness) pair.
    *
    * If multiple threads attempt decryption with the same key, only the first thread that decrypts
    * the payload will emit the result. Subsequent threads will re-use this result.
    */
  private def decryptMessageWithRandomness(
      encryptedViewsMessage: EncryptedViewMessage[TransactionViewType],
      recipients: Recipients,
      ciphertextId: Hash,
      randomness: SecureRandomness,
      synchronizerLimits: SynchronizerLimits,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[DecryptedViewsChained[LightTransactionViewTree]] = {

    def transparencyChecksAndDecryptF: FutureUnlessShutdown[
      Either[Chain[
        EncryptedViewMessageError
      ], MultipleViewTrees[LightTransactionViewTree]]
    ] =
      // TODO(#34787): Add transparency checks here
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
        .leftMap(err => Chain.one(err))
        .value

    // TODO(#15657): If transparency is enabled, we probably don't need to crash here for now, we make sure that a
    // given ciphertext ID is only associated with the one randomness value
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
    val isNewDecryption = new AtomicBoolean(false)
    val decryptionF = underDecryption
      .computeIfAbsent(
        (ciphertextId, randomness),
        _ => {
          isNewDecryption.set(true)
          transparencyChecksAndDecryptF
        },
      )

    decryptionF.flatMap { viewsE =>
      val firstDecryption = isNewDecryption.get()
      viewsE match {
        case Right(lightTransactionMultiViewTree) if firstDecryption =>
          decryptSubviewsAndMergeResults(
            lightTransactionMultiViewTree,
            ciphertextId,
            encryptedViewsMessage.submittingParticipantSignature,
            recipients,
            synchronizerLimits,
          )
        // skip decryption, already decrypted by another thread
        case Right(_) =>
          FutureUnlessShutdown.pure(
            DecryptedViewsChained[LightTransactionViewTree](Chain.empty, Chain.empty)
          )
        case Left(err) =>
          FutureUnlessShutdown.pure(
            DecryptedViewsChained[LightTransactionViewTree](Chain.empty, err)
          )
      }
    }
  }

  private def pruneViewsWithInvalidDescendants(
      views: List[DecryptedViewData[LightTransactionViewTree]],
      decryptionErrors: List[EncryptedViewMessageError],
  )(implicit traceContext: TraceContext): DecryptedViews[LightTransactionViewTree] = {

    def ciphertextIdOf(viewData: DecryptedViewData[LightTransactionViewTree]): ByCiphertextId =
      viewData.ciphertextIdO.getOrElse(
        ErrorUtil.invalidState(
          s"Missing ciphertext ID for decrypted view ${viewData.view.unwrap.viewHash}."
        )
      )

    val sortedViewsInPostOrder =
      views
        .sortBy(_.view.unwrap.viewPosition)(ViewPosition.orderViewPosition.toOrdering)
        .reverse

    val (_, validViews, propagatedErrors) =
      sortedViewsInPostOrder.foldLeft(
        (
          Set.empty[ByCiphertextId],
          Seq.empty[DecryptedViewData[LightTransactionViewTree]],
          List.empty[EncryptedViewMessageError],
        )
      ) { case ((validCiphertextIds, currentlyValidViews, accumulatedErrors), decryptedView) =>
        val viewTree = decryptedView.view.unwrap
        val viewReference = ciphertextIdOf(decryptedView)
        val aSubviewIsInvalid = viewTree.subviewReferences.exists {
          case subviewReference: ByCiphertextId => !validCiphertextIds.contains(subviewReference)
          case _ =>
            ErrorUtil.invalidState(
              s"Invalid subview reference in view ${viewTree.viewHash}: expected a ciphertext ID, but got a view hash"
            )
        }

        if (aSubviewIsInvalid) {
          (
            validCiphertextIds,
            currentlyValidViews,
            accumulatedErrors :+ EncryptedViewMessageError.InvalidSubviewReferenceError(
              s"Parent view ${viewTree.viewHash} is invalid because a subview is invalid"
            ),
          )
        } else {
          (
            validCiphertextIds + viewReference,
            currentlyValidViews :+ decryptedView,
            accumulatedErrors,
          )
        }
      }

    DecryptedViews(validViews, (decryptionErrors ++ propagatedErrors).distinct)
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
          // TODO(#34769): Should we crash here?
          // this is not expected to happen, and we crash to avoid inconsistent behavior
          case Some(existingEnvelope) if existingEnvelope != envelope =>
            ErrorUtil.internalError(
              new IllegalArgumentException(
                s"Different envelope with the same ciphertextID $ciphertextId for participant $participantId: " +
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
        // we start by decrypting all envelopes directly decryptable by this participant and then recursively decrypt
        // any subviews referenced by those views. If either the decryption of the envelope or the transparency checks
        // fail, we record the error and continue with the next envelope.
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

        // Remove any views whose descendant trees are not valid.
        filteredRes = pruneViewsWithInvalidDescendants(viewsList, res.decryptionErrors.toList)

      } yield filteredRes
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
