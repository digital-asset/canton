// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import cats.Monoid
import cats.data.{Chain, EitherT}
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.SyncCryptoError.SyncCryptoEncryptionError
import com.digitalasset.canton.crypto.{
  AsymmetricEncrypted,
  Hash,
  HashAlgorithm,
  HashPurpose,
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
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  DecryptedViewData,
  DecryptedViews,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessor.TransactionProcessorError
import com.digitalasset.canton.participant.protocol.decrypter.ViewMessageDecrypterImplV2.{
  DecryptedViewsChained,
  PruneState,
}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.messages.{
  EncryptedViewMessage,
  EncryptedViewMessageError,
  MultipleViewTrees,
}
import com.digitalasset.canton.protocol.{SynchronizerLimits, ViewHash}
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
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}

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
  * '''Warning: an instance of this class MUST be used for exactly one batch (i.e. one call to
  * [[decryptViews]]) and then discarded.'''
  *
  * The internal maps (`underDecryption`, `ciphertextIdsMap`, `ciphertextRandomnessMap`) hold state
  * that is only meaningful within a single batch:
  *   - '''Correctness:''' the same ciphertext ID may legitimately appear in different
  *     transactions/batches and be associated with a different secure randomness in each. This
  *     could cause participants to reach different verdicts on the same request, i.e., a ledger
  *     fork.
  *   - '''Resource usage:''' the maps are never pruned, so a long-lived instance would grow without
  *     bound.
  */
private[decrypter] class ViewMessageDecrypterImplV2(
    participantId: ParticipantId,
    sessionKeyStore: ConfirmationRequestSessionKeyStore,
    snapshot: SynchronizerSnapshotSyncCryptoApi,
    withTransparencyChecks: Boolean,
    protocolVersion: ProtocolVersion,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val pureCrypto = snapshot.pureCrypto

  private type DecryptionResult =
    Either[Chain[EncryptedViewMessageError], MultipleViewTrees[LightTransactionViewTree]]

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
  private[canton] val underDecryption
      : TrieMap[(Hash, SecureRandomness), FutureUnlessShutdown[DecryptionResult]] = TrieMap.empty

  /** Stores encrypted view messages indexed by ciphertext ID.
    *
    * Throws an exception or crashes if multiple envelopes share the same ciphertext ID (e.g.,
    * differing encrypted randomness values or presence of signatures). This prevents
    * non-deterministic behavior where the system simply retains whichever envelope it processes
    * first.
    *
    * TODO(#37554): With full transparency checks, multiple envelopes with the same ciphertext ID
    * but different metadata must be handled gracefully instead of crashing; this map will need to
    * keep all of them.
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

  def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType]]]],
      synchronizerLimits: SynchronizerLimits,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionProcessorError, DecryptedViews[
    LightTransactionViewTree
  ]] = {

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

    // hash ciphertexts to retrieve the corresponding ciphertext IDs
    batch.forgetNE.foreach { envelope =>
      val ciphertextId =
        envelope.protocolMessage.encryptedViews.computeCiphertextId(snapshot.pureCrypto)

      ciphertextIdsMap.putIfAbsent(ciphertextId, envelope) match {
        case None => ()
        // TODO(#34769): The Some cases can happen and you'll have to deal with it gracefully
        //  under full transparency checks.
        case Some(existingEnvelope) if existingEnvelope != envelope =>
          ErrorUtil.internalError(
            new IllegalArgumentException(
              s"Different envelope with the same ciphertextID $ciphertextId for participant $participantId: " +
                s"existing envelope $existingEnvelope, " +
                s"new envelope $envelope"
            )
          )
        // this is not expected to happen, but we handle it gracefully by preserving the existing envelope
        case Some(_) =>
          // TODO(#34769): Check if this is problematic and if we should remove both the original and duplicate views from the map
          // It is enough to alarm here. The duplicate envelopes are filtered out.
          SyncServiceAlarm
            .Warn(
              s"Discarding duplicate envelope with ciphertext ID $ciphertextId for participant $participantId"
            )
            .report()
      }
    }

    // if the participant is a leaf recipient (within the recipient tree), then it means that the randomness
    // is expected to be encrypted for this participant and can be directly decrypted with its private key.
    val decryptableEnvelopes =
      ciphertextIdsMap.toSeq.filter { case (_, envelope) =>
        envelope.recipients.isLeafRecipient(MemberRecipient(participantId))
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

    def transparencyChecksAndDecryptF: FutureUnlessShutdown[DecryptionResult] =
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
        .leftMap(Chain.one)
        .flatMap { decryptedViews =>
          // An honest sender emits one view hash per view tree, so a mismatch is a malformed message.
          EitherT.cond[FutureUnlessShutdown](
            decryptedViews.viewTrees.sizeCompare(encryptedViewsMessage.viewHashes) == 0,
            decryptedViews,
            Chain.one(
              EncryptedViewMessageError.ViewHashesLengthMismatch(
                numViewTrees = decryptedViews.viewTrees.size,
                numViewHashes = encryptedViewsMessage.viewHashes.size,
              ): EncryptedViewMessageError
            ),
          )
        }
        .flatMap(decryptedViews =>
          if (withTransparencyChecks) {
            runTransparencyChecks(
              randomness,
              decryptedViews,
              encryptedViewsMessage.viewHashes,
              encryptedViewsMessage.viewEncryptionKeyRandomness,
            ).map(_ => decryptedViews)
          } else
            EitherT.rightT[FutureUnlessShutdown, Chain[EncryptedViewMessageError]](decryptedViews)
        )
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

    // Only one decryption per (ciphertext ID, randomness) is in flight; later callers reuse its result.
    val promise = Promise[UnlessShutdown[DecryptionResult]]()
    val newDecryptionF = FutureUnlessShutdown(promise.future)
    val (decryptionF, isNewDecryption) =
      underDecryption.putIfAbsent((ciphertextId, randomness), newDecryptionF) match {
        case None =>
          promise.completeWith(FutureUnlessShutdown.delegate(transparencyChecksAndDecryptF).unwrap)
          (newDecryptionF, true)
        case Some(existingF) => (existingF, false)
      }

    decryptionF.flatMap {
      case Right(lightTransactionMultiViewTree) if isNewDecryption =>
        decryptSubviewsAndMergeResults(
          lightTransactionMultiViewTree.viewTrees.forgetNE,
          ciphertextId,
          encryptedViewsMessage.submittingParticipantSignature,
          recipients,
          synchronizerLimits,
        )
      // skip decryption, already decrypted by another caller
      case Right(_) => FutureUnlessShutdown.pure(DecryptedViewsChained.empty)
      case Left(err) => FutureUnlessShutdown.pure(DecryptedViewsChained(Chain.empty, err))
    }
  }

  private def decryptSubviewsAndMergeResults(
      parentViewTrees: Seq[LightTransactionViewTree],
      parentCiphertextId: Hash,
      submittingParticipantSignature: Option[Signature],
      recipients: Recipients,
      synchronizerLimits: SynchronizerLimits,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[DecryptedViewsChained[LightTransactionViewTree]] = {
    val decryptedSubviews = MonadUtil.parTraverseWithLimit(pureCrypto.encryptionParallelism)(
      parentViewTrees.zipWithIndex
    ) { case (viewTree, index) =>
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
    decryptedSubviews.map(_.combineAll)
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

    val finalState =
      sortedViewsInPostOrder.foldLeft(PruneState(Set.empty, Chain.empty, Chain.empty)) {
        (state, decryptedView) =>
          val viewTree = decryptedView.view.unwrap
          val aSubviewIsInvalid = viewTree.subviewReferences.exists {
            case subviewReference: ByCiphertextId =>
              !state.validViewReferences.contains(subviewReference)
            case _ =>
              ErrorUtil.invalidState(
                s"Invalid subview reference in view ${viewTree.viewHash}: expected a ciphertext ID, but got a view hash"
              )
          }

          if (aSubviewIsInvalid)
            state.copy(errors =
              state.errors :+ EncryptedViewMessageError.InvalidSubviewReferenceError(
                s"Parent view ${viewTree.viewHash} is invalid because a subview is invalid"
              )
            )
          else
            state.copy(
              validViewReferences = state.validViewReferences + ciphertextIdOf(decryptedView),
              validViews = state.validViews :+ decryptedView,
            )
      }

    DecryptedViews(
      finalState.validViews.toList,
      (decryptionErrors ++ finalState.errors.toList).distinct,
    )
  }

  /** Runs transparency checks by verifying that other nodes receiving a given randomness value can
    * decrypt it.
    *
    * Uses deterministic encryption to re-encrypt the randomness with the other public keys and
    * compares the resulting ciphertext against the one in the message. Because deterministic
    * encryption is used, the ciphertexts must be the same.
    *
    * Successful checks are cached by the hash of the encrypted randomness values. The hash
    * determines the randomness and the public keys, but NOT the envelope's recipients or the views'
    * informees: the same encrypted randomness may legitimately appear in views with different
    * witness participants (e.g. a buggy submitter reusing randomness). The cache only covers the
    * re-encryption check; whether each key belongs to an informee is always checked against the
    * current snapshot.
    */
  private def runTransparencyChecks(
      randomness: SecureRandomness,
      decryptedViews: MultipleViewTrees[LightTransactionViewTree],
      viewHashes: NonEmpty[Seq[ViewHash]],
      viewEncryptionKeyRandomness: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Chain[EncryptedViewMessageError], Unit] = {
    lazy val viewContext = s"for message with hash(es) ${viewHashes.forgetNE.mkString(", ")}"

    // Cache key for the (context-independent) re-encryption check. Must cover everything the
    // re-encryption depends on: ciphertext, key and algorithm.
    val builder = Hash.build(HashPurpose.Transparency, HashAlgorithm.Sha256)
    builder.addIterator(
      viewEncryptionKeyRandomness.forgetNE.iterator,
      viewEncryptionKeyRandomness.size,
    )((hb, encrypted) => hb.addByteString(encrypted.toByteString(protocolVersion)))
    val viewEncryptionKeyRandomnessHash = builder.finish()

    for {
      // Context-dependent check, never cached: every entry must be for a key of an informee
      // participant in the current topology snapshot. This keeps the outcome deterministic across
      // participants, e.g. under key rotations.
      informeeParticipants <- getInformeeParticipants(decryptedViews).leftMap(Chain.one)
      keys <- EitherT.right(snapshot.ipsSnapshot.encryptionKeys(informeeParticipants))
      keysById = keys.values.flatten.map(k => k.id -> k).toMap

      (missingKeyErrors, foundEntries) =
        viewEncryptionKeyRandomness.forgetNE.partitionMap { encrypted =>
          keysById
            .get(encrypted.encryptedFor) match {
            case Some(key) => Right(encrypted -> key)
            case None =>
              Left(
                EncryptedViewMessageError.TransparencyCheckError(
                  s"No encryption key found for recipient key ID ${encrypted.encryptedFor} $viewContext"
                )
              )
          }
        }

      entriesWithKeys <- EitherT.fromEither[FutureUnlessShutdown](
        if (missingKeyErrors.isEmpty) Right(foundEntries.toMap)
        else Left(Chain.fromSeq(missingKeyErrors))
      )

      // Context-independent check, cached: re-encrypting the randomness yields the same ciphertext.
      _ <-
        if (sessionKeyStore.hasTransparencyCheck(viewEncryptionKeyRandomnessHash))
          EitherTUtil.unitUS[Chain[EncryptedViewMessageError]]
        else
          for {
            errors <- EitherT.right(
              MonadUtil
                .parTraverseWithLimit(pureCrypto.encryptionParallelism)(entriesWithKeys) {
                  case (encrypted, key) =>
                    FutureUnlessShutdown.outcomeF(Future {
                      pureCrypto.encryptDeterministicWith(
                        randomness,
                        key,
                        encrypted.encryptionAlgorithmSpec,
                      ) match {
                        case Right(reEncrypted) if reEncrypted.ciphertext == encrypted.ciphertext =>
                          Chain.empty[EncryptedViewMessageError]
                        case Right(_) =>
                          Chain.one[EncryptedViewMessageError](
                            EncryptedViewMessageError.TransparencyCheckError(
                              s"Re-encrypted ciphertext does not match the view ciphertext for recipient keyFor} $viewContext"
                            )
                          )
                        case Left(err) =>
                          Chain.one[EncryptedViewMessageError](
                            EncryptedViewMessageError.SyncCryptoEncryptError(
                              SyncCryptoEncryptionError(err)
                            )
                          )
                      }
                    })
                }
                .map(_.combineAll)
            )
            _ <- EitherT.cond[FutureUnlessShutdown](errors.isEmpty, (), errors)
          } yield sessionKeyStore.saveTransparencyCheck(viewEncryptionKeyRandomnessHash)
    } yield ()
  }

  private def getInformeeParticipants(
      decryptedViews: MultipleViewTrees[LightTransactionViewTree]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, Seq[ParticipantId]] = {
    // get all the informees of the decrypted views
    val informeeParties = decryptedViews.viewTrees.forgetNE.flatMap { viewTree =>
      viewTree.informees
    }
    // get the active participants of the informees
    snapshot.ipsSnapshot
      .activeParticipantsOfAll(informeeParties.toList)
      .bimap(
        missingParties =>
          EncryptedViewMessageError.TransparencyCheckError(
            s"Failed to get active participants for informee parties: $missingParties"
          ),
        activeParticipants => activeParticipants.toSeq,
      )
  }
}

private object ViewMessageDecrypterImplV2 {

  final case class DecryptedViewsChained[V <: ViewTree](
      views: Chain[DecryptedViewData[V]],
      decryptionErrors: Chain[EncryptedViewMessageError],
  )

  private final case class PruneState(
      validViewReferences: Set[ByCiphertextId],
      validViews: Chain[DecryptedViewData[LightTransactionViewTree]],
      errors: Chain[EncryptedViewMessageError],
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
