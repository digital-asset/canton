// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.{Chain, EitherT}
import cats.instances.either.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.LoggingConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner.SigningTimestampOverrides
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.GenTransactionTree.ViewWithWitnessesAndRecipients
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo.ExternallySignedSubmission
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.{
  ViewHashAndRecipients,
  ViewKeyDataMap,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionConfirmationRequestFactory.*
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  ContractInstanceOfId,
  TransactionTreeConversionError,
}
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker.ReferenceToFutureContractError
import com.digitalasset.canton.participant.protocol.validation.{
  ContractConsistencyChecker,
  ExtractUsedContractsFromRootViews,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.{
  MediatorGroupRecipient,
  OpenEnvelope,
  Recipients,
}
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ContractHasher, ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString

import scala.annotation.unused
import scala.concurrent.ExecutionContext

/** Factory class for creating transaction confirmation requests from Daml-LF transactions.
  *
  * @param transactionTreeFactory
  *   used to create the payload
  * @param seedGenerator
  *   used to derive the transaction seed
  * @param parallel
  *   to flag if view processing is done in parallel or sequentially. Intended to be set only during
  *   tests to enforce determinism, otherwise it is always set to true.
  *
  * TODO(#32393): Remove `useNewEncryptionAlgorithm` and refactor to have separate paths for v1 and
  * v2 encryption algorithms.
  */
class TransactionConfirmationRequestFactory(
    submitterNode: ParticipantId,
    loggingConfig: LoggingConfig,
    val loggerFactory: NamedLoggerFactory,
    parallel: Boolean = true,
    useNewEncryptionAlgorithm: Boolean = false,
)(val transactionTreeFactory: TransactionTreeFactory, seedGenerator: SeedGenerator)(implicit
    executionContext: ExecutionContext
) extends NamedLogging {

  /** Creates a confirmation request from a well-formed transaction.
    *
    * @param cryptoSnapshot
    *   used to determine participants of parties and for signing and encryption
    * @return
    *   the confirmation request and the transaction root hash (aka transaction id) or an error. See
    *   the documentation of
    *   [[com.digitalasset.canton.participant.protocol.submission.TransactionConfirmationRequestFactory.TransactionConfirmationRequestCreationError]]
    *   for more information on error cases.
    */
  def createConfirmationRequest(
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      submitterInfo: SubmitterInfo,
      workflowId: Option[WorkflowId],
      mediator: MediatorGroupRecipient,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
      approximateTimestampForSigning: CantonTimestamp,
      sessionKeyStore: SessionKeyStore,
      contractInstanceOfId: ContractInstanceOfId,
      maxSequencingTime: CantonTimestamp,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionConfirmationRequestCreationError,
    TransactionConfirmationRequest,
  ] = {
    // For externally signed transaction, the transactionUUID is generated during "prepare" and is part of the hash,
    // so we use that one.
    val transactionUuid = submitterInfo.externallySignedSubmission
      .map(_.transactionUUID)
      .getOrElse(seedGenerator.generateUuid())
    val ledgerTime = wfTransaction.metadata.ledgerTime

    for {
      _ <- assertPartiesCanSubmit(
        submitterInfo,
        cryptoSnapshot,
      )

      transactionSeed = seedGenerator.generateSaltSeed()

      transactionTree <- transactionTreeFactory
        .createTransactionTree(
          wfTransaction,
          submitterInfo,
          workflowId,
          mediator,
          transactionSeed,
          transactionUuid,
          cryptoSnapshot.ipsSnapshot,
          contractInstanceOfId,
          maxSequencingTime,
          validatePackageVettings = true,
        )
        .leftMap(TransactionTreeFactoryError.apply)

      rootViews = transactionTree.rootViews.unblindedElements.toList
      inputContracts = ExtractUsedContractsFromRootViews(rootViews)
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        ContractConsistencyChecker
          .assertInputContractsInPast(inputContracts, ledgerTime)
          .leftMap(errs => ContractConsistencyError(errs))
      )

      confirmationRequest <- createConfirmationRequest(
        transactionTree,
        cryptoSnapshot,
        Some(
          SigningTimestampOverrides(
            approximateTimestampForSigning,
            Some(maxSequencingTime),
          )
        ),
        sessionKeyStore,
        protocolVersion,
      )
    } yield confirmationRequest
  }

  def createConfirmationRequest(
      transactionTree: GenTransactionTree,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
      signingTimestampOverrides: Option[SigningTimestampOverrides],
      sessionKeyStore: SessionKeyStore,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionConfirmationRequestCreationError,
    TransactionConfirmationRequest,
  ] =
    for {
      transactionViewEnvelopes <- createTransactionViewEnvelopes(
        transactionTree,
        cryptoSnapshot,
        signingTimestampOverrides,
        sessionKeyStore,
        protocolVersion,
      )
      submittingParticipantSignature <- cryptoSnapshot
        .sign(
          transactionTree.rootHash.unwrap,
          SigningKeyUsage.ProtocolOnly,
          signingTimestampOverrides,
        )
        .leftMap[TransactionConfirmationRequestCreationError](TransactionSigningError.apply)
    } yield {
      if (loggingConfig.eventDetails) {
        logger.debug(
          s"Transaction tree is ${loggingConfig.api.printer.printAdHoc(transactionTree)}"
        )
      }
      TransactionConfirmationRequest(
        InformeeMessage(
          transactionTree.tryFullInformeeTree(protocolVersion),
          submittingParticipantSignature,
        )(protocolVersion),
        transactionViewEnvelopes,
        protocolVersion,
      )
    }

  private def assertNonLocalPartiesCanSubmit(
      submitterInfo: SubmitterInfo,
      externallySignedSubmission: ExternallySignedSubmission,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantAuthorizationError, Unit] = {
    // Signatures have been validated in the Ledger API when the execute request was received.
    // Therefore, we only do authorization checks at this point
    val signedAs = externallySignedSubmission.signatures.keySet.map(_.toLf)
    val unauthorized = submitterInfo.actAs.toSet -- signedAs
    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        unauthorized.isEmpty,
        (),
        ParticipantAuthorizationError(
          s"External authorization has not been provided for: $unauthorized"
        ),
      )
      notHosted <- EitherT
        .liftF(cryptoSnapshot.ipsSnapshot.hasNoConfirmer(signedAs))
      _ <- EitherT.cond[FutureUnlessShutdown](
        notHosted.isEmpty,
        (),
        ParticipantAuthorizationError(
          s"The following parties are not hosted with confirmation rights on the synchronizer: $notHosted"
        ),
      )
    } yield ()
  }

  private def assertPartiesCanSubmit(
      submitterInfo: SubmitterInfo,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantAuthorizationError, Unit] =
    submitterInfo.externallySignedSubmission match {
      case Some(e) =>
        assertNonLocalPartiesCanSubmit(
          submitterInfo,
          e,
          cryptoSnapshot,
        )
      case None => assertLocalPartiesCanSubmit(submitterInfo.actAs, cryptoSnapshot.ipsSnapshot)
    }

  private def assertLocalPartiesCanSubmit(
      submitters: List[LfPartyId],
      identities: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantAuthorizationError, Unit] =
    EitherT(
      identities
        .hostedOn(submitters.toSet, submitterNode)
        .map { partyWithParticipants =>
          submitters
            .traverse(submitter =>
              partyWithParticipants
                .get(submitter)
                .toRight(
                  ParticipantAuthorizationError(
                    s"$submitterNode does not host $submitter or is not active."
                  )
                )
                .flatMap { relationship =>
                  Either.cond(
                    relationship.permission == Submission,
                    (),
                    ParticipantAuthorizationError(
                      s"$submitterNode is not authorized to submit transactions for $submitter."
                    ),
                  )
                }
            )
            .void
        }
    )

  private def createTransactionViewEnvelopes(
      transactionTree: GenTransactionTree,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
      signingTimestampOverrides: Option[SigningTimestampOverrides],
      sessionKeyStore: SessionKeyStore,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionConfirmationRequestCreationError, Seq[
    OpenEnvelope[TransactionViewMessage]
  ]] = {
    val pureCrypto = cryptoSnapshot.pureCrypto

    // We would like to keep the original order of the trees, so using groupMap alone won't work
    def groupLightTransactionViewTreesByRecipientsWithOrder(
        lightTreesNE: NonEmpty[Seq[(Recipients, LightTransactionViewTree)]]
    ): Seq[(Recipients, NonEmpty[Seq[LightTransactionViewTree]])] = {
      val groupedUnordered = lightTreesNE.groupMap1 { case (recipients, _) => recipients } {
        case (_, lightTree) => lightTree
      }
      val orderedRecipients = lightTreesNE.map(_._1).distinct

      orderedRecipients.map { recipients =>
        recipients -> groupedUnordered.getOrElse(
          recipients,
          // This should never happen, but let's handle it the best way possible
          // (which is logging an error and throwing an exception)
          ErrorUtil.invalidState(
            s"Missing key $recipients when grouping light transaction view trees by recipients."
          ),
        )
      }
    }

    @unused
    /* Creates encrypted open envelopes for transaction views using Ciphertext ID-based encryption
     * (PV36+).
     *
     * This version replaces view-hash-based references with ciphertext IDs derived from the encrypted payload,
     * ensuring correctness even when view hashes are not unique during decryption. Ciphertext IDs correspond
     * to the hash of the ciphertext and the position of the view in the encrypted list of views.
     * This provides a unique, stable, and non-duplicable reference to an encrypted view, which can be
     * used by parent views to refer to their subviews.
     *
     * Only supported for protocol versions > v35.
     */
    def createOpenEnvelopesWithTransactionV2(
        viewsWithWitnessesAndRecipients: NonEmpty[Seq[ViewWithWitnessesAndRecipients]],
        viewKeyDataMap: ViewKeyDataMap,
    ): EitherT[FutureUnlessShutdown, TransactionConfirmationRequestCreationError, Seq[
      OpenEnvelope[EncryptedViewMessage[TransactionViewType.type]]
    ]] = {
      require(
        protocolVersion > ProtocolVersion.v35,
        s"Ciphertext ID-based subview references are only supported for protocol versions > v35 (got $protocolVersion)",
      )

      // Build light view trees for a recipient group, using the subview references generated for
      // the previous groups when it's necessary to refer to subviews.
      def createLightTransactionViewTreesWithSameRecipients(
          viewsWithSameRecipients: NonEmpty[Seq[ViewWithWitnessesAndRecipients]],
          byCiphertextIdMap: Map[ViewHash, ByCiphertextId],
      ): Either[
        TransactionConfirmationRequestCreationError,
        NonEmpty[Seq[LightTransactionViewTree]],
      ] =
        viewsWithSameRecipients.toNEF.traverse { viewWithSameRecipients =>
          val subviewsRandomness = viewWithSameRecipients.view.subviewHashes
            .map(subviewHash => viewKeyDataMap.randomnessByHash(subviewHash))

          LightTransactionViewTree
            .fromTransactionViewTreeUsingCiphertextIdReference(
              viewWithSameRecipients.view,
              subviewsRandomness,
              byCiphertextIdMap,
              protocolVersion,
            )
            .leftMap[TransactionConfirmationRequestCreationError](
              LightTransactionViewTreeCreationError.apply
            )
        }

      // Create a deterministic ciphertext ID for a view by hashing the ciphertext.
      def createCiphertextId(ciphertext: ByteString): Hash = {
        val hashBuilder =
          HashBuilderFromMessageDigest(HashAlgorithm.Sha256, HashPurpose.CiphertextId)
        hashBuilder.addByteString(ciphertext).finish()
      }

      // Create `LightTransactionViewTrees` and encrypt a list of views that share the same recipient tree group.
      def processGroupOfRecipients(
          state: ViewEncryptionAccumulator,
          grouped: (Recipients, NonEmpty[Seq[ViewWithWitnessesAndRecipients]]),
      ): EitherT[
        FutureUnlessShutdown,
        TransactionConfirmationRequestCreationError,
        (
            Seq[(ViewHash, ByCiphertextId)],
            OpenEnvelope[EncryptedViewMessage[TransactionViewType.type]],
        ),
      ] = {

        val (recipients, views) = grouped

        for {

          // Build light transaction view trees (lvt) for each view
          lightTrees <- EitherT.fromEither[FutureUnlessShutdown](
            createLightTransactionViewTreesWithSameRecipients(
              views,
              state.byCiphertextIdMap,
            )
          )

          // Encrypt all views together
          encrypted <- EncryptedViewMessageFactory
            .encryptGroupedViews(TransactionViewType)(
              lightTrees,
              viewKeyDataMap.keyAndEncryptedRandomnessByRecipients(recipients),
              cryptoSnapshot,
              signingTimestampOverrides,
              protocolVersion,
            )
            .leftMap[TransactionConfirmationRequestCreationError](
              EncryptedViewMessageCreationError.apply
            )

          ciphertextId = createCiphertextId(encrypted.encryptedViews.viewTrees.ciphertext)

          ids = lightTrees.zipWithIndex.map { case (lvt, i) =>
            // To use it as a view reference, we combine this ciphertext ID with the relative
            // position of the view within the encrypted list of views.
            lvt.viewHash -> ByCiphertextId(
              ciphertextId = ciphertextId,
              index = NonNegativeInt.tryCreate(i),
            )
          }
        } yield (ids.forgetNE, OpenEnvelope(encrypted, recipients)(protocolVersion))
      }

      if (!viewsWithWitnessesAndRecipients.forall(_.recipients.trees.lengthCompare(1) == 0))
        ErrorUtil.invalidState("Expected all views to have exactly one recipient tree in Phase 1")

      // Group views by recipient tree depth, largest first (i.e. post-order), to ensure that when encrypting a view with
      // subviews, the ciphertext IDs for all subviews are already generated by the time they are referenced.
      // Ordering is non-deterministic within the same group, but that doesn't matter since views with the same
      // recipient tree depth can't reference each other.
      val groupedByRecipients =
        viewsWithWitnessesAndRecipients
          .groupBy(_.recipients)
          .toSeq

      val groupedByDecreasingDepth =
        groupedByRecipients
          // all views must have exactly one recipient tree in Phase 1
          .groupBy { case (recipients, _) => recipients.trees.head1.depth }
          .toSeq
          .sortBy(_._1)(Ordering[Int].reverse)

      // Fold over groups of recipients and accumulate ciphertext IDs and envelopes, ensuring that groups with larger
      // recipient trees are processed first (i.e. post-order).
      MonadUtil
        .foldLeftM(
          initialState = ViewEncryptionAccumulator(Map.empty, Chain.empty),
          groupedByDecreasingDepth,
        ) { case (acc, (_, groupsWithSameDepth)) =>
          val results =
            if (parallel)
              // Process all groups (including encryption) of the same depth in parallel
              MonadUtil
                .parTraverseWithLimit(pureCrypto.encryptionParallelism)(groupsWithSameDepth.toNEF) {
                  group =>
                    processGroupOfRecipients(acc, group)
                }
            else
              // The only reason for ordering the views within a group by their string representation and processing
              // these groups sequentially is to maintain a deterministic order of the resulting envelopes, and this
              // is only used for testing.
              MonadUtil.sequentialTraverse(
                groupsWithSameDepth.sortBy { case (recipients, _) => recipients.toString }.toNEF
              ) { group =>
                processGroupOfRecipients(acc, group)
              }

          // Merge resulting encrypted views and computed ciphertext IDs.
          results.map {
            _.foldLeft(acc) { case (merged, (ids, envelope)) =>
              ViewEncryptionAccumulator(
                merged.byCiphertextIdMap ++ ids,
                merged.envelopes :+ envelope,
              )
            }
          }
        }
        .map(_.envelopes.toList)
    }

    /* Creates encrypted open envelopes for transaction views using viewHash-based references.
     *
     * This is the legacy encryption flow where subviews are referenced using their viewHash. It
     * assumes view hashes are unique and stable during decryption.
     *
     * Used for protocol versions <= v35.
     */
    def createOpenEnvelopesWithTransactionV1(
        viewsWithWitnessesAndRecipients: NonEmpty[Seq[ViewWithWitnessesAndRecipients]],
        viewKeyDataMap: ViewKeyDataMap,
    ): EitherT[FutureUnlessShutdown, TransactionConfirmationRequestCreationError, Seq[
      OpenEnvelope[EncryptedViewMessage[TransactionViewType.type]]
    ]] = {
      // TODO(#32393): Make sure this is only executed for protocol versions that support viewHash-based encryption
      /*require(
        protocolVersion <= ProtocolVersion.v35,
        s"ViewHash-based encryption is only supported for protocol versions <= v35 (got $protocolVersion)",
      )*/

      def makeLightTransactionViewTreeWithRecipient(
          viewWithWitnessesAndRecipients: ViewWithWitnessesAndRecipients
      ): Either[
        TransactionConfirmationRequestCreationError,
        (Recipients, LightTransactionViewTree),
      ] = {
        val randomness = viewWithWitnessesAndRecipients.view.subviewHashes
          .map(subviewHash => viewKeyDataMap.randomnessByHash(subviewHash))

        LightTransactionViewTree
          .fromTransactionViewTreeUsingViewHashReference(
            viewWithWitnessesAndRecipients.view,
            randomness,
            protocolVersion,
          )
          .leftMap[TransactionConfirmationRequestCreationError](
            LightTransactionViewTreeCreationError.apply
          )
          .map { result =>
            (viewWithWitnessesAndRecipients.recipients, result)
          }
      }

      @SuppressWarnings(Array("org.wartremover.warts.PartialFunctionApply"))
      def createOpenEnvelopes(
          lightTreesByRecipients: Seq[(Recipients, NonEmpty[Seq[LightTransactionViewTree]])]
      ): EitherT[FutureUnlessShutdown, TransactionConfirmationRequestCreationError, Seq[
        OpenEnvelope[EncryptedMultipleViewsMessage[TransactionViewType.type]]
      ]] = {
        def encryptViews(
            lightTrees: NonEmpty[Seq[LightTransactionViewTree]],
            recipients: Recipients,
        ) =
          for {
            viewMessage <- EncryptedViewMessageFactory
              .encryptGroupedViews(TransactionViewType)(
                lightTrees,
                viewKeyDataMap.keyAndEncryptedRandomnessByRecipients(recipients),
                cryptoSnapshot,
                signingTimestampOverrides,
                protocolVersion,
              )
              .leftMap[TransactionConfirmationRequestCreationError](
                EncryptedViewMessageCreationError.apply
              )
          } yield OpenEnvelope(viewMessage, recipients)(protocolVersion)

        if (parallel)
          MonadUtil.parTraverseWithLimit(pureCrypto.encryptionParallelism)(lightTreesByRecipients) {
            case (recipients, lightTrees) =>
              encryptViews(lightTrees, recipients)
          }
        else
          MonadUtil
            .sequentialTraverse(lightTreesByRecipients) { case (recipients, lightTrees) =>
              encryptViews(lightTrees, recipients)
            }
      }

      val lightTreesWithRecipientsE = viewsWithWitnessesAndRecipients.toNEF
        .traverse(makeLightTransactionViewTreeWithRecipient)

      if (protocolVersion >= ProtocolVersion.v35) {
        val lightTreesByRecipientsE =
          lightTreesWithRecipientsE.map(groupLightTransactionViewTreesByRecipientsWithOrder)

        for {
          lightTreesByRecipients <- EitherT.fromEither[FutureUnlessShutdown](
            lightTreesByRecipientsE
          )
          envelopes <- createOpenEnvelopes(lightTreesByRecipients)
        } yield envelopes
      } else {
        for {
          lightTreeWithRecipients <- EitherT.fromEither[FutureUnlessShutdown](
            lightTreesWithRecipientsE
          )
          recipients = lightTreeWithRecipients.map(_._1)
          messages <- EncryptedViewMessageFactory
            .encryptNonGroupedViews(TransactionViewType)(
              lightTreeWithRecipients,
              viewKeyDataMap,
              cryptoSnapshot,
              signingTimestampOverrides,
              protocolVersion,
              parallel,
            )
            .leftMap[TransactionConfirmationRequestCreationError](
              EncryptedViewMessageCreationError.apply
            )
        } yield messages.zip(recipients).map { case (message, recipients) =>
          OpenEnvelope(message, recipients)(protocolVersion)
        }
      }
    }

    for {
      viewsWithWitnessesAndRecipients <- transactionTree
        .allTransactionViewTreesWithRecipients(cryptoSnapshot.ipsSnapshot)
        .leftMap[TransactionConfirmationRequestCreationError](e =>
          RecipientsCreationError(e.message)
        )

      viewsKeyDataMap <- EncryptedViewMessageFactory
        .generateKeysFromRecipients(
          viewsWithWitnessesAndRecipients.map {
            case ViewWithWitnessesAndRecipients(tvt, _, recipients, parentRecipients) =>
              (
                ViewHashAndRecipients(tvt.viewHash, recipients),
                parentRecipients,
                tvt.informees.toList,
              )
          },
          parallel,
          pureCrypto,
          cryptoSnapshot,
          sessionKeyStore.convertStore,
        )
        .leftMap[TransactionConfirmationRequestCreationError](e =>
          EncryptedViewMessageCreationError(e)
        )

      viewsWithWitnessesAndRecipientsNE <- EitherT.fromEither[FutureUnlessShutdown](
        NonEmpty
          .from(viewsWithWitnessesAndRecipients)
          .toRight[TransactionConfirmationRequestCreationError](
            NoViewsToEncryptError("There are no view to encrypt")
          )
      )

      envelopes <-
        if (!useNewEncryptionAlgorithm)
          createOpenEnvelopesWithTransactionV1(
            viewsWithWitnessesAndRecipientsNE,
            viewsKeyDataMap,
          )
        else {
          createOpenEnvelopesWithTransactionV2(
            viewsWithWitnessesAndRecipientsNE,
            viewsKeyDataMap,
          )
        }

    } yield envelopes
  }
}

object TransactionConfirmationRequestFactory {
  def apply(
      submitterNode: ParticipantId,
      synchronizerId: PhysicalSynchronizerId,
  )(
      cryptoOps: HashOps & HmacOps,
      hasher: ContractHasher,
      seedGenerator: SeedGenerator,
      loggingConfig: LoggingConfig,
      useLegacyContractIdVersionV11: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): TransactionConfirmationRequestFactory = {

    val transactionTreeFactory = TransactionTreeFactory(
      submitterNode,
      synchronizerId,
      // TODO(#23971): Make this dependent on the protocol version when introducing V2 contract IDs
      if (useLegacyContractIdVersionV11) AuthenticatedContractIdVersionV11
      else AuthenticatedContractIdVersionV12,
      cryptoOps,
      hasher,
      loggerFactory,
    )

    new TransactionConfirmationRequestFactory(submitterNode, loggingConfig, loggerFactory)(
      transactionTreeFactory,
      seedGenerator,
    )
  }

  // Accumulator for ciphertext IDs + final envelopes
  final private case class ViewEncryptionAccumulator(
      byCiphertextIdMap: Map[ViewHash, ByCiphertextId],
      envelopes: Chain[OpenEnvelope[EncryptedViewMessage[TransactionViewType.type]]],
  )

  /** Superclass for all errors that may arise during the creation of a confirmation request.
    */
  sealed trait TransactionConfirmationRequestCreationError
      extends Product
      with Serializable
      with PrettyPrinting

  /** Indicates that the submitterNode is not allowed to represent the submitter or to submit
    * requests.
    */
  final case class ParticipantAuthorizationError(message: String)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[ParticipantAuthorizationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that the submitter could not be mapped to a party.
    */
  final case class MalformedSubmitter(message: String)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[MalformedSubmitter] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that the given transaction is contract-inconsistent.
    */
  final case class ContractConsistencyError(errors: Seq[ReferenceToFutureContractError])
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[ContractConsistencyError] = prettyOfClass(
      unnamedParam(_.errors)
    )
  }

  /** Indicates that the encrypted view message could not be created. */
  final case class EncryptedViewMessageCreationError(
      error: EncryptedViewMessageFactory.EncryptedViewMessageCreationError
  ) extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[EncryptedViewMessageCreationError] = prettyOfParam(
      _.error
    )
  }

  /** Indicates that the transaction could not be converted to a transaction tree.
    * @see
    *   TransactionTreeFactory.TransactionTreeConversionError for more information.
    */
  final case class TransactionTreeFactoryError(cause: TransactionTreeConversionError)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[TransactionTreeFactoryError] = prettyOfParam(_.cause)
  }

  final case class RecipientsCreationError(message: String)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[RecipientsCreationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class LightTransactionViewTreeCreationError(message: String)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[LightTransactionViewTreeCreationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class TransactionSigningError(cause: SyncCryptoError)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[TransactionSigningError] = prettyOfClass(
      unnamedParam(_.cause)
    )
  }

  final case class NoViewsToEncryptError(message: String)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[NoViewsToEncryptError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }
}
