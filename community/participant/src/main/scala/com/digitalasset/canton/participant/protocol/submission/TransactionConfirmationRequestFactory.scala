// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.instances.either.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.LoggingConfig
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner.SigningTimestampOverrides
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.GenTransactionTree.ViewWithWitnessesAndRecipients
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo.ExternallySignedSubmission
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
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
import com.digitalasset.canton.util.IdUtil.catsSemigroupForIdLeftBias
import com.digitalasset.canton.util.{ContractHasher, ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

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
  */
class TransactionConfirmationRequestFactory(
    submitterNode: ParticipantId,
    loggingConfig: LoggingConfig,
    val loggerFactory: NamedLoggerFactory,
    parallel: Boolean = true,
)(val transactionTreeFactory: TransactionTreeFactory, seedGenerator: SeedGenerator)(implicit
    executionContext: ExecutionContext
) extends NamedLogging {

  /** Creates a confirmation request from a wellformed transaction.
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
      keyResolver: LfGlobalKeyMapping,
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
          keyResolver,
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

    def createOpenEnvelopesWithTransaction(
        viewsWithWitnessesAndRecipients: NonEmpty[Seq[ViewWithWitnessesAndRecipients]],
        viewKeyDataMap: ViewKeyDataMap,
    ): EitherT[FutureUnlessShutdown, TransactionConfirmationRequestCreationError, Seq[
      OpenEnvelope[EncryptedViewMessage[TransactionViewType.type]]
    ]] = {
      def makeLightTransactionViewTreeWithRecipient(
          viewWithWitnessesAndRecipients: ViewWithWitnessesAndRecipients
      ): Either[
        TransactionConfirmationRequestCreationError,
        (Recipients, LightTransactionViewTree),
      ] = {
        val randomness = viewWithWitnessesAndRecipients.view.subviewHashes
          .map(subviewHash => viewKeyDataMap.randomnessByHash(subviewHash))

        LightTransactionViewTree
          .fromTransactionViewTree(
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

        if (parallel) {
          lightTreesByRecipients
            .parTraverse { case (recipients, lightTrees) =>
              encryptViews(lightTrees, recipients)
            }
        } else {
          MonadUtil
            .sequentialTraverse(lightTreesByRecipients) { case (recipients, lightTrees) =>
              encryptViews(lightTrees, recipients)
            }
        }
      }

      val lightTreesWithRecipientsE =
        if (parallel) {
          viewsWithWitnessesAndRecipients.toNEF
            .parTraverse(
              makeLightTransactionViewTreeWithRecipient
            )
        } else {
          viewsWithWitnessesAndRecipients.toNEF
            .traverse(makeLightTransactionViewTreeWithRecipient)
        }

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

      envelopes <- createOpenEnvelopesWithTransaction(
        viewsWithWitnessesAndRecipientsNE,
        viewsKeyDataMap,
      )
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
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): TransactionConfirmationRequestFactory = {

    val transactionTreeFactory = TransactionTreeFactory(
      submitterNode,
      synchronizerId,
      // TODO(#23971): Make this dependent on the protocol version when introducing V2 contract IDs
      AuthenticatedContractIdVersionV12,
      cryptoOps,
      hasher,
      loggerFactory,
    )

    new TransactionConfirmationRequestFactory(submitterNode, loggingConfig, loggerFactory)(
      transactionTreeFactory,
      seedGenerator,
    )
  }

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

  /** Indicates that the given transaction is malformed in some way, e.g., it has cycles.
    */
  final case class MalformedLfTransaction(message: String)
      extends TransactionConfirmationRequestCreationError {
    override protected def pretty: Pretty[MalformedLfTransaction] = prettyOfClass(
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
