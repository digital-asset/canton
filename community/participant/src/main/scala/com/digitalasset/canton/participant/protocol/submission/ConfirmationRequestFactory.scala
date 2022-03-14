// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.ledger.participant.state.v2.SubmitterInfo
import com.digitalasset.canton._
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data._
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.protocol.submission.ConfirmationRequestFactory._
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  SerializableContractOfId,
  TransactionTreeConversionError,
}
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker.ReferenceToFutureContractError
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.sequencing.protocol.OpenEnvelope
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** Factory class for creating confirmation requests from Daml-LF transactions.
  *
  * @param transactionTreeFactory used to create the payload
  * @param seedGenerator used to derive the transaction seed
  */
class ConfirmationRequestFactory(
    submitterNode: ParticipantId,
    domain: DomainId,
)(val transactionTreeFactory: TransactionTreeFactory, seedGenerator: SeedGenerator)(implicit
    executionContext: ExecutionContext
) {

  /** Creates a confirmation request from a wellformed transaction.
    *
    * @param cryptoSnapshot used to determine participants of parties and for signing and encryption
    * @return the confirmation request and the transaction root hash (aka transaction id) or an error. See the
    *         documentation of [[com.digitalasset.canton.participant.protocol.submission.ConfirmationRequestFactory.ConfirmationRequestCreationError]]
    *         for more information on error cases.
    */
  def createConfirmationRequest(
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      confirmationPolicy: ConfirmationPolicy,
      submitterInfo: SubmitterInfo,
      ledgerTime: CantonTimestamp,
      workflowId: Option[WorkflowId],
      mediatorId: MediatorId,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      contractInstanceOfId: SerializableContractOfId,
      optKeySeed: Option[SecureRandomness],
      version: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ConfirmationRequestCreationError, ConfirmationRequest] = {
    val transactionUuid = seedGenerator.generateUuid()

    val randomnessLength = EncryptedViewMessage.computeRandomnessLength(cryptoSnapshot)
    val keySeed =
      optKeySeed.getOrElse(SecureRandomness.secureRandomness(randomnessLength))

    for {
      _ <- assertSubmittersNodeAuthorization(submitterInfo.actAs, cryptoSnapshot.ipsSnapshot)

      // Starting with Daml 1.6.0, the daml engine performs authorization validation.

      transactionSeed <- seedGenerator
        .generateSeedForTransaction(submitterInfo.changeId, domain, ledgerTime, transactionUuid)
        .leftMap(SeedGeneratorError)

      transactionTree <- transactionTreeFactory
        .createTransactionTree(
          wfTransaction,
          submitterInfo,
          confirmationPolicy,
          workflowId,
          mediatorId,
          transactionSeed,
          transactionUuid,
          cryptoSnapshot.ipsSnapshot,
          contractInstanceOfId,
        )
        .leftMap(TransactionTreeFactoryError)

      rootViews = transactionTree.rootViews.unblindedElements.toList
      _ <- EitherT.fromEither[Future](
        ContractConsistencyChecker
          .assertInputContractsInPast(rootViews, ledgerTime)
          .leftMap(errs => ContractConsistencyError(errs))
      )

      transactionViewEnvelopes <- createTransactionViewEnvelopes(
        transactionTree,
        cryptoSnapshot,
        keySeed,
        version,
      )
    } yield ConfirmationRequest(
      InformeeMessage(transactionTree.fullInformeeTree),
      transactionViewEnvelopes,
    )
  }

  private def assertSubmittersNodeAuthorization(
      submitters: List[LfPartyId],
      identities: TopologySnapshot,
  ): EitherT[Future, ParticipantAuthorizationError, Unit] = {

    def assertSubmitterNodeAuthorization(submitter: LfPartyId) = {
      for {
        relationship <- EitherT(
          identities
            .hostedOn(submitter, submitterNode)
            .map(
              _.toRight(
                ParticipantAuthorizationError(
                  s"$submitterNode does not host $submitter or is not active."
                )
              )
            )
        )
        _ <- EitherT.cond[Future](
          relationship.permission == Submission,
          (),
          ParticipantAuthorizationError(
            s"$submitterNode is not authorized to submit transactions for $submitter."
          ),
        )
      } yield ()
    }
    submitters.traverse(assertSubmitterNodeAuthorization).map(_ => ())
  }

  private def createTransactionViewEnvelopes(
      transactionTree: GenTransactionTree,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      keySeed: SecureRandomness,
      version: ProtocolVersion,
  )(implicit traceContext: TraceContext): EitherT[Future, ConfirmationRequestCreationError, List[
    OpenEnvelope[TransactionViewMessage]
  ]] = {
    val pureCrypto = cryptoSnapshot.pureCrypto
    for {
      lightTreesWithMetadata <- EitherT.fromEither[Future](
        transactionTree
          .allLightTransactionViewTreesWithWitnessesAndSeeds(keySeed, pureCrypto)
          .leftMap(KeySeedError)
      )
      res <- lightTreesWithMetadata.toList
        .traverse { case (vt, witnesses, seed) =>
          for {
            viewMessage <- EncryptedViewMessageFactory
              .create(TransactionViewType)(vt, cryptoSnapshot, version, Some(seed))
              .leftMap(EncryptedViewMessageCreationError)
            recipients <- witnesses
              .toRecipients(cryptoSnapshot.ipsSnapshot)
              .leftMap[ConfirmationRequestCreationError](e => RecipientsCreationError(e.message))
          } yield OpenEnvelope(viewMessage, recipients)
        }
    } yield res
  }
}

object ConfirmationRequestFactory {
  def apply(submitterNode: ParticipantId, domainId: DomainId)(
      cryptoOps: HashOps with HmacOps,
      seedGenerator: SeedGenerator,
      packageService: PackageService,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): ConfirmationRequestFactory = {

    val transactionTreeFactory =
      TransactionTreeFactoryImpl(
        submitterNode,
        domainId,
        cryptoOps,
        packageService,
        loggerFactory,
      )

    new ConfirmationRequestFactory(submitterNode, domainId)(
      transactionTreeFactory,
      seedGenerator,
    )
  }

  /** Superclass for all errors that may arise during the creation of a confirmation request.
    */
  sealed trait ConfirmationRequestCreationError
      extends Product
      with Serializable
      with PrettyPrinting

  /** Indicates that the submitterNode is not allowed to represent the submitter or to submit requests.
    */
  case class ParticipantAuthorizationError(message: String)
      extends ConfirmationRequestCreationError {
    override def pretty: Pretty[ParticipantAuthorizationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that the submitter is not authorized to commit the given transaction.
    * I.e. the commit built from the submitter and the transaction is not well-authorized.
    */
  case class DamlAuthorizationError(message: String) extends ConfirmationRequestCreationError {
    override def pretty: Pretty[DamlAuthorizationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that the given transaction is malformed in some way, e.g., it has cycles.
    */
  case class MalformedLfTransaction(message: String) extends ConfirmationRequestCreationError {
    override def pretty: Pretty[MalformedLfTransaction] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that the submitter could not be mapped to a party.
    */
  case class MalformedSubmitter(message: String) extends ConfirmationRequestCreationError {
    override def pretty: Pretty[MalformedSubmitter] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  /** Indicates that the given transaction is contract-inconsistent.
    */
  case class ContractConsistencyError(errors: Seq[ReferenceToFutureContractError])
      extends ConfirmationRequestCreationError {
    override def pretty: Pretty[ContractConsistencyError] = prettyOfClass(unnamedParam(_.errors))
  }

  /** Indicates that the given transaction is contract key-inconsistent. */
  case class ContractKeyConsistencyError(errors: Set[LfGlobalKey])
      extends ConfirmationRequestCreationError
      with PrettyPrinting {
    override def pretty: Pretty[ContractKeyConsistencyError] =
      prettyOfClass(
        param("keys", _.errors)
      )
  }

  /** Indicates that the encrypted view message could not be created. */
  case class EncryptedViewMessageCreationError(
      error: EncryptedViewMessageFactory.EncryptedViewMessageCreationError
  ) extends ConfirmationRequestCreationError {
    override def pretty: Pretty[EncryptedViewMessageCreationError] = prettyOfParam(_.error)
  }

  /** Indicates that the transaction could not be converted to a transaction tree.
    * @see TransactionTreeFactory.TransactionTreeConversionError for more information.
    */
  case class TransactionTreeFactoryError(cause: TransactionTreeConversionError)
      extends ConfirmationRequestCreationError {
    override def pretty: Pretty[TransactionTreeFactoryError] = prettyOfParam(_.cause)
  }

  case class SeedGeneratorError(cause: SaltError) extends ConfirmationRequestCreationError {
    override def pretty: Pretty[SeedGeneratorError] = prettyOfParam(_.cause)
  }

  case class RecipientsCreationError(message: String) extends ConfirmationRequestCreationError {
    override def pretty: Pretty[RecipientsCreationError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  case class KeySeedError(cause: HkdfError) extends ConfirmationRequestCreationError {
    override def pretty: Pretty[KeySeedError] = prettyOfParam(_.cause)
  }
}
