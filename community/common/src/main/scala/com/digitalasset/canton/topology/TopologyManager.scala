// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.traverse._
import cats.syntax.traverseFilter._
import com.daml.error._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPublicStoreError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.TopologyManagementErrorGroup.TopologyManagerErrorGroup
import com.digitalasset.canton.error._
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, FlagCloseableAsync}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  IncomingTopologyTransactionAuthorizationValidator,
  SequencedTime,
}
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  TopologyTransactionRejection,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util.{MonadUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.ProtocolVersion
import org.slf4j.event.Level

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

abstract class TopologyManager[E <: CantonError](
    val clock: Clock,
    val crypto: Crypto,
    protected val store: TopologyStore[TopologyStoreId],
    timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync {

  protected val validator =
    new IncomingTopologyTransactionAuthorizationValidator(
      crypto.pureCrypto,
      store,
      None,
      loggerFactory.append("role", "manager"),
    )

  /** returns the current queue size (how many changes are being processed) */
  def queueSize: Int = sequentialQueue.queueSize

  protected def checkTransactionNotAddedBefore(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Unit] = {
    val ret = store
      .exists(transaction)
      .map(x =>
        Either.cond(
          !x,
          (),
          TopologyManagerError.DuplicateTransaction
            .Failure(transaction.transaction, transaction.key.fingerprint),
        )
      )
    EitherT(ret)
  }

  protected def checkRemovalRefersToExisingTx(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Unit] =
    if (
      transaction.operation == TopologyChangeOp.Add || transaction.operation == TopologyChangeOp.Replace
    )
      EitherT.rightT(())
    else {
      for {
        active <- EitherT.right(
          store.findPositiveTransactionsForMapping(transaction.transaction.element.mapping)
        )
        filtered = active.find(sit => sit.transaction.element == transaction.transaction.element)
        _ <- EitherT.cond[Future](
          filtered.nonEmpty,
          (),
          TopologyManagerError.NoCorrespondingActiveTxToRevoke.Element(
            transaction.transaction.element
          ): TopologyManagerError,
        )
      } yield ()
    }

  protected def keyRevocationIsNotDangerous(
      owner: KeyOwner,
      key: PublicKey,
      elementId: TopologyElementId,
      force: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Unit] = {
    lazy val removingLastKeyMustBeForcedError: TopologyManagerError =
      TopologyManagerError.RemovingLastKeyMustBeForced.Failure(key.fingerprint, key.purpose)

    for {
      txs <- EitherT.right(
        store.findPositiveTransactions(
          // Use the max timestamp so that we get the head state
          CantonTimestamp.MaxValue,
          asOfInclusive = true,
          includeSecondary = false,
          types = Seq(DomainTopologyTransactionType.OwnerToKeyMapping),
          filterUid = Some(Seq(owner.uid)),
          filterNamespace = None,
        )
      )
      remaining = txs.toIdentityState.collect {
        case TopologyStateUpdateElement(id, OwnerToKeyMapping(`owner`, remainingKey))
            if id != elementId && key.purpose == remainingKey.purpose =>
          key
      }
      _ <-
        if (force && remaining.isEmpty) {
          logger.info(s"Transaction will forcefully remove last ${key.purpose} of $owner")
          EitherT.rightT[Future, TopologyManagerError](())
        } else EitherT.cond[Future](remaining.nonEmpty, (), removingLastKeyMustBeForcedError)
    } yield ()
  }

  protected def transactionIsNotDangerous(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyManagerError, Unit] =
    if (transaction.transaction.op == TopologyChangeOp.Add)
      EitherT.rightT(())
    else {
      transaction.transaction.element.mapping match {
        case OwnerToKeyMapping(owner, key) =>
          keyRevocationIsNotDangerous(owner, key, transaction.transaction.element.id, force)
        // TODO(i1031) check certificate revocations for creating dangling states
        case _ => EitherT.rightT(())
      }
    }

  def signedMappingAlreadyExists(
      mapping: TopologyMapping,
      signingKey: Fingerprint,
  )(implicit traceContext: TraceContext): Future[Boolean] =
    for {
      txs <- store.findPositiveTransactionsForMapping(mapping)
      mappings = txs.map(x => (x.transaction.element.mapping, x.key.fingerprint))
    } yield mappings.contains((mapping, signingKey))

  protected def checkMappingOfTxDoesNotExistYet(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      allowDuplicateMappings: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Unit] =
    if (allowDuplicateMappings || transaction.transaction.op != TopologyChangeOp.Add) {
      EitherT.rightT(())
    } else {
      (for {
        exists <- EitherT.right(
          signedMappingAlreadyExists(
            transaction.transaction.element.mapping,
            transaction.key.fingerprint,
          )
        )
        _ <- EitherT.cond[Future](
          !exists,
          (),
          TopologyManagerError.MappingAlreadyExists.Failure(
            transaction.transaction.element,
            transaction.key.fingerprint,
          ): TopologyManagerError,
        )
      } yield ()): EitherT[Future, TopologyManagerError, Unit]
    }

  protected def checkNewTransaction(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, E, Unit]

  protected def build[Op <: TopologyChangeOp](
      transaction: TopologyTransaction[Op],
      signingKey: Option[Fingerprint],
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, E, SignedTopologyTransaction[Op]] = {
    for {
      // find signing key
      key <- signingKey match {
        case Some(key) => EitherT.rightT[Future, E](key)
        case None => signingKeyForTransactionF(transaction)
      }
      // fetch public key
      pubkey <- crypto.cryptoPublicStore
        .signingKey(key)
        .leftMap(x => wrapError(TopologyManagerError.InternalError.CryptoPublicError(x)))
        .subflatMap(_.toRight(wrapError(TopologyManagerError.PublicKeyNotInStore.Failure(key))))
      // create signed transaction
      signed <- SignedTopologyTransaction
        .create(
          transaction,
          pubkey,
          crypto.pureCrypto,
          crypto.privateCrypto,
          protocolVersion,
        )
        .leftMap {
          case SigningError.UnknownSigningKey(keyId) =>
            wrapError(TopologyManagerError.SecretKeyNotInStore.Failure(keyId))
          case err => wrapError(TopologyManagerError.InternalError.TopologySigningError(err))
        }
    } yield signed
  }

  /** Authorizes a new topology transaction by signing it and adding it to the topology state
    *
    * @param transaction the transaction to be signed and added
    * @param signingKey  the key which should be used to sign
    * @param protocolVersion the protocol version corresponding to the transaction
    * @param force       force dangerous operations, such as removing the last signing key of a participant
    * @param replaceExisting if true and the transaction op is add, then we'll replace existing active mappings before adding the new
    * @return            the domain state (initialized or not initialized) or an error code of why the addition failed
    */
  def authorize[Op <: TopologyChangeOp](
      transaction: TopologyTransaction[Op],
      signingKey: Option[Fingerprint],
      protocolVersion: ProtocolVersion,
      force: Boolean = false,
      replaceExisting: Boolean = false,
  )(implicit traceContext: TraceContext): EitherT[Future, E, SignedTopologyTransaction[Op]] = {
    sequentialQueue.executeE(
      {
        logger.debug(show"Attempting to authorize ${transaction.element.mapping} with $signingKey")
        for {
          signed <- build(transaction, signingKey, protocolVersion)
          _ <- process(signed, force, replaceExisting, allowDuplicateMappings = false)
        } yield signed
      },
      "authorize transaction",
    )

  }

  protected def signingKeyForTransactionF(
      transaction: TopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, E, Fingerprint] = {
    for {
      // need to execute signing key finding sequentially, as the caches in the validator are not thread safe
      keys <- EitherT.right(
        validator.getValidSigningKeysForMapping(clock.uniqueTime(), transaction.element.mapping)
      )
      fingerprint <- findSigningKey(keys).leftMap(wrapError)
    } yield fingerprint
  }

  private def findSigningKey(
      keys: Seq[Fingerprint]
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Fingerprint] =
    keys.reverse.toList
      .filterA(fingerprint =>
        crypto.cryptoPrivateStore
          .existsSigningKey(fingerprint)
      )
      .map(x => x.headOption)
      .leftMap[TopologyManagerError](x => TopologyManagerError.InternalError.CryptoPrivateError(x))
      .subflatMap(_.toRight(TopologyManagerError.NoAppropriateSigningKeyInStore.Failure(keys)))

  def add(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean = false,
      replaceExisting: Boolean = false,
      allowDuplicateMappings: Boolean = false,
  )(implicit traceContext: TraceContext): EitherT[Future, E, Unit] = {
    // Ensure sequential execution of `process`: When processing signed topology transactions, we test whether they can be
    // added incrementally to the existing state. Therefore, we need to sequence
    // (testing + adding) and ensure that we don't concurrently insert these
    // transactions.
    sequentialQueue.executeE(
      process(transaction, force, replaceExisting, allowDuplicateMappings),
      "add transaction",
    )
  }

  protected val sequentialQueue = new SimpleExecutionQueue()

  /** sequential(!) processing of topology transactions
    *
    * @param force force a dangerous change (such as revoking the last key)
    * @param allowDuplicateMappings whether to reject a transaction if a similar transaction leading to the same result already exists
    */
  protected def process[Op <: TopologyChangeOp](
      transaction: SignedTopologyTransaction[Op],
      force: Boolean,
      replaceExisting: Boolean,
      allowDuplicateMappings: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, E, Unit] = {
    def checkValidationResult(
        validated: Seq[ValidatedTopologyTransaction]
    ): EitherT[Future, E, Unit] = {
      EitherT.fromEither((validated.find(_.rejectionReason.nonEmpty) match {
        case Some(
              ValidatedTopologyTransaction(
                `transaction`,
                Some(TopologyTransactionRejection.NotAuthorized),
              )
            ) =>
          Left(TopologyManagerError.UnauthorizedTransaction.Failure(): TopologyManagerError)
        case Some(
              ValidatedTopologyTransaction(
                `transaction`,
                Some(TopologyTransactionRejection.SignatureCheckFailed(err)),
              )
            ) =>
          Left(TopologyManagerError.InvalidSignatureError.Failure(err): TopologyManagerError)
        case Some(tx: ValidatedTopologyTransaction) =>
          Left(TopologyManagerError.InternalError.ReplaceExistingFailed(tx))
        case None => Right(())
      }).leftMap(wrapError))
    }

    def addOneByOne(
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
    ): EitherT[Future, E, Unit] = {
      MonadUtil.sequentialTraverse_(transactions) { tx =>
        val now = clock.uniqueTime()
        preNotifyObservers(Seq(tx))
        logger.info(
          show"Applied topology transaction ${tx.transaction.op} ${tx.transaction.element.mapping} at $now"
        )
        for {
          _ <- EitherT.right(
            store.append(
              SequencedTime(now),
              EffectiveTime(now),
              Seq(ValidatedTopologyTransaction.valid(tx)),
            )
          ): EitherT[Future, E, Unit]
          _ <- EitherT.right(notifyObservers(now, Seq(tx)))
        } yield ()
      }
    }

    val isUniquenessRequired = transaction.operation match {
      case TopologyChangeOp.Replace => false
      case _ => true
    }

    val now = clock.uniqueTime()
    val ret = for {
      // uniqueness check on store: ensure that transaction hasn't been added before
      _ <-
        if (isUniquenessRequired) checkTransactionNotAddedBefore(transaction).leftMap(wrapError)
        else EitherT.pure[Future, E](())
      _ <- checkRemovalRefersToExisingTx(transaction).leftMap(wrapError)
      _ <- checkMappingOfTxDoesNotExistYet(transaction, allowDuplicateMappings).leftMap(wrapError)
      _ <- transactionIsNotDangerous(transaction, force).leftMap(wrapError)
      _ <- checkNewTransaction(transaction, force) // domain / participant specific checks
      deactivateExisting <- removeExistingTransactions(transaction, replaceExisting)
      updateTx = transaction +: deactivateExisting
      res <- EitherT.right(validator.validateAndUpdateHeadAuthState(now, updateTx))
      _ <- checkValidationResult(res._2)
      // TODO(i1251) batch adding once we overhaul the domain identity dispatcher (right now, adding multiple tx with same ts doesn't work)
      _ <- addOneByOne(updateTx)
    } yield ()

    ret.leftMap { err =>
      // if there was an intermittent failure, just reset the auth validator (will reload the state)
      validator.reset()
      err
    }
  }

  protected def removeExistingTransactions(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      replaceExisting: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, E, Seq[SignedTopologyTransaction[TopologyChangeOp]]] =
    if (!replaceExisting || transaction.operation == TopologyChangeOp.Remove) {
      EitherT.rightT(Seq())
    } else {
      val (nsFilter, uidFilter) = transaction.uniquePath.maybeUid match {
        case Some(uid) => (None, Some(Seq(uid)))
        case None => (Some(Seq(transaction.uniquePath.namespace)), None)
      }

      for {
        rawTxs <- EitherT.right(
          store.findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = false,
            includeSecondary = false,
            types = Seq(transaction.uniquePath.dbType),
            filterUid = uidFilter,
            filterNamespace = nsFilter,
          )
        )
        reverse <- rawTxs.adds.toDomainTopologyTransactions.view
          .filter(
            _.transaction.element.mapping.isReplacedBy(
              transaction.transaction.element.mapping
            )
          )
          .toList
          .traverse(x => build(x.transaction.reverse, None, x.representativeProtocolVersion))
      } yield reverse
    }

  protected def notifyObservers(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Future[Unit]

  @nowarn("cat=unused")
  protected def preNotifyObservers(transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]])(
      implicit traceContext: TraceContext
  ): Unit = {}

  protected def wrapError(error: TopologyManagerError)(implicit traceContext: TraceContext): E

  /** Generates a signed legal identity claim of the given claim */
  def generate(
      claim: LegalIdentityClaim
  )(implicit traceContext: TraceContext): EitherT[Future, E, SignedLegalIdentityClaim] = {
    claim.evidence match {
      case LegalIdentityClaimEvidence.X509Cert(pem) =>
        for {
          pubKey <- (for {
            cert <- X509Certificate.fromPem(pem)
            key <- cert.publicKey(crypto.javaKeyConverter)
          } yield key)
            .leftMap(x => wrapError(TopologyManagerError.CertificateGenerationError.Failure(x)))
            .toEitherT[Future]
          claimHash = claim.hash(crypto.pureCrypto)
          // Sign the legal identity claim with the legal entity key as specified in the evidence
          signed <- crypto.privateCrypto
            .sign(claimHash, pubKey.fingerprint)
            .leftMap(err => wrapError(TopologyManagerError.InternalError.TopologySigningError(err)))
            .map(signature =>
              SignedLegalIdentityClaim(claim.uid, claim.getCryptographicEvidence, signature)
            )
        } yield signed
    }
  }

  def genTransaction(op: TopologyChangeOp, mapping: TopologyMapping)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyManagerError, TopologyTransaction[TopologyChangeOp]] = {
    import TopologyChangeOp._
    (op, mapping) match {
      case (Add, mapping: TopologyStateUpdateMapping) =>
        EitherT.rightT(TopologyStateUpdate.createAdd(mapping))
      case (Remove, mapping: TopologyStateUpdateMapping) =>
        for {
          tx <- EitherT(
            store
              .findPositiveTransactionsForMapping(mapping)
              .map(
                _.headOption.toRight[TopologyManagerError](
                  TopologyManagerError.NoCorrespondingActiveTxToRevoke.Mapping(mapping)
                )
              )
          )
        } yield tx.transaction.reverse

      case (Replace, mapping: DomainGovernanceMapping) =>
        EitherT.pure(DomainGovernanceTransaction(mapping))

      case (op, mapping) =>
        EitherT.fromEither(
          Left(TopologyManagerError.InternalError.IncompatibleOpMapping(op, mapping))
        )
    }
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty._
    Seq(
      sequentialQueue.asCloseable(
        "topology-manager-sequential-queue",
        timeouts.shutdownProcessing.unwrap,
      )
    )
  }

}

sealed trait TopologyManagerError extends CantonError

object TopologyManagerError extends TopologyManagerErrorGroup {

  @Explanation(
    """This error indicates that there was an internal error within the topology manager."""
  )
  @Resolution("Inspect error message for details.")
  object InternalError
      extends ErrorCode(
        id = "TOPOLOGY_MANAGER_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    case class CryptoPublicError(error: CryptoPublicStoreError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Operation on the public crypto store failed"
        )
        with TopologyManagerError

    case class CryptoPrivateError(error: CryptoPrivateStoreError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Operation on the secret crypto store failed"
        )
        with TopologyManagerError

    case class IncompatibleOpMapping(op: TopologyChangeOp, mapping: TopologyMapping)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The operation is incompatible with the mapping"
        )
        with TopologyManagerError

    case class TopologySigningError(error: SigningError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Creating a signed transaction failed due to a crypto error"
        )
        with TopologyManagerError

    case class ReplaceExistingFailed(invalid: ValidatedTopologyTransaction)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Replacing existing transaction failed upon removal"
        )
        with TopologyManagerError

  }

  @Explanation(
    """This error indicates that the secret key with the respective fingerprint can not be found."""
  )
  @Resolution(
    "Ensure you only use fingerprints of secret keys stored in your secret key store."
  )
  object SecretKeyNotInStore
      extends ErrorCode(
        id = "SECRET_KEY_NOT_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Failure(keyId: Fingerprint)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Secret key with given fingerprint could not be found"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a command contained a fingerprint referring to a public key not being present in the public key store."""
  )
  @Resolution(
    "Upload the public key to the public key store using $node.keys.public.load(.) before retrying."
  )
  object PublicKeyNotInStore
      extends ErrorCode(
        id = "PUBLIC_KEY_NOT_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Failure(keyId: Fingerprint)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Public key with given fingerprint is missing in the public key store"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the uploaded signed transaction contained an invalid signature."""
  )
  @Resolution(
    "Ensure that the transaction is valid and uses a crypto version understood by this participant."
  )
  object InvalidSignatureError
      extends ErrorCode(
        id = "INVALID_TOPOLOGY_TX_SIGNATURE_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {

    override def logLevel: Level = Level.WARN

    case class Failure(error: SignatureCheckError)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Transaction signature verification failed"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a transaction has already been added previously."""
  )
  @Resolution(
    """Nothing to do as the transaction is already registered. Note however that a revocation is " +
    final. If you want to re-enable a statement, you need to re-issue an new transaction."""
  )
  object DuplicateTransaction
      extends ErrorCode(
        id = "DUPLICATE_TOPOLOGY_TRANSACTION",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    case class Failure(transaction: TopologyTransaction[TopologyChangeOp], authKey: Fingerprint)(
        implicit val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The given topology transaction already exists."
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that a topology transaction would create a state that already exists and has been authorized with the same key."""
  )
  @Resolution("""Your intended change is already in effect.""")
  object MappingAlreadyExists
      extends ErrorCode(
        id = "TOPOLOGY_MAPPING_ALREADY_EXISTS",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    case class Failure(existing: TopologyStateElement[TopologyMapping], authKey: Fingerprint)(
        implicit val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "A matching topology mapping authorized with the same key already exists in this state"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error results if the topology manager did not find a secret key in its store to authorize a certain topology transaction."""
  )
  @Resolution("""Inspect your topology transaction and your secret key store and check that you have the 
      appropriate certificates and keys to issue the desired topology transaction. If the list of candidates is empty, 
      then you are missing the certificates.""")
  object NoAppropriateSigningKeyInStore
      extends ErrorCode(
        id = "NO_APPROPRIATE_SIGNING_KEY_IN_STORE",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    case class Failure(candidates: Seq[Fingerprint])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Could not find an appropriate signing key to issue the topology transaction"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the desired certificate could not be created."""
  )
  @Resolution("""Inspect the underlying error for details.""")
  object CertificateGenerationError
      extends ErrorCode(
        id = "CERTIFICATE_GENERATION_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    case class Failure(error: X509CertificateError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Failed to generate the certificate"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempt to add a transaction was rejected, as the signing key is not authorized within the current state."""
  )
  @Resolution(
    """Inspect the topology state and ensure that valid namespace or identifier delegations of the signing key exist or upload them before adding this transaction."""
  )
  object UnauthorizedTransaction
      extends ErrorCode(
        id = "UNAUTHORIZED_TOPOLOGY_TRANSACTION",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Failure()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Topology transaction is not properly authorized"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempt to add a removal transaction was rejected, as the mapping / element affecting the removal did not exist."""
  )
  @Resolution(
    """Inspect the topology state and ensure the mapping and the element id of the active transaction you are trying to revoke matches your revocation arguments."""
  )
  object NoCorrespondingActiveTxToRevoke
      extends ErrorCode(
        id = "NO_CORRESPONDING_ACTIVE_TX_TO_REVOKE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Mapping(mapping: TopologyMapping)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            "There is no active topology transaction matching the mapping of the revocation request"
        )
        with TopologyManagerError
    case class Element(element: TopologyStateElement[TopologyMapping])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "There is no active topology transaction matching the element of the revocation request"
        )
        with TopologyManagerError
  }

  @Explanation(
    """This error indicates that the attempted key removal would remove the last valid key of the given entity, making the node unusuable."""
  )
  @Resolution(
    """Add the `force = true` flag to your command if you are really sure what you are doing."""
  )
  object RemovingLastKeyMustBeForced
      extends ErrorCode(
        id = "REMOVING_LAST_KEY_MUST_BE_FORCED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Failure(key: Fingerprint, purpose: KeyPurpose)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Topology transaction would remove the last key of the given entity"
        )
        with TopologyManagerError
  }

  abstract class DomainErrorGroup extends ErrorGroup()
  abstract class ParticipantErrorGroup extends ErrorGroup()

}
