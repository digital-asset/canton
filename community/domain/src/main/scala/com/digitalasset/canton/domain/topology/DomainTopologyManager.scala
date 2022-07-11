// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import cats.syntax.traverse._
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.topology.DomainTopologyManagerError.TopologyManagerParentError
import com.digitalasset.canton.error._
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.{
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.{AuthorizedStore, DomainStore}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.LegalIdentityClaimEvidence.X509Cert
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.annotation.nowarn
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}

/** Simple callback trait to inform system about changes on the topology manager
  */
trait DomainIdentityStateObserver {
  // receive update on domain topology transaction change AFTER the local change was added to the state
  @nowarn("cat=unused")
  def addedSignedTopologyTransaction(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Unit = ()

  // receive an update on the participant identity change BEFORE the change is added to the state
  def willChangeTheParticipantState(
      participantId: ParticipantId,
      attributes: ParticipantAttributes,
  ): Unit = ()

}

object DomainTopologyManager {
  type AddMemberHook =
    (AuthenticatedMember, Option[X509Cert]) => EitherT[Future, DomainTopologyManagerError, Unit]

  val addMemberNoOp =
    (_: AuthenticatedMember, _: Option[X509Cert]) =>
      EitherT[Future, DomainTopologyManagerError, Unit](Future.successful(Right(())))

  val legalIdentityHookNoOp: X509Certificate => EitherT[Future, String, Unit] =
    (_: X509Certificate) => EitherT[Future, String, Unit](Future.successful(Right(())))

  def transactionsAreSufficientToInitializeADomain(
      id: DomainId,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
      mustHaveActiveMediator: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, String, Unit] = {
    val store = new InMemoryTopologyStore(AuthorizedStore, loggerFactory)
    val ts = CantonTimestamp.Epoch
    EitherT
      .right(
        store
          .append(
            SequencedTime(ts),
            EffectiveTime(ts),
            transactions.map(x => ValidatedTopologyTransaction(x, None)),
          )
      )
      .flatMap { _ =>
        isInitializedAt(id, store, ts.immediateSuccessor, mustHaveActiveMediator, loggerFactory)
      }
  }

  def isInitializedAt[T <: TopologyStoreId](
      id: DomainId,
      store: TopologyStore[T],
      timestamp: CantonTimestamp,
      mustHaveActiveMediator: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, String, Unit] = {
    val useStateStore = store.storeId match {
      case AuthorizedStore => false
      case DomainStore(_, _) => true
    }
    val dbSnapshot = new StoreBasedTopologySnapshot(
      timestamp,
      store,
      initKeys =
        Map(), // we need to do this because of this map here, as the target client will mix-in the map into the response
      useStateTxs = useStateStore,
      packageDependencies = StoreBasedDomainTopologyClient.NoPackageDependencies,
      loggerFactory,
    )
    def hasSigningKey(owner: KeyOwner): EitherT[Future, String, SigningPublicKey] = EitherT(
      dbSnapshot
        .signingKey(owner)
        .map(_.toRight(s"$owner signing key is missing"))
    )
    // check that we have at least one mediator and one sequencer
    for {
      // first, check that we have domain parameters
      _ <- EitherT(
        dbSnapshot
          .findDynamicDomainParameters()
          .map(_.toRight("Dynamic domain parameters are not set yet"))
      )
      // then, check that we have at least one mediator
      mediators <- EitherT.right(dbSnapshot.mediators())
      _ <- EitherT.cond[Future](
        !mustHaveActiveMediator || mediators.nonEmpty,
        (),
        "No mediator domain state authorized yet.",
      )
      // check that topology manager has a signing key
      _ <- hasSigningKey(DomainTopologyManagerId(id.uid))
      // check that all mediators have a signing key
      _ <- mediators.toList.traverse(hasSigningKey)
      // check that sequencer has a signing key
      _ <- hasSigningKey(SequencerId(id.uid))
    } yield ()
  }

}

/** Domain manager implementation
  *
  * The domain manager is the topology manager of a domain. The read side of the domain manager is the identity
  * providing service.
  *
  * The domain manager is a special local manager but otherwise follows the same logic as a local manager.
  *
  * The domain manager exposes three main functions for manipulation:
  * - authorize - take an Identity Transaction, sign it with the given private key and add it to the state
  * - add       - take a signed Identity Transaction and add it to the given state
  * - set       - update the participant state
  *
  * In order to bootstrap a domain, we need to add appropriate signing keys for the domain identities (topology manager,
  * sequencer, mediator).
  *
  * In order to add a participant, we need to add appropriate signing and encryption keys. Once they are there, we can
  * set the participant state to enabled.
  */
class DomainTopologyManager(
    val id: DomainTopologyManagerId,
    clock: Clock,
    override val store: TopologyStore[TopologyStoreId.AuthorizedStore],
    addParticipantHook: DomainTopologyManager.AddMemberHook,
    override val crypto: Crypto,
    override protected val timeouts: ProcessingTimeout,
    val protocolVersion: ProtocolVersion,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyManager[DomainTopologyManagerError](
      clock,
      crypto,
      store,
      timeouts,
      protocolVersion,
      loggerFactory,
    )(ec)
    with RequestProcessingStrategy.ManagerHooks {

  private val observers = mutable.ListBuffer[DomainIdentityStateObserver]()
  def addObserver(observer: DomainIdentityStateObserver): Unit = blocking(synchronized {
    val _ = observers += observer
  })

  private def sendToObservers(action: DomainIdentityStateObserver => Unit): Unit =
    blocking(synchronized(observers.foreach(action)))

  /** Authorizes a new topology transaction by signing it and adding it to the topology state
    *
    * @param transaction the transaction to be signed and added
    * @param signingKey  the key which should be used to sign
    * @param force       force dangerous operations, such as removing the last signing key of a participant
    * @param replaceExisting if true and the transaction op is add, then we'll replace existing active mappings before adding the new
    * @return            the domain state (initialized or not initialized) or an error code of why the addition failed
    */
  def authorize[Op <: TopologyChangeOp](
      transaction: TopologyTransaction[Op],
      signingKey: Option[Fingerprint],
      force: Boolean,
      replaceExisting: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainTopologyManagerError, SignedTopologyTransaction[Op]] =
    authorize(transaction, signingKey, protocolVersion, force, replaceExisting)

  override protected def notifyObservers(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful(sendToObservers(_.addedSignedTopologyTransaction(timestamp, transactions)))

  /** Return a set of initial keys we can use before the sequenced store has seen any topology transaction */
  def getKeysForBootstrapping()(implicit
      traceContext: TraceContext
  ): Future[Map[KeyOwner, Seq[PublicKey]]] =
    store.findInitialState(id)

  override def addParticipant(
      participantId: ParticipantId,
      x509: Option[X509Cert],
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] = {
    addParticipantHook(participantId, x509)
  }

  private def checkTransactionIsNotForAlienDomainEntities(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] =
    transaction.transaction.element.mapping match {
      case OwnerToKeyMapping(ParticipantId(_), _) | OwnerToKeyMapping(MediatorId(_), _) =>
        EitherT.rightT(())
      case OwnerToKeyMapping(owner, _) if (owner.uid != this.id.uid) =>
        EitherT.leftT(DomainTopologyManagerError.AlienDomainEntities.Failure(owner.uid))
      case _ => EitherT.rightT(())
    }

  protected def checkNewTransaction(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainTopologyManagerError, Unit] =
    for {
      _ <- checkCorrectProtocolVersion(transaction)
      _ <- checkTransactionIsNotForAlienDomainEntities(transaction)
      _ <- checkNotAddingToWrongDomain(transaction)
      _ <- checkNotEnablingParticipantWithoutKeys(transaction)
    } yield ()

  private def checkNotAddingToWrongDomain(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] = {
    val domainId = id.domainId
    transaction.restrictedToDomain match {
      case None | Some(`domainId`) => EitherT.pure(())
      case Some(otherDomain) =>
        EitherT.leftT(DomainTopologyManagerError.WrongDomain.Failure(otherDomain))
    }
  }

  // Representative for the class of protocol versions of SignedTopologyTransaction
  private val signedTopologyTransactionRepresentativeProtocolVersion =
    SignedTopologyTransaction.protocolVersionRepresentativeFor(protocolVersion)

  private def checkCorrectProtocolVersion(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] = {

    EitherT.cond[Future](
      transaction.representativeProtocolVersion == signedTopologyTransactionRepresentativeProtocolVersion,
      (),
      DomainTopologyManagerError.WrongProtocolVersion.Failure(
        domainProtocolVersion = protocolVersion,
        transactionProtocolVersion = transaction.representativeProtocolVersion.representative,
      ),
    )

    val domainId = id.domainId
    transaction.restrictedToDomain match {
      case None | Some(`domainId`) => EitherT.pure(())
      case Some(otherDomain) =>
        EitherT.leftT(DomainTopologyManagerError.WrongDomain.Failure(otherDomain))
    }
  }

  private def checkNotEnablingParticipantWithoutKeys(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] =
    if (transaction.operation == TopologyChangeOp.Remove) EitherT.pure(())
    else
      transaction.transaction.element.mapping match {
        case ParticipantState(side, _, participant, permission, _) =>
          checkParticipantHasKeys(side, participant, permission)
        case _ => EitherT.pure(())
      }

  private def checkParticipantHasKeys(
      side: RequestSide,
      participantId: ParticipantId,
      permission: ParticipantPermission,
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] = {
    if (permission == ParticipantPermission.Disabled) EitherT.rightT(())
    else {
      for {
        txs <- EitherT.right(
          store.findPositiveTransactions(
            CantonTimestamp.MaxValue,
            asOfInclusive = false,
            includeSecondary = false,
            types = Seq(
              DomainTopologyTransactionType.OwnerToKeyMapping,
              DomainTopologyTransactionType.ParticipantState,
            ),
            filterUid = Some(
              Seq(participantId.uid)
            ), // unique path is using participant id for both types of topo transactions
            filterNamespace = None,
          )
        )
        keysAndSides = txs.adds.toTopologyState
          .map(_.mapping)
          .foldLeft((KeyCollection(Seq(), Seq()), false)) {
            case ((keys, sides), OwnerToKeyMapping(`participantId`, key)) =>
              (keys.addTo(key), sides)
            case (
                  (keys, _),
                  ParticipantState(otherSide, domain, `participantId`, permission, _),
                ) if permission.isActive && id.domainId == domain && side != otherSide =>
              (keys, true)
            case (acc, _) => acc
          }
        (keys, hasOtherSide) = keysAndSides
        _ <- EitherT.cond[Future](
          keys.hasBothKeys() || (!hasOtherSide && side != RequestSide.Both),
          (),
          DomainTopologyManagerError.ParticipantNotInitialized
            .Failure(participantId, keys): DomainTopologyManagerError,
        )
      } yield ()
    }
  }

  override protected def preNotifyObservers(
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
  )(implicit traceContext: TraceContext): Unit = {
    transactions.iterator
      .filter(_.transaction.op == TopologyChangeOp.Add)
      .map(_.transaction.element.mapping)
      .foreach {
        case ParticipantState(side, _, participant, permission, trustLevel)
            if side != RequestSide.To =>
          val attributes = ParticipantAttributes(permission, trustLevel)
          sendToObservers(_.willChangeTheParticipantState(participant, attributes))
          logger.info(s"Setting participant $participant state to $attributes")
        case _ => ()
      }
  }

  override protected def wrapError(error: TopologyManagerError)(implicit
      traceContext: TraceContext
  ): DomainTopologyManagerError =
    TopologyManagerParentError(error)

  /** Return true if domain identity is sufficiently initialized such that it can be used */
  def isInitialized(
      mustHaveActiveMediator: Boolean,
      logReason: Boolean = true,
  )(implicit traceContext: TraceContext): Future[Boolean] =
    isInitializedET(mustHaveActiveMediator).value
      .map {
        case Left(reason) =>
          if (logReason)
            logger.debug(s"Domain is not yet initialised: ${reason}")
          false
        case Right(_) => true
      }

  def isInitializedET(
      mustHaveActiveMediator: Boolean
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    DomainTopologyManager
      .isInitializedAt(
        id.domainId,
        store,
        CantonTimestamp.MaxValue,
        mustHaveActiveMediator,
        loggerFactory,
      )

  def executeSequential(fut: => Future[Unit], description: String)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    sequentialQueue.execute(fut, description)
  }

  override def issueParticipantStateForDomain(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainTopologyManagerError, Unit] = {
    authorize(
      TopologyStateUpdate.createAdd(
        ParticipantState(
          RequestSide.From,
          id.domainId,
          participantId,
          ParticipantPermission.Submission,
          TrustLevel.Ordinary,
        ),
        protocolVersion,
      ),
      signingKey = None,
      force = false,
      replaceExisting = true,
    ).map(_ => ())
  }

  override def addFromRequest(transaction: SignedTopologyTransaction[TopologyChangeOp])(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainTopologyManagerError, Unit] =
    add(transaction, force = true, replaceExisting = true, allowDuplicateMappings = true)
}

sealed trait DomainTopologyManagerError extends CantonError
object DomainTopologyManagerError extends TopologyManagerError.DomainErrorGroup() {

  case class TopologyManagerParentError(parent: TopologyManagerError)(implicit
      val loggingContext: ErrorLoggingContext
  ) extends DomainTopologyManagerError
      with ParentCantonError[TopologyManagerError]

  @Explanation(
    """This error indicates an external issue with the member addition hook."""
  )
  @Resolution("""Consult the error details.""")
  object FailedToAddMember
      extends ErrorCode(id = "FAILED_TO_ADD_MEMBER", ErrorCategory.MaliciousOrFaultyBehaviour) {
    case class Failure(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "The add member hook failed"
        )
        with DomainTopologyManagerError
  }

  @Explanation(
    """This error is returned if a domain topology manager attempts to activate a 
      participant without having all necessary data, such as keys or domain trust certificates."""
  )
  @Resolution("""Register the necessary keys or trust certificates and try again.""")
  object ParticipantNotInitialized
      extends ErrorCode(
        id = "PARTICIPANT_NOT_INITIALIZED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Failure(participantId: ParticipantId, currentKeys: KeyCollection)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The participant can not be enabled without registering the necessary keys first"
        )
        with DomainTopologyManagerError
    case class Reject(participantId: ParticipantId, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(reason)
        with DomainTopologyManagerError
  }

  object InvalidOrFaultyOnboardingRequest
      extends ErrorCode(
        id = "MALICOUS_OR_FAULTY_ONBOARDING_REQUEST",
        ErrorCategory.MaliciousOrFaultyBehaviour,
      ) {
    case class Failure(participantId: ParticipantId, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "The participant submitted invalid or insufficient topology transactions during onboarding"
        )
        with DomainTopologyManagerError
  }

  @Explanation(
    """This error is returned if a transaction attempts to add keys for alien domain manager or sequencer entities to this domain topology manager."""
  )
  @Resolution(
    """Use a participant topology manager if you want to manage foreign domain keys"""
  )
  object AlienDomainEntities
      extends ErrorCode(
        id = "ALIEN_DOMAIN_ENTITIES",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Failure(alienUid: UniqueIdentifier)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            "Keys of alien domain entities can not be managed through a domain topology manager"
        )
        with DomainTopologyManagerError
  }

  @Explanation(
    """This error is returned if a transaction has a protocol version different than the one spoken on the domain."""
  )
  @Resolution(
    """Recreate the transaction with a correct protocol version."""
  )
  object WrongProtocolVersion
      extends ErrorCode(
        id = "WRONG_PROTOCOL_VERSION",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Failure(
        domainProtocolVersion: ProtocolVersion,
        transactionProtocolVersion: ProtocolVersion,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            "Mismatch between protocol version of the transaction and the one spoken on the domain."
        )
        with DomainTopologyManagerError
  }

  @Explanation(
    """This error is returned if a transaction restricted to a domain should be added to another domain."""
  )
  @Resolution(
    """Recreate the content of the transaction with a correct domain identifier."""
  )
  object WrongDomain
      extends ErrorCode(id = "WRONG_DOMAIN", ErrorCategory.InvalidGivenCurrentSystemStateOther) {
    case class Failure(wrongDomain: DomainId)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Domain restricted transaction can not be added to different domain"
        )
        with DomainTopologyManagerError
  }

}
