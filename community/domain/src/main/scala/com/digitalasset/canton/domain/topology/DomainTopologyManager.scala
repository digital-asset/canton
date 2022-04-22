// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.topology.DomainTopologyManagerError.TopologyManagerParentError
import com.digitalasset.canton.error._
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{DomainId, _}
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.transaction.LegalIdentityClaimEvidence.X509Cert
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Add
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext

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

  def isInitialized(
      id: DomainId,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  ): Boolean = {
    def hasKey(owner: KeyOwner): Boolean = transactions.exists {
      case SignedTopologyTransaction(
            TopologyStateUpdate(
              Add,
              TopologyStateUpdateElement(_, OwnerToKeyMapping(`owner`, key)),
            ),
            _,
            _,
          ) =>
        key.isSigning
      case _ => false
    }
    DomainMember.listAll(id).map(hasKey).forall(identity)
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
    val id: UniqueIdentifier,
    clock: Clock,
    override val store: TopologyStore,
    addParticipantHook: DomainTopologyManager.AddMemberHook,
    override val crypto: Crypto,
    override protected val timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyManager[DomainTopologyManagerError](
      clock,
      crypto,
      store,
      timeouts,
      loggerFactory,
    )(ec) {

  val managerId = DomainTopologyManagerId(id)

  private val observers = mutable.ListBuffer[DomainIdentityStateObserver]()
  def addObserver(observer: DomainIdentityStateObserver): Unit = blocking(synchronized {
    val _ = observers += observer
  })

  private def sendToObservers(action: DomainIdentityStateObserver => Unit): Unit =
    blocking(synchronized(observers.foreach(action)))

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

  def addParticipant(
      participantId: ParticipantId,
      x509: Option[X509Cert],
  ): EitherT[Future, DomainTopologyManagerError, Unit] = {
    addParticipantHook(participantId, x509)
  }

  protected def checkTransactionIsNotForAlienDomainEntities(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] =
    transaction.transaction.element.mapping match {
      case OwnerToKeyMapping(ParticipantId(_), _) | OwnerToKeyMapping(MediatorId(_), _) =>
        EitherT.rightT(())
      case OwnerToKeyMapping(owner, _) if (owner.uid != this.id) =>
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
      _ <- checkTransactionIsNotForAlienDomainEntities(transaction)
      _ <- checkNotAddingToWrongDomain(transaction)
      _ <- checkNotEnablingParticipantWithoutKeys(transaction)
    } yield ()

  protected def checkNotAddingToWrongDomain(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] = {
    val domainId = managerId.domainId
    transaction.restrictedToDomain match {
      case None | Some(`domainId`) => EitherT.pure(())
      case Some(otherDomain) =>
        EitherT.leftT(DomainTopologyManagerError.WrongDomain.Failure(otherDomain))
    }
  }

  protected def checkNotEnablingParticipantWithoutKeys(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Unit] =
    if (transaction.operation == TopologyChangeOp.Remove) EitherT.pure(())
    else
      transaction.transaction.element.mapping match {
        case ParticipantState(side, _, participant, permission, _) if side != RequestSide.To =>
          checkParticipantHasKeys(participant, permission)
        case _ => EitherT.pure(())
      }

  private def checkParticipantHasKeys(
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
            Seq(DomainTopologyTransactionType.OwnerToKeyMapping),
            filterUid = Some(Seq(participantId.uid)),
            filterNamespace = None,
          )
        )
        keys = txs.adds.toIdentityState
          .map(_.mapping)
          .collect { case OwnerToKeyMapping(`participantId`, key) =>
            key
          }
          .foldLeft(KeyCollection(Seq(), Seq()))((acc, elem) => acc.addTo(elem))
        _ <- EitherT.cond[Future](
          keys.hasBothKeys(),
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

  /** Return true if domain identity is sufficiently initialized
    *
    * The state is sufficiently initialized if it contains the domain entities signing keys.
    */
  def isInitialized(implicit traceContext: TraceContext): Future[Boolean] = {
    store
      .findPositiveTransactions(
        asOf = CantonTimestamp.MaxValue,
        asOfInclusive = true,
        includeSecondary = false,
        types = Seq(DomainTopologyTransactionType.OwnerToKeyMapping),
        filterUid = Some(Seq(id)),
        filterNamespace = None,
      )
      .map { elements =>
        DomainTopologyManager.isInitialized(
          DomainId(id),
          elements.combine.toDomainTopologyTransactions,
        )
      }
  }

  def executeSequential(fut: => Future[Unit], description: String)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    sequentialQueue.execute(fut, description)
  }

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
      participant without having previously registered the necessary keys."""
  )
  @Resolution("""Register the necessary keys and try again.""")
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
