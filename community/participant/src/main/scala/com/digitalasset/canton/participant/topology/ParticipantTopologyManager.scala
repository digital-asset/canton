// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.traverse._
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Crypto, Fingerprint}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error._
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown.syntax._
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.participant.topology.ParticipantTopologyManager.PostInitCallbacks
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantErrorGroup
import com.digitalasset.canton.topology.{DomainId, _}
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil._

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}

trait ParticipantTopologyManagerObserver {
  def addedNewTransactions(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
}

/** Participant side topology manager
  *
  * @param clock the participants clock
  * @param store the topology store to be used for the authorized store
  * @param crypto the set of crypto methods
  */
class ParticipantTopologyManager(
    clock: Clock,
    override val store: TopologyStore,
    crypto: Crypto,
    override protected val timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyManager[ParticipantTopologyManagerError](
      clock,
      crypto,
      store,
      timeouts,
      loggerFactory,
    )(ec) {

  private val observers = mutable.ListBuffer[ParticipantTopologyManagerObserver]()
  def addObserver(observer: ParticipantTopologyManagerObserver): Unit = blocking(synchronized {
    val _ = observers += observer
  })

  private val postInitCallbacks = new AtomicReference[Option[PostInitCallbacks]](None)
  def setPostInitCallbacks(callbacks: PostInitCallbacks): Unit =
    postInitCallbacks.set(Some(callbacks))

  private val participantIdO = new AtomicReference[Option[ParticipantId]](None)
  def setParticipantId(participantId: ParticipantId) = participantIdO.set(Some(participantId))

  private def packageService(implicit
      traceContext: TraceContext
  ): EitherT[Future, ParticipantTopologyManagerError, PostInitCallbacks] =
    EitherT.fromEither[Future](
      postInitCallbacks
        .get()
        .toRight(
          ParticipantTopologyManagerError.UninitializedParticipant
            .Reject("Can not vet packages on an uninitialised participant")
        )
    )

  // function to support sequential reading from identity store
  def sequentialStoreRead(run: => Future[Unit], description: String): Future[Unit] = {
    sequentialQueue.execute(run, description)
  }

  override protected def wrapError(error: TopologyManagerError)(implicit
      traceContext: TraceContext
  ): ParticipantTopologyManagerError =
    IdentityManagerParentError(error)

  override protected def notifyObservers(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Future[Unit] =
    blocking(synchronized(observers.toList))
      .traverse(_.addedNewTransactions(timestamp, transactions))
      .map(_ => ())
      .onShutdown(())

  private def checkOwnerToKeyMappingRefersToExistingKeys(
      participantId: ParticipantId,
      mapping: OwnerToKeyMapping,
      force: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, ParticipantTopologyManagerError, Unit] = {

    mapping match {
      // if tx is for this node, check that we do have this key
      case OwnerToKeyMapping(`participantId`, key) =>
        crypto.cryptoPrivateStore
          .existsPrivateKey(key.fingerprint)
          .leftMap(err => wrapError(TopologyManagerError.InternalError.CryptoPrivateError(err)))
          .subflatMap { exists =>
            if (exists) {
              Right(())
            } else {
              Left(
                ParticipantTopologyManagerError.DangerousKeyUseCommandRequiresForce
                  .NoSuchKey(key.fingerprint)
              )
            }
          }

      // if tx for another node, we require force
      case OwnerToKeyMapping(pid: ParticipantId, _) if !force =>
        EitherT.leftT(
          ParticipantTopologyManagerError.DangerousKeyUseCommandRequiresForce.AlienParticipant(pid)
        )
      // anything else, pass through
      case _ => EitherT.rightT(())
    }
  }

  private def checkPackageVettingRefersToExistingPackages(
      participantId: ParticipantId,
      callbacks: PostInitCallbacks,
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, ParticipantTopologyManagerError, Unit] =
    for {
      _ <- (transaction.transaction match {
        case TopologyStateUpdate(
              TopologyChangeOp.Add,
              TopologyStateUpdateElement(_, VettedPackages(pid, packageIds)),
            ) if participantId == pid && !force =>
          for {
            dependencies <- callbacks
              .packageDependencies(packageIds.toList)
              .leftMap(ParticipantTopologyManagerError.CannotVetDueToMissingPackages.Missing(_))
            unvetted <- EitherT.right(unvettedPackages(pid, dependencies))
            _ <- EitherT
              .cond[Future](
                unvetted.isEmpty,
                (),
                ParticipantTopologyManagerError.DependenciesNotVetted
                  .Reject(unvetted),
              ): EitherT[Future, ParticipantTopologyManagerError, Unit]
          } yield ()
        case TopologyStateUpdate(op, TopologyStateUpdateElement(_, vp @ VettedPackages(_, _))) =>
          if (force) {
            logger.info(show"Using force to authorize $op of $vp")
            EitherT.rightT[Future, ParticipantTopologyManagerError](())
          } else {
            EitherT.leftT(
              ParticipantTopologyManagerError.DangerousVettingCommandsRequireForce.Reject()
            )
          }
        case _ =>
          EitherT.rightT[Future, ParticipantTopologyManagerError](())
      }): EitherT[Future, ParticipantTopologyManagerError, Unit]
    } yield ()

  private def runWithParticipantId(
      run: ParticipantId => EitherT[Future, ParticipantTopologyManagerError, Unit]
  )(implicit traceContext: TraceContext): EitherT[Future, ParticipantTopologyManagerError, Unit] =
    runIfInitialized(participantIdO, "Participant id is not set yet", run)

  private def runIfInitialized[A](
      itemO: AtomicReference[Option[A]],
      msg: String,
      run: A => EitherT[Future, ParticipantTopologyManagerError, Unit],
  )(implicit traceContext: TraceContext): EitherT[Future, ParticipantTopologyManagerError, Unit] =
    itemO
      .get()
      .fold(
        EitherT.leftT[Future, Unit](
          ParticipantTopologyManagerError.UninitializedParticipant
            .Reject(msg): ParticipantTopologyManagerError
        )
      )(run(_))

  override protected def checkNewTransaction(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, ParticipantTopologyManagerError, Unit] =
    transaction.transaction.element.mapping match {
      case x: OwnerToKeyMapping if transaction.operation == TopologyChangeOp.Add =>
        runWithParticipantId(checkOwnerToKeyMappingRefersToExistingKeys(_, x, force))
      case _: VettedPackages =>
        runWithParticipantId(participantId =>
          runIfInitialized(
            postInitCallbacks,
            "Post init callbacks are not set yet",
            (callbacks: PostInitCallbacks) =>
              checkPackageVettingRefersToExistingPackages(
                participantId,
                callbacks,
                transaction,
                force,
              ),
          )
        )
      case _ => EitherT.rightT(())
    }

  def issueParticipantDomainStateCert(participantId: ParticipantId, domainId: DomainId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[ParticipantTopologyManagerError, Unit]] = {

    def alreadyTrusted: EitherT[Future, ParticipantTopologyManagerError, Boolean] =
      EitherT.right(
        store
          .findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = true,
            includeSecondary = false,
            types = Seq(DomainTopologyTransactionType.ParticipantState),
            filterUid = Some(Seq(participantId.uid)),
            filterNamespace = None,
          )
          .map(_.adds.result.iterator.map(_.transaction.transaction.element.mapping).exists {
            case ParticipantState(side, `domainId`, `participantId`, permission, _)
                if side != RequestSide.From && permission.isActive =>
              true
            case _ => false
          })
      )

    def trustDomain: EitherT[Future, ParticipantTopologyManagerError, Unit] =
      authorize(
        TopologyStateUpdate.createAdd(
          ParticipantState(
            RequestSide.To,
            domainId,
            participantId,
            ParticipantPermission.Submission,
            TrustLevel.Ordinary,
          )
        ),
        signingKey = None,
        force = false,
        replaceExisting = true,
      ).map(_ => ())

    // check if cert already exists
    performUnlessClosingF {
      (for {
        have <- alreadyTrusted
        _ <- if (have) EitherT.rightT[Future, ParticipantTopologyManagerError](()) else trustDomain
      } yield ()).value
    }
  }

  private def unvettedPackages(pid: ParticipantId, packages: Set[PackageId])(implicit
      traceContext: TraceContext
  ): Future[Set[PackageId]] = {
    this.store
      .findPositiveTransactions(
        CantonTimestamp.MaxValue,
        asOfInclusive = true,
        includeSecondary = false,
        types = Seq(VettedPackages.dbType),
        filterUid = Some(Seq(pid.uid)),
        filterNamespace = None,
      )
      .map { current =>
        current.adds.toIdentityState
          .foldLeft(packages) {
            case (acc, TopologyStateUpdateElement(_, vs: VettedPackages)) =>
              acc -- vs.packageIds
            case (acc, _) => acc
          }
      }
  }

  def vetPackages(packages: Seq[PackageId], syncVetting: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ParticipantTopologyManagerError, Unit] = {
    val packageSet = packages.toSet

    def authorizeVetting(
        pid: ParticipantId
    ): EitherT[Future, ParticipantTopologyManagerError, SignedTopologyTransaction[
      TopologyChangeOp
    ]] =
      authorize(
        TopologyStateUpdate.createAdd(VettedPackages(pid, packages)),
        None,
        force = false,
      )

    def waitForPackagesBeingVetted(
        pid: ParticipantId,
        clients: () => Seq[DomainTopologyClient],
    ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Boolean] =
      if (syncVetting) {
        EitherT.right(
          clients()
            .traverse {
              _.await(
                _.findUnvettedPackagesOrDependencies(pid, packageSet).value.map(x =>
                  x.exists(_.isEmpty)
                ),
                timeouts.network.duration,
              )
            }
            .map(_ => true)
        )
      } else EitherT.rightT(false)

    for {
      // get package service (stored in an atomic reference)
      participantId <- EitherT.fromEither[Future](
        participantIdO
          .get()
          .toRight(
            ParticipantTopologyManagerError.UninitializedParticipant
              .Reject("Participant id is not yet set")
          )
      )
      callbacks <- packageService
      isVetted <- EitherT.right(unvettedPackages(participantId, packageSet).map(_.isEmpty))
      _ <-
        if (isVetted) {
          logger.debug(show"The following packages are already vetted: ${packages}")
          EitherT.rightT[Future, ParticipantTopologyManagerError](())
        } else {
          authorizeVetting(participantId).map(_ => ())
        }
      // register our handle to wait for the packages being vetted
      appeared <- waitForPackagesBeingVetted(participantId, () => callbacks.clients())
        .onShutdown(Right(false))
    } yield {
      if (appeared) {
        logger.debug("Packages appeared on all connected domains.")
      }
    }
  }

}

object ParticipantTopologyManager {
  // the package service depends on the topology manager and vice versa. therefore, we do have to inject these callbacks after init
  trait PostInitCallbacks {
    def clients(): Seq[DomainTopologyClient]
    def packageDependencies(packages: List[PackageId])(implicit
        traceContext: TraceContext
    ): EitherT[Future, PackageId, Set[PackageId]]
  }

}

sealed trait ParticipantTopologyManagerError extends CantonError
object ParticipantTopologyManagerError extends ParticipantErrorGroup {

  case class IdentityManagerParentError(parent: TopologyManagerError)(implicit
      val loggingContext: ErrorLoggingContext
  ) extends ParticipantTopologyManagerError
      with ParentCantonError[TopologyManagerError] {
    override def logOnCreation: Boolean = false
  }

  @Explanation(
    """This error indicates that a package vetting command failed due to packages not existing locally.
      |This can be due to either the packages not being present or their dependencies being missing.
      |When vetting a package, the package must exist on the participant, as otherwise the participant
      |will not be able to process a transaction relying on a particular package."""
  )
  @Resolution(
    "Ensure that the package exists locally before issuing such a transaction."
  )
  object CannotVetDueToMissingPackages
      extends ErrorCode(
        id = "CANNOT_VET_DUE_TO_MISSING_PACKAGES",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    case class Missing(packages: PackageId)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Package vetting failed due to packages not existing on the local node"
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates that a dangerous package vetting command was rejected.
      |This is the case if a vetting command, if not run correctly, could potentially lead to a ledger fork.
      |The vetting authorization checks the participant for the presence of the given set of
      |packages (including their dependencies) and allows only to vet for the given participant id.
      |In rare cases where a more centralised topology manager is used, this behaviour can be overridden
      |with force. However, if a package is vetted but not present on the participant, the participant will
      |refuse to process any transaction of the given domain until the problematic package has been uploaded."""
  )
  @Resolution("Set force=true if you really know what you are doing.")
  object DangerousVettingCommandsRequireForce
      extends ErrorCode(
        id = "DANGEROUS_VETTING_COMMANDS_REQUIRE_FORCE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Reject()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Package vetting failed due to packages not existing on the local node"
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates a vetting request failed due to dependencies not being vetted.
      |On every vetting request, the set supplied packages is analysed for dependencies. The
      |system requires that not only the main packages are vetted explicitly but also all dependencies.
      |This is necessary as not all participants are required to have the same packages installed and therefore
      |not every participant can resolve the dependencies implicitly."""
  )
  @Resolution("Vet the dependencies first and then repeat your attempt.")
  object DependenciesNotVetted
      extends ErrorCode(
        id = "DEPENDENCIES_NOT_VETTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Reject(unvetted: Set[PackageId])(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Package vetting failed due to dependencies not being vetted"
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates that a request involving topology management was attempted on a participant that is not yet initialised.
      |During initialisation, only namespace and identifier delegations can be managed."""
  )
  @Resolution("Initialise the participant and retry.")
  object UninitializedParticipant
      extends ErrorCode(
        id = "UNINITIALIZED_PARTICIPANT",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Reject(_cause: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = _cause
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates that a dangerous owner to key mapping authorization was rejected.
      |This is the case if a command is run that could break a participant.
      |If the command was run to assign a key for the given participant, then the command 
      |was rejected because the key is not in the participants private store.
      |If the command is run on a participant to issue transactions for another participant, 
      |then such commands must be run with force, as they are very dangerous and could easily break
      |the participant.
      |As an example, if we assign an encryption key to a participant that the participant does not
      |have, then the participant will be unable to process an incoming transaction. Therefore we must
      |be very careful to not create such situations.
      | """
  )
  @Resolution("Set force=true if you really know what you are doing.")
  object DangerousKeyUseCommandRequiresForce
      extends ErrorCode(
        id = "DANGEROUS_KEY_USE_COMMAND_REQUIRES_FORCE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class AlienParticipant(participant: ParticipantId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Issuing owner to key mappings for alien participants requires force=yes"
        )
        with ParticipantTopologyManagerError
    case class NoSuchKey(fingerprint: Fingerprint)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = show"Can not assign unknown key $fingerprint to this participant"
        )
        with ParticipantTopologyManagerError
  }

}
