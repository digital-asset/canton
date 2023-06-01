// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.config.TopologyConfig
import com.digitalasset.canton.domain.topology.DomainTopologyManagerError.ParticipantNotInitialized
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponseResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
}
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  SnapshotAuthorizationValidator,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

private[domain] trait RequestProcessingStrategy {

  def decide(
      requestedBy: Member,
      participant: ParticipantId,
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[List[messages.RegisterTopologyTransactionResponseResult.State]]

}

private[domain] object RequestProcessingStrategy {

  import RegisterTopologyTransactionResponseResult.State.*

  trait ManagerHooks {
    def addFromRequest(
        transaction: SignedTopologyTransaction[TopologyChangeOp]
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, DomainTopologyManagerError, Unit]

    def issueParticipantStateForDomain(participantId: ParticipantId)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, DomainTopologyManagerError, Unit]
  }

  class Impl(
      config: TopologyConfig,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      authorizedStore: TopologyStore[AuthorizedStore],
      targetDomainClient: DomainTopologyClient,
      hooks: ManagerHooks,
      timeouts: ProcessingTimeout,
      val loggerFactory: NamedLoggerFactory,
      futureSupervisor: FutureSupervisor,
  )(implicit ec: ExecutionContext)
      extends RequestProcessingStrategy
      with NamedLogging {

    protected def awaitParticipantIsActive(participantId: ParticipantId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Boolean] =
      targetDomainClient.await(_.isParticipantActive(participantId), timeouts.network.unwrap)

    private val authorizedSnapshot = makeSnapshot(authorizedStore)

    private def makeSnapshot(store: TopologyStore[TopologyStoreId]): StoreBasedTopologySnapshot =
      new StoreBasedTopologySnapshot(
        CantonTimestamp.MaxValue, // we use a max value here, as this will give us the "head snapshot" transactions (valid_from < t && until.isNone)
        store,
        Map(),
        useStateTxs = false,
        packageDependencies = StoreBasedDomainTopologyClient.NoPackageDependencies,
        loggerFactory,
      )

    private def toResult[T](
        eitherT: EitherT[FutureUnlessShutdown, DomainTopologyManagerError, T]
    ): FutureUnlessShutdown[RegisterTopologyTransactionResponseResult.State] =
      eitherT
        .leftMap {
          case DomainTopologyManagerError.TopologyManagerParentError(
                TopologyManagerError.DuplicateTransaction.Failure(_, _)
              ) | DomainTopologyManagerError.TopologyManagerParentError(
                TopologyManagerError.MappingAlreadyExists.Failure(_, _)
              ) =>
            Duplicate
          case DomainTopologyManagerError.TopologyManagerParentError(err)
              if err.code == TopologyManagerError.NoCorrespondingActiveTxToRevoke =>
            Obsolete
          case err => Failed
        }
        .map(_ => Accepted)
        .merge

    private def addTransactions(
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]]
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[List[RegisterTopologyTransactionResponseResult.State]] = {
      def process(transaction: SignedTopologyTransaction[TopologyChangeOp]) =
        FutureUnlessShutdown.outcomeF(authorizedStore.exists(transaction)).flatMap {
          case false => toResult(hooks.addFromRequest(transaction))
          case true => FutureUnlessShutdown.pure(Duplicate)
        }
      for {
        res <- MonadUtil.sequentialTraverse(transactions)(process)
      } yield res.toList
    }

    def processExistingParticipantRequest(
        participant: ParticipantId,
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[List[RegisterTopologyTransactionResponseResult.State]] = {
      // TODO(i4933) enforce limits with respect to parties, packages, keys, certificates etc.
      for {
        isActive <- FutureUnlessShutdown.outcomeF(
          authorizedSnapshot.isParticipantActive(participant)
        )
        res <-
          if (isActive) addTransactions(transactions)
          else {
            logger.warn(
              s"Failed to process participant ${participant} request as participant is not active"
            )
            FutureUnlessShutdown.pure(rejectAll(transactions))
          }
      } yield res
    }

    private def rejectAll(
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]]
    ): List[RegisterTopologyTransactionResponseResult.State] =
      transactions.map(_ => RegisterTopologyTransactionResponseResult.State.Rejected)

    private def failAll(
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]]
    ): List[RegisterTopologyTransactionResponseResult.State] =
      transactions.map(_ => RegisterTopologyTransactionResponseResult.State.Failed)

    private def validateOnlyOnboardingTransactions(
        participantId: ParticipantId,
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, DomainTopologyManagerError, Unit] = if (protocolVersion < ProtocolVersion.v3)
      EitherT.pure(())
    else {
      val allowedSet =
        TopologyStore.initialParticipantDispatchingSet + DomainTopologyTransactionType.NamespaceDelegation + DomainTopologyTransactionType.IdentifierDelegation
      EitherT
        .fromEither[Future](transactions.traverse { tx =>
          for {
            _ <- Either.cond(
              allowedSet.contains(tx.transaction.element.mapping.dbType),
              (),
              s"Transaction type of ${tx.transaction.element.mapping} must not be sent as part of an on-boarding request",
            )
            _ <- Either.cond(
              tx.transaction.op == TopologyChangeOp.Add,
              (),
              s"Invalid operation of ${tx.transaction.element.mapping}: ${tx.operation}",
            )
            _ <- Either.cond(
              tx.transaction.element.mapping.requiredAuth.uids.forall(_ == participantId.uid),
              (),
              s"Invalid transaction uids in ${tx.transaction.element.mapping} not corresponding to the participant uid ${participantId.uid}",
            )
            _ <- Either.cond(
              tx.transaction.element.mapping.requiredAuth.namespaces._1
                .forall(_ == participantId.uid.namespace),
              (),
              s"Invalid transaction namespaces in ${tx.transaction.element.mapping} not corresponding to participant namespace ${participantId.uid.namespace}",
            )
          } yield ()
        })
        .leftMap[DomainTopologyManagerError](err =>
          DomainTopologyManagerError.InvalidOrFaultyOnboardingRequest
            .Failure(participantId = participantId, reason = err)
        )
        .map(_ => ())
    }

    private def validateAuthorizationOfOnboardingTransactions(
        participantId: ParticipantId,
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, DomainTopologyManagerError, Unit] = {

      val store =
        new InMemoryTopologyStore(
          AuthorizedStore,
          loggerFactory,
          timeouts,
          futureSupervisor,
        )
      val validator =
        new SnapshotAuthorizationValidator(
          CantonTimestamp.MaxValue,
          store,
          timeouts,
          loggerFactory,
          futureSupervisor,
        )

      def requiresReset(tx: SignedTopologyTransaction[TopologyChangeOp]): Boolean =
        tx.transaction.element.mapping.dbType == DomainTopologyTransactionType.NamespaceDelegation ||
          tx.transaction.element.mapping.dbType == DomainTopologyTransactionType.IdentifierDelegation

      // check that all transactions are authorized
      EitherT(
        MonadUtil.foldLeftM(
          Right(()): Either[DomainTopologyManagerError, Unit],
          transactions.zipWithIndex,
        ) {
          case (Right(_), (tx, idx)) =>
            val ts = CantonTimestamp.Epoch.plusMillis(idx.toLong)
            // incrementally add it to the store and check the validation
            for {
              isValidated <- validator.authorizedBy(tx).map(_.nonEmpty)
              _ <- FutureUnlessShutdown.outcomeF(
                store.append(
                  SequencedTime(ts),
                  EffectiveTime(ts),
                  Seq(ValidatedTopologyTransaction(tx, None)),
                )
              )
              // if the transaction was a namespace delegation, drop it
              _ <- if (requiresReset(tx)) validator.reset() else FutureUnlessShutdown.unit
            } yield Either.cond(
              isValidated,
              (),
              DomainTopologyManagerError.InvalidOrFaultyOnboardingRequest.Failure(
                participantId,
                s"Unauthorized topology transaction in onboarding request: $tx",
              ),
            )
          case (acc, _) => FutureUnlessShutdown.pure(acc)
        }
      )
    }

    def processOnboardingRequest(
        participant: ParticipantId,
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[List[RegisterTopologyTransactionResponseResult.State]] = {
      // create a temporary store so we can use some convenience functions
      val store = new InMemoryTopologyStore(
        TopologyStoreId.AuthorizedStore,
        loggerFactory,
        timeouts,
        futureSupervisor,
      )
      val ts = CantonTimestamp.Epoch
      val initF = store.append(
        SequencedTime(ts),
        EffectiveTime(ts),
        transactions.map(ValidatedTopologyTransaction(_, None)),
      )

      val snapshot = makeSnapshot(store)
      type S = SignedTopologyTransaction[TopologyChangeOp]

      def extract(items: Seq[S])(matcher: TopologyMapping => Boolean) =
        items.partition(elem =>
          elem.transaction.op == TopologyChangeOp.Add &&
            matcher(elem.transaction.element.mapping)
        )

      val (participantStates, reduced1) = extract(transactions) {
        case ParticipantState(side, domain, pid, permission, _) =>
          domain == domainId && participant == pid && (side == RequestSide.To || side == RequestSide.Both) && permission.isActive
        case _ => false
      }

      def reject(msg: String) =
        DomainTopologyManagerError.ParticipantNotInitialized.Reject(participant, msg)

      def maliciousOrFaulty(msg: String) =
        DomainTopologyManagerError.InvalidOrFaultyOnboardingRequest.Failure(participant, msg)

      // if not open, check if domain trust certificate exists
      // check that we have:
      //   - owner to key mappings
      //   - domain trust certificate
      //   - if necessary, legal certificate
      //   - plus all delegations
      // if domain is open, then issue domain trust certificate,
      val ret = for {
        _ <- EitherT.right(initF).mapK(FutureUnlessShutdown.outcomeK)
        // check that all transactions are authorized (note, they could still be out of order)
        _ <- validateAuthorizationOfOnboardingTransactions(participant, transactions)
        // check that we only got the transactions we expected
        _ <- validateOnlyOnboardingTransactions(participant, transactions).mapK(
          FutureUnlessShutdown.outcomeK
        )
        // check that we have a domain trust certificate of the participant
        _ <- EitherT.cond[FutureUnlessShutdown](
          participantStates.nonEmpty,
          (),
          maliciousOrFaulty(s"Participant ${participant} has not sent a domain trust certificate"),
        )
        domainTrustsParticipant <- participantIsAllowed(participant).mapK(
          FutureUnlessShutdown.outcomeK
        )
        _ <- EitherT.cond[FutureUnlessShutdown](
          domainTrustsParticipant || config.open,
          (),
          reject(s"Participant ${participant} is not on the allow-list of this closed network"),
        )
        keys <- EitherT.right(snapshot.allKeys(participant)).mapK(FutureUnlessShutdown.outcomeK)
        _ <- EitherT.cond[FutureUnlessShutdown](
          keys.hasBothKeys(),
          (),
          DomainTopologyManagerError.ParticipantNotInitialized.Failure(participant, keys),
        )
        participantCert <- EitherT
          .right(snapshot.findParticipantCertificate(participant))
          .mapK(FutureUnlessShutdown.outcomeK)
        _ <- EitherT.cond[FutureUnlessShutdown](
          participantCert.nonEmpty || !config.requireParticipantCertificate,
          (),
          reject(
            s"Participant $participant needs a SignedLegalIdentityClaim, but did not provide one"
          ),
        )
        res <- EitherT.right(addTransactions(reduced1.toList))
        // Finally, add the participant state (keys need to be pushed before the participant is active)
        rest <- EitherT
          .right(addTransactions(participantStates.toList))
        // Activate the participant
        _ <-
          if (!domainTrustsParticipant)
            hooks.issueParticipantStateForDomain(participant)
          else EitherT.rightT[FutureUnlessShutdown, DomainTopologyManagerError](())
        // wait until the participant has become active on the domain. we do that such that this
        // request doesn't terminate before the participant can proceed with subscribing.
        // on shutdown, we don't wait, as the participant will subsequently fail to subscribe anyway
        active <- EitherT
          .right[DomainTopologyManagerError](
            targetDomainClient.await(
              _.isParticipantActive(participant),
              timeouts.network.unwrap,
            )
          )
      } yield {
        if (active) {
          logger.info(s"Successfully onboarded $participant")
        } else {
          logger.error(
            s"Auto-activation of participant $participant initiated, but was not observed within ${timeouts.network}"
          )
        }
        res ++ rest
      }
      ret.fold(
        {
          case ParticipantNotInitialized.Reject(_, _) => rejectAll(transactions)
          case _ => failAll(transactions)
        },
        x => x,
      )
    }

    private def participantIsAllowed(
        pid: ParticipantId
    )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Boolean] = {
      EitherT.right(
        authorizedStore
          .findPositiveTransactions(
            asOf =
              CantonTimestamp.MaxValue, // use max timestamp to select the "head snapshot" of the authorized store
            asOfInclusive = false,
            includeSecondary = false,
            types = Seq(DomainTopologyTransactionType.ParticipantState),
            filterUid = Some(Seq(pid.uid)),
            filterNamespace = None,
          )
          .map { res =>
            res.toIdentityState.exists {
              case TopologyStateUpdateElement(_, x: ParticipantState) => x.side == RequestSide.From
              case _ => false
            }
          }
      )
    }

    override def decide(
        requestedBy: Member,
        participant: ParticipantId,
        transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[List[RegisterTopologyTransactionResponseResult.State]] = {
      requestedBy match {
        case UnauthenticatedMemberId(uid) =>
          // check that we have:
          //   - owner to key mappings
          //   - domain trust certificate
          //   - if necessary, legal certificate
          //   - plus all delegations
          // if domain is open, then issue domain trust certificate,
          // if not open, check if domain trust certificate exists
          processOnboardingRequest(participant, transactions)
        case requester: ParticipantId if requester == participant =>
          processExistingParticipantRequest(participant, transactions)
        case member: AuthenticatedMember =>
          logger.warn(s"RequestedBy ${requestedBy} does not match participant ${participant}")
          FutureUnlessShutdown.pure(rejectAll(transactions))
      }
    }
  }

}

private[domain] class DomainTopologyManagerRequestService(
    strategy: RequestProcessingStrategy,
    crypto: CryptoPureApi,
    protocolVersion: ProtocolVersion,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with DomainTopologyManagerRequestService.Handler {

  import RegisterTopologyTransactionResponseResult.State.*

  override def newRequest(
      requestedBy: Member,
      participant: ParticipantId,
      res: List[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[List[RegisterTopologyTransactionResponseResult]] = {
    for {
      // run pre-checks first
      preChecked <- FutureUnlessShutdown.outcomeF(res.parTraverse(preCheck))
      preCheckedTx = res.zip(preChecked)
      valid = preCheckedTx.collect { case (tx, Accepted) =>
        tx
      }
      // pass the tx that passed the pre-check to the strategy
      outcome <- strategy.decide(requestedBy, participant, valid)
    } yield {
      val (rest, result) =
        preCheckedTx.foldLeft((outcome, List.empty[RegisterTopologyTransactionResponseResult])) {
          case ((result :: rest, acc), (tx, Accepted)) =>
            (
              rest,
              acc :+ RegisterTopologyTransactionResponseResult.create(
                tx.uniquePath.toProtoPrimitive,
                result,
                protocolVersion,
              ),
            )
          case ((rest, acc), (tx, failed)) =>
            (
              rest,
              acc :+ RegisterTopologyTransactionResponseResult.create(
                tx.uniquePath.toProtoPrimitive,
                failed,
                protocolVersion,
              ),
            )
        }
      ErrorUtil.requireArgument(rest.isEmpty, "rest should be empty!")
      ErrorUtil.requireArgument(
        result.lengthCompare(res) == 0,
        "result should have same length as tx",
      )
      val output = res
        .zip(result)
        .map { case (tx, response) =>
          show"${response.state.toString} -> ${tx.transaction.op} ${tx.transaction.element.mapping}"
        }
        .mkString("\n  ")
      logger.info(
        show"Register topology request by ${requestedBy} for $participant yielded\n  $output"
      )
      result
    }
  }

  private def preCheck(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  ): Future[RegisterTopologyTransactionResponseResult.State] = {
    (for {
      // check validity of signature
      _ <- EitherT
        .fromEither[Future](transaction.verifySignature(crypto))
        .leftMap(_ => Failed)
    } yield Accepted).merge
  }

}

private[domain] object DomainTopologyManagerRequestService {

  trait Handler {
    def newRequest(
        requestedBy: Member,
        participant: ParticipantId,
        res: List[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[List[RegisterTopologyTransactionResponseResult]]
  }

  def create(
      config: TopologyConfig,
      manager: DomainTopologyManager,
      domainClient: DomainTopologyClient,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      futureSupervisor: FutureSupervisor,
  )(implicit ec: ExecutionContext): DomainTopologyManagerRequestService = {

    new DomainTopologyManagerRequestService(
      new RequestProcessingStrategy.Impl(
        config,
        manager.id.domainId,
        manager.protocolVersion,
        manager.store,
        domainClient,
        manager,
        timeouts,
        loggerFactory,
        futureSupervisor,
      ),
      manager.crypto.pureCrypto,
      manager.protocolVersion,
      loggerFactory,
    )
  }
}
