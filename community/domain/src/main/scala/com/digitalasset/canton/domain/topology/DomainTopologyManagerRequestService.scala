// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import cats.syntax.functor._
import cats.syntax.traverse._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.config.TopologyConfig
import com.digitalasset.canton.domain.topology.RequestProcessingStrategy.{
  AutoApproveStrategy,
  AutoRejectStrategy,
  QueueStrategy,
}
import com.digitalasset.canton.domain.topology.RequestProcessingStrategyConfig.{
  AutoApprove,
  Queue,
  Reject,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown.syntax._
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Add
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.RequestedStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreFactory,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, HasProtoV0, MonadUtil}

import scala.concurrent.{ExecutionContext, Future}

sealed trait RegisterTopologyTransactionRequestState {
  def errorString: Option[String] = None
}

object RegisterTopologyTransactionRequestState {

  object Requested extends RegisterTopologyTransactionRequestState
  case class Failed(error: String) extends RegisterTopologyTransactionRequestState {
    override def errorString: Option[String] = Some(error)
  }
  object Rejected extends RegisterTopologyTransactionRequestState
  object Accepted extends RegisterTopologyTransactionRequestState
  object Duplicate extends RegisterTopologyTransactionRequestState

  /** Unnecessary removes are marked as obsolete */
  object Obsolete extends RegisterTopologyTransactionRequestState

}

/** Policy that determines whether/how the domain topology manager approves topology transactions. */
sealed trait RequestProcessingStrategyConfig
object RequestProcessingStrategyConfig {

  /** Dictates to approve all topology transactions.
    *
    * @param autoEnableParticipant determines whether a participant is enabled automatically
    *                              when an [[com.digitalasset.canton.topology.transaction.OwnerToKeyMapping]]
    *                              is authorized.
    */
  case class AutoApprove(autoEnableParticipant: Boolean) extends RequestProcessingStrategyConfig

  /** When an topology transaction is submitted, the topology manager stores it as inactive.
    * The transaction becomes effective, as soon as the domain administrator decides to activate it.
    */
  object Queue extends RequestProcessingStrategyConfig

  /** Dictates to reject all topology transactions. */
  object Reject extends RequestProcessingStrategyConfig
}

trait RequestProcessingStrategy {

  def decide(transactions: List[SignedTopologyTransaction[TopologyChangeOp]])(implicit
      traceContext: TraceContext
  ): Future[List[RegisterTopologyTransactionRequestState]]

}

object RequestProcessingStrategy {

  import RegisterTopologyTransactionRequestState._

  class AutoApproveStrategy(
      manager: DomainTopologyManager,
      targetDomainClient: DomainTopologyClient,
      autoEnableParticipant: Boolean,
      requireParticipantCert: Boolean,
      timeouts: ProcessingTimeout,
      val loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext)
      extends RequestProcessingStrategy
      with NamedLogging {

    protected def awaitParticipantIsActive(participantId: ParticipantId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Boolean] =
      targetDomainClient.await(
        _.isParticipantActive(participantId),
        timeouts.network.unwrap,
      )

    private def findPotentialActivations(
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
    ): Set[ParticipantId] = {
      transactions.iterator
        .filter(_.transaction.op == Add)
        .map(_.transaction.element.mapping)
        .collect {
          // if a new key has been added
          case OwnerToKeyMapping(pid @ ParticipantId(_), _) => pid
          // or a new trust certificate
          case ParticipantState(_, _, pid, _, _) => pid
          // or a legal identity claim
          case SignedLegalIdentityClaim(uid, _, _) => ParticipantId(uid)
        }
        .toSet
    }

    override def decide(transactions: List[SignedTopologyTransaction[TopologyChangeOp]])(implicit
        traceContext: TraceContext
    ): Future[List[RegisterTopologyTransactionRequestState]] = {

      def toResult[T](
          eitherT: EitherT[Future, DomainTopologyManagerError, T]
      ): Future[RegisterTopologyTransactionRequestState] =
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
            case err => Failed(err.toString)
          }
          .map(_ => Accepted)
          .merge

      def process(transaction: SignedTopologyTransaction[TopologyChangeOp]) =
        toResult(manager.add(transaction, force = true))

      for {
        res <- MonadUtil.sequentialTraverse(transactions)(process)
        activations = if (autoEnableParticipant) findPotentialActivations(transactions) else Seq()
        _ <- activations.toList.traverse(eventuallyActivateParticipant)
      } yield res.toList

    }

    private def participantTrustsDomain(
        pid: ParticipantId
    )(implicit traceContext: TraceContext): EitherT[Future, DomainTopologyManagerError, Boolean] = {
      EitherT.right(
        manager.store
          .findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = false,
            includeSecondary = false,
            types = Seq(DomainTopologyTransactionType.ParticipantState),
            filterUid = Some(Seq(pid.uid)),
            filterNamespace = None,
          )
          .map { res =>
            res.toIdentityState.exists {
              case TopologyStateUpdateElement(_, x: ParticipantState)
                  if x.side != RequestSide.From =>
                true
              case _ => false
            }
          }
      )
    }

    private def eventuallyActivateParticipant(
        pid: ParticipantId
    )(implicit traceContext: TraceContext): Future[Unit] = {
      val client = new StoreBasedTopologySnapshot(
        CantonTimestamp.MaxValue,
        manager.store,
        Map(),
        useStateTxs = false,
        StoreBasedDomainTopologyClient.NoPackageDependencies,
        loggerFactory,
      )
      val ret = for {
        participantCert <- EitherT.right(client.findParticipantCertificate(pid))
        hasParticipantState <- EitherT.right(client.findParticipantState(pid).map(_.isDefined))
        hasParticipantKeys <- EitherT.right(client.allKeys(pid).map(_.hasBothKeys()))
        hasCert = (requireParticipantCert && participantCert.isDefined) || !requireParticipantCert
        trustsDomain <- participantTrustsDomain(pid)
        _ <-
          if (!hasParticipantState && hasParticipantKeys && hasCert && trustsDomain) {
            logger.debug(s"Auto-enabling new participant $pid")
            for {
              // Add the new participant
              _ <- manager.addParticipant(pid, participantCert)
              // Activate the participant
              _ <- manager.authorize(
                TopologyStateUpdate.createAdd(
                  ParticipantState(
                    RequestSide.From,
                    manager.managerId.domainId,
                    pid,
                    ParticipantPermission.Submission,
                    TrustLevel.Ordinary,
                  )
                ),
                signingKey = None,
                force = false,
                replaceExisting = true,
              )
              // wait until the participant has become active on the domain. we do that such that this
              // request doesn't terminate before the participant can proceed with subscribing.
              // on shutdown, we don't wait, as the participant will subsequently fail to subscribe anyway
              active <- EitherT
                .right[DomainTopologyManagerError](awaitParticipantIsActive(pid))
                .onShutdown(Right(true))
            } yield {
              if (!active) {
                logger.error(
                  s"Auto-activation of participant $pid initiated, but was not observed within ${timeouts.network}"
                )
              }
            }
          } else EitherT.rightT[Future, DomainTopologyManagerError](())
      } yield ()
      EitherTUtil.logOnError(ret, s"Failed to auto-activate participant $pid").value.void
    }
  }

  class AutoRejectStrategy extends RequestProcessingStrategy {
    override def decide(transactions: List[SignedTopologyTransaction[TopologyChangeOp]])(implicit
        traceContext: TraceContext
    ): Future[List[RegisterTopologyTransactionRequestState]] =
      Future.successful(transactions.map(_ => Rejected))
  }

  class QueueStrategy(clock: Clock, store: TopologyStore, val loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext
  ) extends RequestProcessingStrategy
      with NamedLogging {

    override def decide(transactions: List[SignedTopologyTransaction[TopologyChangeOp]])(implicit
        traceContext: TraceContext
    ): Future[List[RegisterTopologyTransactionRequestState]] = {
      def process(
          transaction: SignedTopologyTransaction[TopologyChangeOp]
      ): Future[RegisterTopologyTransactionRequestState] =
        for {
          alreadyInStore <- store.exists(transaction)
          _ <-
            if (alreadyInStore) Future.unit
            else
              store.append(
                clock.uniqueTime(),
                List(ValidatedTopologyTransaction(transaction, None)),
              )
        } yield if (alreadyInStore) Duplicate else Requested

      MonadUtil.sequentialTraverse(transactions)(process).map(_.toList)
    }
  }
}

case class RequestResult(uniquePath: UniquePath, state: RegisterTopologyTransactionRequestState)
    extends HasProtoV0[v0.RegisterTopologyTransactionResponse.Result] {
  import RegisterTopologyTransactionRequestState._

  override def toProtoV0: v0.RegisterTopologyTransactionResponse.Result = {
    import v0.RegisterTopologyTransactionResponse.Result.State
    def reply(state: State, errString: String) =
      new v0.RegisterTopologyTransactionResponse.Result(
        uniquePath = uniquePath.toProtoPrimitive,
        state = state,
        errorMessage = errString,
      )
    state match {
      case Requested => reply(State.REQUESTED, "")
      case Failed(err) => reply(State.FAILED, err)
      case Rejected => reply(State.REJECTED, "")
      case Accepted => reply(State.ACCEPTED, "")
      case Duplicate => reply(State.DUPLICATE, "")
      case Obsolete => reply(State.OBSOLETE, "")
    }
  }
}

class DomainTopologyManagerRequestService(
    strategy: RequestProcessingStrategy,
    store: TopologyStore,
    crypto: CryptoPureApi,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  import RegisterTopologyTransactionRequestState._

  def newRequest(
      res: List[SignedTopologyTransaction[TopologyChangeOp]]
  )(implicit traceContext: TraceContext): Future[List[RequestResult]] = {
    for {
      // run pre-checks first
      preChecked <- res.traverse(preCheck)
      preCheckedTx = res.zip(preChecked)
      valid = preCheckedTx.collect { case (tx, Accepted) =>
        tx
      }
      // pass the tx that passed the pre-check to the strategy
      outcome <- strategy.decide(valid)
    } yield {
      val (rest, result) = preCheckedTx.foldLeft((outcome, List.empty[RequestResult])) {
        case ((result :: rest, acc), (tx, Accepted)) =>
          (rest, acc :+ RequestResult(tx.uniquePath, result))
        case ((rest, acc), (tx, failed)) => (rest, acc :+ RequestResult(tx.uniquePath, failed))
      }
      ErrorUtil.requireArgument(rest.isEmpty, "rest should be empty!")
      ErrorUtil.requireArgument(
        result.lengthCompare(res) == 0,
        "result should have same length as tx",
      )
      result
    }
  }

  private def preCheck(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): Future[RegisterTopologyTransactionRequestState] = {
    (for {
      // check validity of signature
      _ <- EitherT
        .fromEither[Future](transaction.verifySignature(crypto))
        .leftMap(err => Failed(s"Signature check failed with ${err}"))
      // check if transaction isn't already registered. note that this is checked again in the idm, so we just fail fast here
      _ <- EitherT(store.exists(transaction).map { alreadyExists =>
        Either.cond[RegisterTopologyTransactionRequestState, Unit](!alreadyExists, (), Duplicate)
      })
    } yield Accepted).merge
  }

}

object DomainTopologyManagerRequestService {
  def create(
      config: TopologyConfig,
      manager: DomainTopologyManager,
      domainClient: DomainTopologyClient,
      clock: Clock,
      storeFactory: TopologyStoreFactory,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): DomainTopologyManagerRequestService = {

    def createStrategy(strategyConfig: RequestProcessingStrategyConfig): RequestProcessingStrategy =
      strategyConfig match {
        case AutoApprove(activate) =>
          new AutoApproveStrategy(
            manager,
            domainClient,
            activate,
            config.requireParticipantCertificate,
            timeouts,
            loggerFactory,
          )
        case Reject => new AutoRejectStrategy()
        case Queue => new QueueStrategy(clock, storeFactory.forId(RequestedStore), loggerFactory)
      }

    new DomainTopologyManagerRequestService(
      createStrategy(config.permissioning),
      manager.store,
      manager.crypto.pureCrypto,
      loggerFactory,
    )
  }
}
