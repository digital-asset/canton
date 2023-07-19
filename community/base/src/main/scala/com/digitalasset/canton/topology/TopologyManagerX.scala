// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.TopologyStoreX
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.SimpleExecutionQueue
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

trait TopologyManagerObserver {
  def addedNewTransactions(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
}

class TopologyManagerX(
    val clock: Clock,
    val crypto: Crypto,
    val store: TopologyStoreX[AuthorizedStore],
    val timeouts: ProcessingTimeout,
    protocolVersion: ProtocolVersion,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  // sequential queue to run all the processing that does operate on the state
  protected val sequentialQueue = new SimpleExecutionQueue(
    "topology-manager-x-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  private val processor = new TopologyStateProcessorX(store, loggerFactory)

  def queueSize: Int = sequentialQueue.queueSize

  private val observers = new AtomicReference[Seq[TopologyManagerObserver]](Seq.empty)
  def addObserver(observer: TopologyManagerObserver): Unit =
    observers.updateAndGet(_ :+ observer).discard

  @VisibleForTesting
  def clearObservers(): Unit = observers.set(Seq.empty)

  /** Authorizes a new topology transaction by signing it and adding it to the topology state
    *
    * @param op              the operation that should be performed
    * @param mapping         the mapping that should be added
    * @param signingKeys     the key which should be used to sign
    * @param protocolVersion the protocol version corresponding to the transaction
    * @param expectFullAuthorization whether the transaction must be fully signed and authorized by keys on this node
    * @param force           force dangerous operations, such as removing the last signing key of a participant
    * @return the domain state (initialized or not initialized) or an error code of why the addition failed
    */
  def proposeAndAuthorize(
      op: TopologyChangeOpX,
      mapping: TopologyMappingX,
      serial: Option[PositiveInt],
      signingKeys: Seq[Fingerprint],
      protocolVersion: ProtocolVersion,
      expectFullAuthorization: Boolean,
      force: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, GenericSignedTopologyTransactionX] = {
    logger.debug(show"Attempting to build, sign, and ${op} ${mapping} with serial $serial")
    for {
      tx <- build(op, mapping, serial, protocolVersion, signingKeys).mapK(
        FutureUnlessShutdown.outcomeK
      )
      signedTx <- signTransaction(tx, signingKeys, isProposal = !expectFullAuthorization)
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- add(Seq(signedTx), force = force, expectFullAuthorization)
    } yield signedTx
  }

  /** Authorizes an existing topology transaction by signing it and adding it to the topology state.
    * If {@code expectFullAuthorization} is {@code true} and the topology transaction cannot be fully
    * authorized with keys from this node, returns with an error and the existing topology transaction
    * remains unchanged.
    *
    * @param transactionHash the uniquely identifying hash of a previously proposed topology transaction
    * @param signingKeys the key which should be used to sign
    * @param force force dangerous operations, such as removing the last signing key of a participant
    * @param expectFullAuthorization whether the resulting transaction must be fully authorized or not
    * @return the signed transaction or an error code of why the addition failed
    */
  def accept(
      transactionHash: TxHash,
      signingKeys: Seq[Fingerprint],
      force: Boolean,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, GenericSignedTopologyTransactionX] = {
    // TODO(#11255): check that there is an existing topology transaction with the hash of the unique key
    for {
      proposals <- EitherT
        .right[TopologyManagerError](
          store.findProposalsByTxHash(EffectiveTime(clock.now), NonEmpty(Set, transactionHash))
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      existingTransaction =
        (proposals match {
          case Seq() => ??? // TODO(#11255) proper error
          case Seq(tx) => tx
          case _otherwise => ??? // TODO(#11255) proper error
        })
      extendedTransaction <- extendSignature(existingTransaction, signingKeys).mapK(
        FutureUnlessShutdown.outcomeK
      )
      _ <- add(
        Seq(extendedTransaction),
        force = force,
        expectFullAuthorization = expectFullAuthorization,
      )
    } yield {
      extendedTransaction
    }
  }

  def build[Op <: TopologyChangeOpX, M <: TopologyMappingX](
      op: Op,
      mapping: M,
      serial: Option[PositiveInt],
      protocolVersion: ProtocolVersion,
      newSigningKeys: Seq[Fingerprint],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyManagerError, TopologyTransactionX[Op, M]] = {
    for {
      existingTransactions <- EitherT.right(
        store.findTransactionsForMapping(EffectiveTime.MaxValue, NonEmpty(Set, mapping.uniqueKey))
      )
      _ = if (existingTransactions.sizeCompare(1) > 0)
        logger.warn(
          s"found more than one valid mapping for unique key ${mapping.uniqueKey} of type ${mapping.code}"
        )
      existingTransaction = existingTransactions
        .sortBy(_.transaction.serial)
        .lastOption
        .map(t => (t.transaction.op, t.transaction.mapping, t.transaction.serial, t.signatures))

      // If the same operation and mapping is proposed repeatedly, insist that
      // new keys are being added. Otherwise reject consistently with daml 2.x-based topology management.
      _ <- existingTransaction match {
        case Some((`op`, `mapping`, _, existingSignatures)) if op == TopologyChangeOpX.Replace =>
          EitherT.cond[Future][TopologyManagerError, Unit](
            (newSigningKeys.toSet -- existingSignatures.map(_.signedBy).toSet).nonEmpty,
            (),
            TopologyManagerError.MappingAlreadyExists
              .FailureX(mapping, existingSignatures.map(_.signedBy)),
          )
        case _ => EitherT.rightT[Future, TopologyManagerError](())
      }

      theSerial <- ((existingTransaction, serial) match {
        case (None, None) =>
          // auto-select 1
          EitherT.rightT(PositiveInt.one)
        case (None, Some(proposed)) =>
          // didn't find an existing transaction, therefore the proposed serial must be 1
          EitherT.cond[Future][TopologyManagerError, PositiveInt](
            proposed == PositiveInt.one,
            PositiveInt.one,
            TopologyManagerError.SerialMismatch.Failure(PositiveInt.one, proposed),
          )

        // TODO(#11255) existing mapping and the proposed mapping are the same. does this only add a (superfluous) signature?
        //              maybe we should reject this proposal, but for now we need this to pass through successfully, because we don't
        //              support proper topology transaction validation yet, especially not for multi-sig transactions.
        case (Some((`op`, `mapping`, existingSerial, _)), None) =>
          // auto-select existing
          EitherT.rightT(existingSerial)
        case (Some((`op`, `mapping`, existingSerial, _)), Some(proposed)) =>
          EitherT.cond[Future](
            existingSerial == proposed,
            existingSerial,
            TopologyManagerError.SerialMismatch.Failure(existingSerial, proposed),
          )

        case (Some((_, _, existingSerial, _)), None) =>
          // auto-select existing+1
          EitherT.rightT(existingSerial + PositiveInt.one)
        case (Some((_, _, existingSerial, _)), Some(proposed)) =>
          // check that the proposed serial matches existing+1
          val next = existingSerial + PositiveInt.one
          EitherT.cond[Future](
            next == proposed,
            next,
            TopologyManagerError.SerialMismatch.Failure(next, proposed),
          )
      }): EitherT[Future, TopologyManagerError, PositiveInt]
    } yield TopologyTransactionX(op, theSerial, mapping, protocolVersion)
  }

  def signTransaction[Op <: TopologyChangeOpX, M <: TopologyMappingX](
      transaction: TopologyTransactionX[Op, M],
      signingKeys: Seq[Fingerprint],
      isProposal: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyManagerError, SignedTopologyTransactionX[Op, M]] = {
    for {
      // find signing keys.
      keys <- (signingKeys match {
        case first +: rest =>
          // TODO(#11255) should we check whether this node could sign with keys that are required in addition to the ones provided in signingKeys, and fetch those keys?
          EitherT.pure(NonEmpty.mk(Set, first, rest: _*))
        case _empty =>
          // TODO(#11255) get signing keys for transaction.
          EitherT.leftT(
            TopologyManagerError.InternalError.ImplementMe(
              "Automatic signing key lookup not yet implemented. Please specify a signing explicitly."
            )
          )
      }): EitherT[Future, TopologyManagerError, NonEmpty[Set[Fingerprint]]]
      // create signed transaction
      signed <- SignedTopologyTransactionX
        .create(
          transaction,
          keys,
          isProposal,
          crypto.pureCrypto,
          crypto.privateCrypto,
          // TODO(#11255) The `SignedTopologyTransactionX` may use a different versioning scheme than the contained transaction. Use the right protocol version here
          transaction.representativeProtocolVersion.representative,
        )
        .leftMap {
          case SigningError.UnknownSigningKey(keyId) =>
            TopologyManagerError.SecretKeyNotInStore.Failure(keyId)
          case err => TopologyManagerError.InternalError.TopologySigningError(err)
        }: EitherT[Future, TopologyManagerError, SignedTopologyTransactionX[Op, M]]
    } yield signed
  }

  def extendSignature[Op <: TopologyChangeOpX, M <: TopologyMappingX](
      transaction: SignedTopologyTransactionX[Op, M],
      signingKey: Seq[Fingerprint],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyManagerError, SignedTopologyTransactionX[Op, M]] = {
    for {
      // find signing keys
      keys <- (signingKey match {
        case keys @ (_first +: _rest) =>
          // TODO(#11255) filter signing keys relevant for the required authorization for this transaction
          EitherT.rightT(keys.toSet)
        case _ =>
          // TODO(#11255) fetch signing keys that are relevant for the required authorization for this transaction
          EitherT.leftT(
            TopologyManagerError.InternalError.ImplementMe(
              "Automatic signing key lookup not yet implemented. Please specify a signing explicitly."
            )
          )
      }): EitherT[Future, TopologyManagerError, Set[Fingerprint]]
      signatures <- keys.toSeq.parTraverse(
        crypto.privateCrypto
          .sign(transaction.transaction.hash.hash, _)
          .leftMap(err =>
            TopologyManagerError.InternalError.TopologySigningError(err): TopologyManagerError
          )
      )
    } yield transaction.addSignatures(signatures)
  }

  /** sequential(!) adding of topology transactions
    *
    * @param force force a dangerous change (such as revoking the last key)
    */
  def add(
      transactions: Seq[GenericSignedTopologyTransactionX],
      force: Boolean,
      expectFullAuthorization: Boolean,
      abortOnError: Boolean = true,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
    sequentialQueue.executeE(
      {
        val ts = clock.uniqueTime()
        for {
          // validate incrementally and apply to in-memory state
          _ <- processor
            .validateAndApplyAuthorization(
              SequencedTime(ts),
              EffectiveTime(ts),
              transactions,
              abortIfCascading = !force,
              abortOnError = abortOnError,
              expectFullAuthorization,
            )
            .leftMap { res =>
              res.flatMap(_.rejectionReason).headOption match {
                case Some(rejection) => rejection.toTopologyManagerError
                case None =>
                  TopologyManagerError.InternalError
                    .Other("Topology transaction validation failed but there are no rejections")
              }
            }
          _ <- EitherT.right(notifyObservers(ts, transactions))
        } yield ()
      },
      "add-topology-transaction",
    )

  /** notify observers about new transactions about to be stored */
  protected def notifyObservers(
      timestamp: CantonTimestamp,
      transactions: Seq[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): Future[Unit] = Future
    .sequence(
      observers
        .get()
        .map(_.addedNewTransactions(timestamp, transactions).onShutdown(()))
    )
    .map(_ => ())

  override protected def onClosed(): Unit = Lifecycle.close(store, sequentialQueue)(logger)

}
