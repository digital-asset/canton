// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, FlagCloseableAsync, SyncCloseable}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.SimpleExecutionQueue
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class TopologyManagerX(
    val clock: Clock,
    val crypto: Crypto,
    protected val store: TopologyStoreX[TopologyStoreId],
    val timeouts: ProcessingTimeout,
    protocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync {

  // sequential queue to run all the processing that does operate on the state
  protected val sequentialQueue = new SimpleExecutionQueue()
  private val processor = new TopologyStateProcessorX(store, loggerFactory)

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
  ): EitherT[Future, TopologyManagerError, GenericSignedTopologyTransactionX] = {
    logger.debug(show"Attempting to build, sign, and ${op} ${mapping}")
    for {
      tx <- build(op, mapping, serial = serial, protocolVersion)
      signedTx <- signTransaction(tx, signingKeys, isProposal = !expectFullAuthorization)
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
  ): EitherT[Future, TopologyManagerError, GenericSignedTopologyTransactionX] = {
    // TODO(#11255): check that there is an existing topology transaction with the hash of the unique key
    for {
      proposals <- EitherT.right[TopologyManagerError](
        store.findProposalsByTxHash(EffectiveTime(clock.now), Seq(transactionHash))
      )
      existingTransaction =
        (proposals match {
          case Seq() => ??? // TODO(#11255) proper error
          case Seq(tx) => tx
          case _otherwise => ??? // TODO(#11255) proper error
        })
      extendedTransaction <- extendSignature(existingTransaction, signingKeys)
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
  ): EitherT[Future, TopologyManagerError, TopologyTransactionX[Op, M]] = {
    for {
      // find serial number
      theSerial <- (serial match {
        case Some(value) =>
          // TODO(#11255) check for the latest topology transaction for the uniqueness key: serial = previousSerial + 1
          EitherT.rightT(value)
        case None =>
          // TODO(#11255) get next serial for mapping
          EitherT.rightT(PositiveInt.one)
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
          EitherT.leftT(TopologyManagerError.InternalError.ImplementMe())
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
          EitherT.leftT(TopologyManagerError.InternalError.ImplementMe())
      }): EitherT[Future, TopologyManagerError, Set[Fingerprint]]
      signatures <- keys.toSeq.parTraverse(
        crypto.privateCrypto
          .sign(transaction.transaction.hash.hash, _)
          .leftMap(err => TopologyManagerError.InternalError.ImplementMe(): TopologyManagerError)
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
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Unit] =
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
              // TODO(#11255) fix error handling
              TopologyManagerError.InternalError
                .Other(res.flatMap(_.rejectionReason).map(_.asString).mkString("[", ", ", "]"))
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
  ): Future[Unit] = Future.unit

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    Seq(
      SyncCloseable("topology-manager-store", store.close()),
      sequentialQueue.asCloseable(
        "topology-manager-sequential-queue",
        timeouts.shutdownProcessing.unwrap,
      ),
    )
  }

}
