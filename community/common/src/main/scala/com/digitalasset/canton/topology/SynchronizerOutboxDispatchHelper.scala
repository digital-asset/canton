// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.common.sequencer.RegisterTopologyTransactionHandle
import com.digitalasset.canton.config.TopologyConfig
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
  RunOnClosing,
}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyTransaction}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.AllExceptionRetryPolicy
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, MonadUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

trait SynchronizerOutboxDispatchHelper extends NamedLogging {
  protected def psid: PhysicalSynchronizerId

  protected def memberId: Member

  final protected def protocolVersion: ProtocolVersion = psid.protocolVersion

  protected def crypto: SynchronizerCrypto

  protected def topologyConfig: TopologyConfig

  protected def topologyTransaction(
      tx: GenericSignedTopologyTransaction
  ): PrettyPrinting = tx.transaction

  protected def onlyApplicable(
      transactions: Seq[GenericSignedTopologyTransaction]
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] =
    FutureUnlessShutdown.pure(
      transactions.filter(x => x.mapping.restrictedToSynchronizer.forall(_ == psid.logical))
    )

  /** Re-signs the given transactions for the synchronizer's protocol version if they are not
    * already in it, using the keys that authorized the original transaction. Fails if any such key
    * is unavailable on this node.
    */
  protected def convertTransactions(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Seq[GenericSignedTopologyTransaction]] =
    MonadUtil.parTraverseWithLimit(topologyConfig.broadcastBatchSize)(transactions)(
      convertTransaction
    )

  private def convertTransaction(
      tx: GenericSignedTopologyTransaction
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, GenericSignedTopologyTransaction] =
    if (tx.transaction.isEquivalentTo(protocolVersion)) {
      EitherT.rightT(tx)
    } else {
      val authorizingKeys = tx.signatures.map(_.authorizingLongTermKey)
      for {
        _ <- MonadUtil.sequentialTraverse_(authorizingKeys.forgetNE.toSeq)(fingerprint =>
          crypto.cryptoPrivateStore
            .existsSigningKey(fingerprint)
            .leftMap(err => s"Failed to check availability of signing key $fingerprint: $err")
            .subflatMap(exists =>
              Either.cond(
                exists,
                (),
                s"Signing key $fingerprint for topology transaction ${tx.mapping} is not available " +
                  s"on this node to re-sign it for protocol version $protocolVersion",
              )
            )
        )
        converted = TopologyTransaction(
          tx.transaction.operation,
          tx.transaction.serial,
          tx.transaction.mapping,
          protocolVersion,
        )
        resigned <- SignedTopologyTransaction
          .signAndCreate(
            converted,
            authorizingKeys,
            tx.isProposal,
            crypto.privateCrypto,
            protocolVersion,
          )
          .leftMap(err =>
            s"Failed to re-sign topology transaction ${tx.mapping} for protocol version " +
              s"$protocolVersion: $err"
          )
      } yield resigned
    }

  protected def isFailedState(response: TopologyTransactionsBroadcast.State): Boolean =
    response == TopologyTransactionsBroadcast.State.Failed

  def isExpectedState(state: TopologyTransactionsBroadcast.State): Boolean = state match {
    case TopologyTransactionsBroadcast.State.Failed => false
    case TopologyTransactionsBroadcast.State.Accepted => true
  }
}

trait StoreBasedSynchronizerOutboxDispatchHelper extends SynchronizerOutboxDispatchHelper {

  def authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore]
}

trait SynchronizerOutboxDispatch extends NamedLogging with FlagCloseable {
  this: SynchronizerOutboxDispatchHelper =>

  protected def targetStore: TopologyStore[TopologyStoreId.SynchronizerStore]
  protected def handle: RegisterTopologyTransactionHandle

  // register handle close task
  // this will ensure that the handle is closed before the outbox, aborting any retries
  runOnOrAfterClose_(new RunOnClosing {
    override def name: String = "close-handle"
    override def done: Boolean = handle.isClosing
    override def run()(implicit traceContext: TraceContext): Unit = LifeCycle.close(handle)(logger)
  })(TraceContext.empty)

  protected def notAlreadyPresent(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] =
    targetStore.filterProvidesAdditionalSignatures(transactions)

  protected def dispatch(
      synchronizerAlias: SynchronizerAlias,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Seq[TopologyTransactionsBroadcast.State]] =
    if (transactions.isEmpty) EitherT.rightT(Seq.empty)
    else {
      implicit val success = retry.Success.always
      val ret = retry
        .Backoff(
          logger,
          this,
          topologyConfig.topologyTransactionObservationTimeout.retries(1.second),
          1.second,
          10.seconds,
          "push topology transaction",
        )
        .unlessShutdown(
          {
            logger.debug(
              s"Attempting to push ${transactions.size} topology transactions to $synchronizerAlias: ${transactions
                  .map(_.hash)}"
            )
            FutureUnlessShutdownUtil.logOnFailureUnlessShutdown(
              handle.submit(transactions),
              s"Pushing topology transactions to $synchronizerAlias",
            )
          },
          AllExceptionRetryPolicy,
        )
        .map { responses =>
          if (responses.sizeCompare(transactions) != 0) {
            logger.error(
              s"Topology request contained ${transactions.length} txs, but I received responses for ${responses.length}"
            )
          }
          logger.debug(
            s"$synchronizerAlias responded the following for the given topology transactions: $responses"
          )
          val failedResponses =
            responses.zip(transactions).collect {
              case (response, tx) if isFailedState(response) => tx
            }

          Either.cond(
            failedResponses.isEmpty,
            responses,
            s"The synchronizer $synchronizerAlias failed the following topology transactions: $failedResponses",
          )
        }
      EitherT(
        ret
      )
    }
}
