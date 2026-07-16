// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import com.digitalasset.canton.config.TopologyConfig
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyTransaction}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

object TopologySigningHelper {

  /** Re-signs the given transactions for the synchronizer's protocol version if they are not
    * already in it, using the keys that authorized the original transaction. Fails if any such key
    * is unavailable on this node.
    */
  def convertTransactions(
      transactions: Seq[GenericSignedTopologyTransaction],
      protocolVersion: ProtocolVersion,
      crypto: SynchronizerCrypto,
      topologyConfig: TopologyConfig,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Seq[GenericSignedTopologyTransaction]] =
    MonadUtil.parTraverseWithLimit(topologyConfig.broadcastBatchSize)(transactions)(
      convertTransaction(_, protocolVersion, crypto)
    )

  private def convertTransaction(
      tx: GenericSignedTopologyTransaction,
      protocolVersion: ProtocolVersion,
      crypto: SynchronizerCrypto,
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
}
