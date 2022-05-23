// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.processing.TransactionAuthorizationValidator.AuthorizationChain
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.transaction.{
  RequiredAuth,
  SignedTopologyTransaction,
  TopologyChangeOp,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SimpleExecutionQueue
import io.functionmeta.functionFullName

import scala.concurrent.{ExecutionContext, Future}
import com.digitalasset.canton.topology.store.TopologyStoreId

/** Compute the authorization chain for a certain UID */
class SnapshotAuthorizationValidator(
    asOf: CantonTimestamp,
    val store: TopologyStore[TopologyStoreId],
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends TransactionAuthorizationValidator
    with NamedLogging {

  private val sequential = new SimpleExecutionQueue()

  def authorizedBy(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit
      traceContext: TraceContext
  ): Future[Option[AuthorizationChain]] = {
    // preload our cache. note, we don't want to load stuff into our cache concurrently, so we
    // squeeze this through a sequential execution queue
    val preloadF = transaction.transaction.element.mapping.requiredAuth match {
      case RequiredAuth.Ns(namespace, _) =>
        sequential.execute(
          loadAuthorizationGraphs(asOf, Set(namespace)),
          functionFullName,
        )
      case RequiredAuth.Uid(uids) =>
        sequential.execute(
          {
            val graphF = loadAuthorizationGraphs(asOf, uids.map(_.namespace).toSet)
            val delF = loadIdentifierDelegations(asOf, Seq.empty, uids.toSet)
            graphF.zip(delF)
          },
          functionFullName,
        )
    }
    preloadF.map { _ =>
      authorizationChainFor(transaction)
    }
  }

}
