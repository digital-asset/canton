// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.daml.metrics.Timed
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.participant.store.{ContractStore, PersistedContractInstance}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

// this is a wrapper trait around ContractStore to be used from Ledger API layer and handle exceptions and shutdowns uniformly
trait LedgerApiContractStore {

  def lookupPersisted(id: LfContractId)(implicit
      traceContext: TraceContext
  ): Future[Option[PersistedContractInstance]]

  def lookupBatchedNonReadThrough(internalContractIds: Iterable[Long])(implicit
      traceContext: TraceContext
  ): Future[Map[Long, PersistedContractInstance]]

  def lookupBatchedInternalIdsNonReadThrough(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, Long]]

  def lookupBatchedContractIdsNonReadThrough(internalContractIds: Iterable[Long])(implicit
      traceContext: TraceContext
  ): Future[Map[Long, LfContractId]]
}

final case class LedgerApiContractStoreImpl(
    participantContractStore: ContractStore,
    loggerFactory: NamedLoggerFactory,
    metrics: LedgerApiServerMetrics,
)(implicit ec: ExecutionContext)
    extends LedgerApiContractStore
    with NamedLogging {

  def lookupPersisted(id: LfContractId)(implicit
      traceContext: TraceContext
  ): Future[Option[PersistedContractInstance]] =
    Timed
      .future(
        metrics.contractStore.lookupPersisted,
        failOnShutdown(
          participantContractStore
            .lookupPersisted(id)
        ),
      )

  def lookupBatchedNonReadThrough(internalContractIds: Iterable[Long])(implicit
      traceContext: TraceContext
  ): Future[Map[Long, PersistedContractInstance]] =
    Timed.future(
      metrics.contractStore.lookupBatched,
      failOnShutdown(
        participantContractStore
          .lookupBatchedNonReadThrough(internalContractIds)
      ),
    )

  def lookupBatchedInternalIdsNonReadThrough(contractIds: Iterable[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, Long]] =
    Timed.future(
      metrics.contractStore.lookupBatchedInternalIds,
      failOnShutdown(
        participantContractStore
          .lookupBatchedInternalIdsNonReadThrough(contractIds)
      ),
    )

  def lookupBatchedContractIdsNonReadThrough(internalContractIds: Iterable[Long])(implicit
      traceContext: TraceContext
  ): Future[Map[Long, LfContractId]] =
    Timed
      .future(
        metrics.contractStore.lookupBatchedContractIds,
        failOnShutdown(
          participantContractStore
            .lookupBatchedContractIdsNonReadThrough(internalContractIds)
        ),
      )

  private def failOnShutdown[T](f: FutureUnlessShutdown[T])(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[T] =
    f.failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)

}
