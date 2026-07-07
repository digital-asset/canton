// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.syntax.bifunctor.*
import cats.syntax.traverse.*
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.admin.grpc
import com.digitalasset.canton.topology.store.TopologyStore

/** Common functionality used by various topology related grpc services.
  */
object GrpcTopologyServiceUtil {

  /** Returns topology stores that are active and fully initialized for the requested store ids. If
    */
  def collectActiveStores[InternalStore <: topology.store.TopologyStoreId](
      requestedStores: Seq[grpc.TopologyStoreId],
      allStores: Seq[
        TopologyStoreInitializationStatus[InternalStore, TopologyStore]
      ],
      psidLookup: PsidLookup,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[RpcError, Seq[TopologyStore[InternalStore]]] = {
    val filteredStoreIds =
      if (requestedStores.isEmpty) {
        // if no store was requested explicitly, use all local stores and all active synchronizer stores
        val activeStores = extractLocalAndLogicalStoreIds(
          allStores.map(s => grpc.TopologyStoreId.fromInternal(s.storeId))
        ).flatMap(
          // if no specific store was requested, don't report errors from the active psid resolution,
          // but instead just return what is available.
          resolveLogicalSynchronizerId(_, psidLookup).toSeq
        ).toSeq
        Right(activeStores)
      } else {
        // if specific synchronizers were requested, report an error if it cannot be resolved
        requestedStores.distinct.traverse(resolveLogicalSynchronizerId(_, psidLookup))
      }

    filteredStoreIds.flatMap(
      _.traverse(requestedStoreId =>
        allStores
          .find(_.storeId == requestedStoreId)
          .toRight(TopologyManagerError.TopologyStoreUnknown.Failure(requestedStoreId))
          .flatMap(_.toEither)
          .leftWiden[RpcError]
      )
    )
  }

  /** Turns physical synchronizer store ids into logical synchronizer store ids, but doesn't change
    * other types of store ids.
    * @return
    *   a set of store ids that do not contain physical synchronizer store ids
    */
  private def extractLocalAndLogicalStoreIds(
      requested: Seq[grpc.TopologyStoreId]
  ): Set[TopologyStoreId] =
    requested.view.map {
      case grpc.TopologyStoreId.Synchronizer(Right(psid)) =>
        grpc.TopologyStoreId.Synchronizer(psid.logical)
      case otherwise => otherwise
    }.toSet

  /** Converts the admin api store ids into internal store ids, by resolving logical synchronizer
    * store ids to the physical store id that is currently active. If no physical store is currently
    * marked as active, returns an error.
    */
  private def resolveLogicalSynchronizerId(
      grpcTopologyStoreId: grpc.TopologyStoreId,
      psidLookup: PsidLookup,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[RpcError, topology.store.TopologyStoreId] =
    grpcTopologyStoreId
      .toInternal(psidLookup)
      .leftMap(TopologyManagerError.TopologyStoreUnknown.NoActiveSynchronizer(_))

}
