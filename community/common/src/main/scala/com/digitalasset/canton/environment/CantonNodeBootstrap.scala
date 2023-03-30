// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import akka.actor.ActorSystem
import cats.data.EitherT
import cats.syntax.functorFilter.*
import com.digitalasset.canton
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.LocalNodeConfig
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.{
  GrpcInitializationService,
  GrpcTopologyAggregationService,
  GrpcTopologyManagerReadService,
  GrpcTopologyManagerWriteService,
}
import com.digitalasset.canton.topology.admin.v0.{
  InitializationServiceGrpc,
  TopologyManagerWriteServiceGrpc,
}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ServerServiceDefinition

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.Future

/** Bootstrapping class used to drive the initialization of a canton node (domain and participant)
  *
  * (wait for unique id) -> receive initId ->  notify actual implementation via idInitialized
  */
abstract class CantonNodeBootstrapBase[
    T <: CantonNode,
    NodeConfig <: LocalNodeConfig,
    ParameterConfig <: CantonNodeParameters,
    Metrics <: BaseMetrics,
](
    arguments: CantonNodeBootstrapCommonArguments[NodeConfig, ParameterConfig, Metrics]
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
) extends CantonNodeBootstrapCommon[T, NodeConfig, ParameterConfig, Metrics](arguments) {

  protected val authorizedTopologyStore =
    TopologyStore(TopologyStoreId.AuthorizedStore, storage, timeouts, loggerFactory)
  this.adminServerRegistry
    .addService(
      canton.topology.admin.v0.TopologyManagerReadServiceGrpc
        .bindService(
          new GrpcTopologyManagerReadService(
            sequencedTopologyStores,
            ips,
            crypto,
            loggerFactory,
          ),
          executionContext,
        )
    )
    .discard

  this.adminServerRegistry
    .addService(
      InitializationServiceGrpc
        .bindService(
          new GrpcInitializationService(clock, this, crypto.cryptoPublicStore),
          executionContext,
        )
    )
    ._2
    .addService(
      canton.topology.admin.v0.TopologyAggregationServiceGrpc
        .bindService(
          new GrpcTopologyAggregationService(
            // TODO(#11255) remove map filter
            sequencedTopologyStores.mapFilter(TopologyStoreId.select[TopologyStoreId.DomainStore]),
            ips,
            loggerFactory,
          ),
          executionContext,
        )
    )
    ._2
    .addService(
      canton.topology.admin.v0.TopologyManagerReadServiceGrpc
        .bindService(
          new GrpcTopologyManagerReadService(
            sequencedTopologyStores :+ authorizedTopologyStore,
            ips,
            crypto,
            loggerFactory,
          ),
          executionContext,
        )
    )
    .discard

  /** All existing domain stores */
  protected def sequencedTopologyStores: Seq[TopologyStore[TopologyStoreId]]

  /** Initialize the node with an externally provided identity. */
  override def initializeWithProvidedId(nodeId: NodeId): EitherT[Future, String, Unit] = {
    for {
      _ <- storeId(nodeId)
      _ <- initialize(nodeId)
    } yield ()
  }

  protected def startTopologyManagementWriteService[E <: CantonError](
      topologyManager: TopologyManager[E],
      authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
  ): Unit = {
    adminServerRegistry
      .addServiceU(
        topologyManagerWriteService(topologyManager, authorizedStore)
      )
  }

  protected def topologyManagerWriteService[E <: CantonError](
      topologyManager: TopologyManager[E],
      authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
  ): ServerServiceDefinition = {
    TopologyManagerWriteServiceGrpc.bindService(
      new GrpcTopologyManagerWriteService(
        topologyManager,
        authorizedStore,
        crypto.cryptoPublicStore,
        parameterConfig.initialProtocolVersion,
        loggerFactory,
      ),
      executionContext,
    )

  }

  // utility functions used by automatic initialization of domain and participant
  protected def authorizeStateUpdate[E <: CantonError](
      manager: TopologyManager[E],
      key: SigningPublicKey,
      mapping: TopologyStateUpdateMapping,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    authorizeIfNew(
      manager,
      TopologyStateUpdate.createAdd(mapping, protocolVersion),
      key,
      protocolVersion,
    )

  protected def authorizeIfNew[E <: CantonError, Op <: TopologyChangeOp](
      manager: TopologyManager[E],
      transaction: TopologyTransaction[Op],
      signingKey: SigningPublicKey,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] = for {
    exists <- EitherT.right(
      manager.signedMappingAlreadyExists(transaction.element.mapping, signingKey.fingerprint)
    )
    res <-
      if (exists) {
        logger.debug(s"Skipping existing ${transaction.element.mapping}")
        EitherT.rightT[Future, String](())
      } else
        manager
          .authorize(transaction, Some(signingKey.fingerprint), protocolVersion, false)
          .leftMap(_.toString)
          .map(_ => ())
  } yield res

  override protected def onClosed(): Unit = {
    Lifecycle.close(authorizedTopologyStore)(logger)
    super.onClosed()
  }

}
