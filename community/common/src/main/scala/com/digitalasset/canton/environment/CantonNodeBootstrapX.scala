// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import akka.actor.ActorSystem
import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.{InitConfigBase, LocalNodeConfig}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.{
  GrpcIdentityInitializationServiceX,
  GrpcTopologyManagerReadServiceX,
  GrpcTopologyManagerWriteServiceX,
}
import com.digitalasset.canton.topology.admin.v1 as topologyProto
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.Future

/** CantonNodeBootstrapX trait insists that nodes have their own topology manager
  * and that they have the ability to auto-initialize their identity on their own.
  */
abstract class CantonNodeBootstrapX[
    T <: CantonNode,
    NodeConfig <: LocalNodeConfig,
    ParameterConfig <: CantonNodeParameters,
    Metrics <: BaseMetrics,
](
    arguments: CantonNodeBootstrapCommonArguments[
      NodeConfig,
      ParameterConfig,
      Metrics,
    ]
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
) extends CantonNodeBootstrapCommon[T, NodeConfig, ParameterConfig, Metrics](arguments) {

  protected val authorizedTopologyStore: TopologyStoreX[TopologyStoreId.AuthorizedStore] =
    TopologyStoreX(TopologyStoreId.AuthorizedStore, storage, timeouts, loggerFactory)

  protected def sequencedTopologyStores: Seq[TopologyStoreX[TopologyStoreId]]

  /** All nodes have their own topology manager */
  protected val topologyManager: TopologyManagerX =
    new TopologyManagerX(
      clock,
      crypto,
      authorizedTopologyStore,
      timeouts,
      parameterConfig.initialProtocolVersion,
      loggerFactory,
    )

  adminServerRegistry
    .addServiceU(
      topologyProto.TopologyManagerReadServiceXGrpc
        .bindService(
          new GrpcTopologyManagerReadServiceX(
            sequencedTopologyStores :+ authorizedTopologyStore,
            ips,
            crypto,
            loggerFactory,
          ),
          executionContext,
        )
    )
  adminServerRegistry
    .addServiceU(
      topologyProto.TopologyManagerWriteServiceXGrpc
        .bindService(
          new GrpcTopologyManagerWriteServiceX(
            topologyManager,
            authorizedTopologyStore,
            this,
            crypto,
            parameterConfig.initialProtocolVersion,
            clock,
            loggerFactory,
          ),
          executionContext,
        )
    )
  adminServerRegistry
    .addServiceU(
      topologyProto.IdentityInitializationServiceXGrpc
        .bindService(
          new GrpcIdentityInitializationServiceX(
            clock,
            this,
            crypto.cryptoPublicStore,
          ),
          executionContext,
        )
    )

  /** member depends on node type */
  protected def member(uid: UniqueIdentifier): Member

  /** Initialize the node with an externally provided identity. */
  override def initializeWithProvidedId(nodeId: NodeId): EitherT[Future, String, Unit] = {
    // TODO(#11255) test that we have all certificates and keys (should be equivalent to the state that we get from autoInitIdentity
    for {
      _ <- storeId(nodeId)
      _ <- initialize(nodeId)
    } yield ()
  }

  /** Generate an identity for the node. */
  override protected def autoInitializeIdentity(
      initConfigBase: InitConfigBase
  ): EitherT[Future, String, Unit] =
    withNewTraceContext { implicit traceContext =>
      val protocolVersion = parameterConfig.initialProtocolVersion
      for {
        // create keys
        namespaceKey <- getOrCreateSigningKey(s"$name-namespace")

        // create id
        identifierName = initConfigBase.identity
          .flatMap(_.nodeIdentifier.identifierName)
          .getOrElse(name.unwrap)
        identifier <- EitherT
          .fromEither[Future](Identifier.create(identifierName))
          .leftMap(err => s"Failed to convert name to identifier: $err")
        uid = UniqueIdentifier(
          identifier,
          Namespace(namespaceKey.fingerprint),
        )
        nodeId = NodeId(uid)

        // init topology manager
        nsd <- EitherT.fromEither[Future](
          NamespaceDelegationX.create(
            Namespace(namespaceKey.fingerprint),
            namespaceKey,
            isRootDelegation = true,
          )
        )
        _ <- authorizeStateUpdate(namespaceKey, nsd, protocolVersion)
        // all nodes need a signing key
        signingKey <- getOrCreateSigningKey(s"$name-signing")
        // key owner id depends on the type of node
        ownerId = member(nodeId.identity)
        // participants need also an encryption key
        keys <-
          if (ownerId.code == ParticipantId.Code) {
            for {
              encryptionKey <- getOrCreateEncryptionKey(s"$name-encryption")
            } yield NonEmpty.mk(Seq, signingKey, encryptionKey)
          } else {
            EitherT.rightT[Future, String](NonEmpty.mk(Seq, signingKey))
          }

        // register the keys
        _ <- authorizeStateUpdate(
          namespaceKey,
          OwnerToKeyMappingX(ownerId, None)(keys),
          protocolVersion,
        )

        // finally, we store the node id, which means that the node will not be auto-initialised next time when we start
        _ <- storeId(nodeId)
        // TODO(#11255) now, we should be ready to be initialised with a specific domain
      } yield ()
    }

  protected def authorizeStateUpdate(
      key: SigningPublicKey,
      mapping: TopologyMappingX,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] = {
    topologyManager
      .proposeAndAuthorize(
        TopologyChangeOpX.Replace,
        mapping,
        serial = None,
        Seq(key.fingerprint),
        protocolVersion,
        expectFullAuthorization = true,
      )
      // TODO(#11255) error handling
      .leftMap(_.toString)
      .map(_ => ())
  }

}
