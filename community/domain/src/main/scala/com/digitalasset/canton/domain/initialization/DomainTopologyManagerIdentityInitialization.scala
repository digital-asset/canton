// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.initialization

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.InitConfigBase
import com.digitalasset.canton.config.RequireTypes.InstanceName
import com.digitalasset.canton.crypto.{SigningPublicKey, X509Certificate}
import com.digitalasset.canton.domain.config.DomainNodeParameters
import com.digitalasset.canton.domain.topology.DomainTopologyManager
import com.digitalasset.canton.environment.CantonNodeBootstrapBase
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.{
  DomainParametersChange,
  NamespaceDelegation,
  OwnerToKeyMapping,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.Future

trait DomainTopologyManagerIdentityInitialization {
  self: CantonNodeBootstrapBase[_, _, DomainNodeParameters] =>

  def initializeTopologyManagerIdentity(
      name: InstanceName,
      legalIdentityHook: X509Certificate => EitherT[Future, String, Unit],
      initialDynamicDomainParameters: DynamicDomainParameters,
      protocolVersion: ProtocolVersion,
      initConfigBase: InitConfigBase,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, (NodeId, DomainTopologyManager, SigningPublicKey)] = {
    // initialize domain with local keys
    val idName =
      initConfigBase.identity.flatMap(_.nodeIdentifier.identifierName).getOrElse(name.unwrap)
    for {
      id <- Identifier.create(idName).toEitherT[Future]
      // first, we create the namespace key for this node
      namespaceKey <- getOrCreateSigningKey(s"$name-namespace")
      // then, we create the topology manager signing key
      topologyManagerSigningKey <- getOrCreateSigningKey(s"$name-topology-manager-signing")
      // using the namespace key and the identifier, we create the uid of this node
      uid = UniqueIdentifier(id, Namespace(namespaceKey.fingerprint))
      nodeId = NodeId(uid)
      // now, we kick off the topology manager services so we can start building topology transactions
      topologyManager <- initializeIdentityManagerAndServices(nodeId).toEitherT[Future]
      // first, we issue the root namespace delegation for our namespace
      _ <- authorizeStateUpdate(
        topologyManager,
        namespaceKey,
        NamespaceDelegation(
          Namespace(namespaceKey.fingerprint),
          namespaceKey,
          isRootDelegation = true,
        ),
        protocolVersion,
      )
      // then, we initialise the domain parameters
      _ <- authorizeDomainGovernance(
        topologyManager,
        namespaceKey,
        DomainParametersChange(DomainId(uid), initialDynamicDomainParameters),
        protocolVersion,
      )

      // now, we assign the topology manager key with the domain topology manager
      domainTopologyManagerId = DomainTopologyManagerId(uid)
      _ <- authorizeStateUpdate(
        topologyManager,
        namespaceKey,
        OwnerToKeyMapping(domainTopologyManagerId, topologyManagerSigningKey),
        protocolVersion,
      )

      // Setup the legal identity of the domain nodes
      _ <-
        if (initConfig.identity.exists(_.generateLegalIdentityCertificate)) {
          (new LegalIdentityInit(certificateGenerator, crypto))
            .getOrGenerateCertificate(
              uid,
              Seq(MediatorId(uid), domainTopologyManagerId),
            )
            .flatMap(legalIdentityHook)
        } else {
          EitherT.rightT[Future, String](())
        }

    } yield (nodeId, topologyManager, namespaceKey)
  }

  protected def initializeIdentityManagerAndServices(
      nodeId: NodeId
  ): Either[String, DomainTopologyManager]

}
