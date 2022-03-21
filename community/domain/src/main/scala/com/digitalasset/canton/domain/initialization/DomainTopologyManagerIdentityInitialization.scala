// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.initialization

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.config.RequireTypes.InstanceName
import com.digitalasset.canton.crypto.{KeyName, PublicKey, X509Certificate}
import com.digitalasset.canton.domain.config.DomainNodeParameters
import com.digitalasset.canton.domain.topology.DomainTopologyManager
import com.digitalasset.canton.environment.CantonNodeBootstrapBase
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.topology.transaction.{
  DomainParametersChange,
  NamespaceDelegation,
  OwnerToKeyMapping,
}
import com.digitalasset.canton.topology.{DomainId, _}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait DomainTopologyManagerIdentityInitialization {
  self: CantonNodeBootstrapBase[_, _, DomainNodeParameters] =>

  def initializeTopologyManagerIdentity(
      name: InstanceName,
      legalIdentityHook: X509Certificate => EitherT[Future, String, Unit],
      initialDynamicDomainParameters: DynamicDomainParameters,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, (NodeId, DomainTopologyManager, PublicKey)] =
    // initialize domain with local keys
    for {
      id <- Identifier.create(name.unwrap).toEitherT[Future]
      // TODO(i5202) if we crash here, we will like not be able to recover.
      nsName <- KeyName.create(s"$name-namespace").toEitherT[Future]
      ns <- crypto
        .generateSigningKey(name = Some(nsName))
        .leftMap(err => s"Failed to generate namespace key: $err")
      idmName <- KeyName.create(s"$name-signing").toEitherT[Future]
      idmKey <- crypto
        .generateSigningKey(name = Some(idmName))
        .leftMap(err => s"Failed to generate identity key: $err")
      uid = UniqueIdentifier(id, Namespace(ns.fingerprint))
      nodeId = NodeId(uid)
      _ <- storeId(nodeId)

      idMgr = initializeIdentityManagerAndServices(nodeId)

      _ <- authorizeStateUpdate(
        idMgr,
        ns,
        NamespaceDelegation(Namespace(ns.fingerprint), ns, isRootDelegation = true),
      )

      _ <- authorizeDomainGovernance(
        idMgr,
        ns,
        DomainParametersChange(DomainId(uid), initialDynamicDomainParameters),
      )

      domainTopologyManagerId = DomainTopologyManagerId(uid)
      _ <- authorizeStateUpdate(idMgr, ns, OwnerToKeyMapping(domainTopologyManagerId, idmKey))

      // Setup the legal identity of the domain nodes
      domainCert <- new LegalIdentityInit(certificateGenerator, crypto)
        .generateCertificate(uid, Seq(MediatorId(uid), domainTopologyManagerId))
      _ <- legalIdentityHook(domainCert)
    } yield (nodeId, idMgr, ns)

  protected def initializeIdentityManagerAndServices(nodeId: NodeId): DomainTopologyManager

}
