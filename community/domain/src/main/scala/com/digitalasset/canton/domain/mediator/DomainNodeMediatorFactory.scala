// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.InstanceName
import com.digitalasset.canton.crypto.{Crypto, KeyName, PublicKey, SigningPublicKey}
import com.digitalasset.canton.domain.topology.DomainTopologyManager
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait DomainNodeMediatorFactory {

  /** When the domain node first initializes it needs an initial mediator key to create the essential state of the domain. */
  def fetchInitialMediatorKey(
      domainName: InstanceName,
      domainId: DomainId,
      mediatorId: MediatorId,
      namespaceKey: PublicKey,
      topologyManager: DomainTopologyManager,
      crypto: Crypto,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, SigningPublicKey]

  def mediatorRuntimeFactory: MediatorRuntimeFactory
}

object DomainNodeMediatorFactory {

  /** Trait for extended in the enterprise environment */
  trait Embedded extends DomainNodeMediatorFactory {
    override def fetchInitialMediatorKey(
        domainName: InstanceName,
        domainId: DomainId,
        mediatorId: MediatorId,
        namespaceKey: PublicKey,
        topologyManager: DomainTopologyManager,
        crypto: Crypto,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[Future, String, SigningPublicKey] =
      for {
        keyName <- EitherT.fromEither[Future](KeyName.create(s"$domainName-mediator-signing"))
        res <- crypto
          .generateSigningKey(name = Some(keyName))
          .leftMap(err => s"Failed to generate mediator signing key: $err")
      } yield res

    override val mediatorRuntimeFactory: MediatorRuntimeFactory = CommunityMediatorRuntimeFactory
  }

  /** The Mediator is operating within the DomainNode */
  object Embedded extends Embedded

}
