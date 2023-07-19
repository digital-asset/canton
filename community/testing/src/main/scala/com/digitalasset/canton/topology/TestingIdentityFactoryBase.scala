// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, SyncCryptoApiProvider}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.client.{IdentityProvidingServiceClient, TopologySnapshot}

import scala.concurrent.Future

trait TestingIdentityFactoryBase {

  def forOwner(owner: KeyOwner): SyncCryptoApiProvider
  def forOwnerAndDomain(
      owner: KeyOwner,
      domain: DomainId = DefaultTestIdentities.domainId,
  ): DomainSyncCryptoClient
  def ips(): IdentityProvidingServiceClient
  def topologySnapshot(
      domainId: DomainId = DefaultTestIdentities.domainId,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
      timestampForDomainParameters: CantonTimestamp = CantonTimestamp.Epoch,
  ): TopologySnapshot

}
