// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.package_service.{
  ListVettedPackagesRequest,
  PackageMetadataFilter,
  PackageServiceGrpc,
  TopologyStateFilter,
}
import com.digitalasset.canton.auth.ClaimApiArea
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

final class ListVettedPackagesAuthIT
    extends PublicServiceCallAuthTests
    with ApiAreaScopeServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "PackageService#ListVettedPackages"

  override def apiAreaScopeBaseToken = standardToken(randomPartyReadUser)
  override def apiAreaScopeRestrictions = List[(List[ClaimApiArea.Area], Boolean)](
    List(ClaimApiArea.ledgerRead.area) -> true,
    List(ClaimApiArea.partiesAllocate.area) -> false,
  )

  private val request =
    ListVettedPackagesRequest(
      Some(PackageMetadataFilter(Seq(), Seq())),
      Some(TopologyStateFilter(Seq(), Seq())),
      "",
      0,
    )

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    stub(PackageServiceGrpc.stub(channel), context.token).listVettedPackages(request)

}
