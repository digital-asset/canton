// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.identity_provider_config_service.ListIdentityProviderConfigsRequest
import com.digitalasset.canton.auth.ClaimApiArea
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

final class ListIdentityProviderConfigsAuthIT
    extends AdminServiceCallAuthTests
    with IdentityProviderConfigAuth
    with ApiAreaScopeServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String =
    "IdentityProviderConfigService#ListIdentityProviderConfigs"

  override def apiAreaScopeBaseToken = adminToken
  override def apiAreaScopeRestrictions = List(
    List(ClaimApiArea.idpsRead.area) -> true,
    List(ClaimApiArea.ledgerRead.area) -> false,
  )

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    idpStub(context).listIdentityProviderConfigs(ListIdentityProviderConfigsRequest())

}
