// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.digitalasset.canton.integration.TestConsoleEnvironment

import scala.concurrent.Future

final class AllocateExternalPartyBoxToIDPAuthIT extends AllocationBoxToIDPAuthTests {

  override def serviceCallName: String =
    "PartyManagementService#AllocateExternalParty(<grant-rights-to-IDP-parties>)"

  protected def allocateFunction(
      serviceCallContext: ServiceCallContext,
      party: String,
      userId: String = "",
      identityProviderIdOverride: Option[String] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): Future[String] =
    allocateExternalParty(serviceCallContext, party, userId, identityProviderIdOverride)

}
