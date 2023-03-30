// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.logging.LoggingContext
import com.digitalasset.canton.ledger.api.domain.IdentityProviderId
import com.digitalasset.canton.platform.localstore.api.IdentityProviderConfigStore

import scala.concurrent.Future

class IdentityProviderExists(identityProviderConfigStore: IdentityProviderConfigStore) {
  def apply(id: IdentityProviderId)(implicit
      loggingContext: LoggingContext
  ): Future[Boolean] =
    id match {
      case IdentityProviderId.Default => Future.successful(true)
      case id: IdentityProviderId.Id =>
        identityProviderConfigStore.identityProviderConfigExists(id)
    }
}
