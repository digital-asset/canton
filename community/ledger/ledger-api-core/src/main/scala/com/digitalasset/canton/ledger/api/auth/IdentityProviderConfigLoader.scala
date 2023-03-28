// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.logging.LoggingContext
import com.digitalasset.canton.ledger.api.domain.IdentityProviderConfig

import scala.concurrent.Future

trait IdentityProviderConfigLoader {

  def getIdentityProviderConfig(issuer: String)(implicit
      loggingContext: LoggingContext
  ): Future[IdentityProviderConfig]

}
