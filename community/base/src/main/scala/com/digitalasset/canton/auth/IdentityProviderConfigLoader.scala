// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.user.IdentityProviderConfig

import scala.concurrent.Future

trait IdentityProviderConfigLoader {

  def getIdentityProviderConfig(issuer: String)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[IdentityProviderConfig]

}
