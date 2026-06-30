// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.logging.SuppressionRule
import org.slf4j.event.Level

object SuppressionRules {

  val AuthInterceptorSuppressionRule: SuppressionRule =
    SuppressionRule.forLogger[AuthInterceptor] && SuppressionRule.Level(Level.WARN)

  val AuthServiceJWTSuppressionRule: SuppressionRule =
    (SuppressionRule.forLogger[AuthInterceptor]
      || SuppressionRule.LoggerNameContains("AuthServiceJWTCodec"))
      && SuppressionRule.LevelAndAbove(Level.WARN)

  val ApiUserManagementServiceSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("ApiUserManagementService") &&
      SuppressionRule.Level(Level.WARN)

  val ApiPartyManagementServiceSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("ApiPartyManagementService") &&
      SuppressionRule.Level(Level.WARN)

  val DbActiveContractStoreConsistencyCheckSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("DbActiveContractStore") &&
      SuppressionRule.Level(Level.WARN)

  val AuthStartupConfigSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("AuthServiceJWT$") &&
      SuppressionRule.Level(Level.WARN)
}
