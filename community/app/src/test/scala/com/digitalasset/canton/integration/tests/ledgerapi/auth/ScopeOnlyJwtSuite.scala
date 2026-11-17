// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.StandardJWTTokenFormat
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthStartupConfigSuppressionRule
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixtureAbstract
import com.digitalasset.canton.integration.{
  CantonEnvironmentSetup,
  ConfigTransforms,
  EnvironmentDefinition,
}
import org.scalatest.BeforeAndAfterAll

/** A suite whose participant accepts the deprecated scope-only tokens: enables them with
  * `ConfigTransforms.enableScopeOnlyJwt` before the config plugins run and suppresses the startup
  * warning about them. The suite's config plugin still has to configure a `target-scope` for the
  * tokens it sends.
  *
  * The flag is added by overriding `environmentDefinition`, so a suite that overrides
  * `environmentDefinition` itself must build on `super.environmentDefinition`, or it loses the
  * transform and its participant rejects the scope-only tokens again.
  */
// TODO (i#33090): remove together with the scope-only token format
trait ScopeOnlyJwtEnabledSuite extends CantonFixtureAbstract with BeforeAndAfterAll {
  this: CantonEnvironmentSetup =>

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransform(ConfigTransforms.enableScopeOnlyJwt)

  // the participant warns at startup that it is configured for the deprecated scope-only tokens
  override def beforeAll(): Unit =
    loggerFactory.suppress(AuthStartupConfigSuppressionRule) {
      super.beforeAll()
    }
}

/** A [[ScopeOnlyJwtEnabledSuite]] that also mints scope-only tokens by default. */
// TODO (i#33090): remove together with the scope-only token format
trait ScopeOnlyJwtSuite extends ScopeOnlyJwtEnabledSuite {
  this: CantonEnvironmentSetup =>

  override protected def defaultTokenFormat: StandardJWTTokenFormat = StandardJWTTokenFormat.Scope
}
