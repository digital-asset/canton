// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.config.{CantonConfig, SharedCantonConfig, StorageConfig}
import com.digitalasset.canton.environment.{CantonEnvironment, Environment}
import com.digitalasset.canton.integration.ConfigTransforms.ConfigNodeType

package object integration {
  type BaseTestConsoleEnvironment[C <: SharedCantonConfig[C], +E <: Environment[C]] =
    E#Console & TestEnvironment[? <: C]
  type AnyTestConsoleEnvironment =
    BaseTestConsoleEnvironment[? <: SharedCantonConfig[?], ? <: Environment[?]]
  type TestConsoleEnvironment = BaseTestConsoleEnvironment[CantonConfig, CantonEnvironment]
  type CantonTestEnvironment = TestEnvironment[CantonConfig]
  type EnvironmentSetupPlugin = BaseEnvironmentSetupPlugin[CantonConfig, CantonEnvironment]
  type CantonEnvironmentSetup = EnvironmentSetup[CantonConfig, CantonEnvironment]
  type SharedEnvironment = BaseSharedEnvironment[CantonConfig, CantonEnvironment]
  type IsolatedEnvironments = BaseIsolatedEnvironments[CantonConfig, CantonEnvironment]
  type CantonBaseIntegrationTest = BaseIntegrationTest[CantonConfig, CantonEnvironment]
  type ConfigTransform = CantonConfig => CantonConfig
  type StorageConfigTransform =
    (ConfigNodeType, String, StorageConfig) => StorageConfig
}
