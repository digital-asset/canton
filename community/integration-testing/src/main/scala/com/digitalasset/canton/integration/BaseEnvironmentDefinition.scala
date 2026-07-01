// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.config.{SharedCantonConfig, TestingConfigInternal}
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.logging.NamedLoggerFactory

/** Definition of how a environment should be configured and setup.
  * @param baseConfig
  *   the base config to use (typically loaded from a pre-canned config file or sample)
  * @param testingConfig
  *   the testing specifics bits of the config
  * @param setups
  *   a function to configure the environment before tests can be run.
  * @param teardown
  *   a function to perform cleanup after the environment has been destroyed.
  * @param configTransforms
  *   transforms to perform on the base configuration before starting the environment (typically
  *   making ports unique or some other specialization for the particular tests you're running)
  */
abstract class BaseEnvironmentDefinition[
    C
      <: SharedCantonConfig[
        C
      ],
    E <: Environment[C],
](
    val baseConfig: C,
    val testingConfig: TestingConfigInternal,
    val setups: List[BaseTestConsoleEnvironment[C, E] => Unit] = Nil,
    val teardown: Unit => Unit = _ => (),
    val configTransforms: Seq[C => C],
) {

  /** Create a canton configuration by applying the configTransforms to the base config. Some
    * transforms may have side-effects (such as incrementing the next available port number) so only
    * do before constructing an environment.
    */
  def generateConfig: C =
    configTransforms.foldLeft(baseConfig)((config, transform) => transform(config))

  def createTestConsole(
      environment: E,
      loggerFactory: NamedLoggerFactory,
  ): BaseTestConsoleEnvironment[C, E]
}
