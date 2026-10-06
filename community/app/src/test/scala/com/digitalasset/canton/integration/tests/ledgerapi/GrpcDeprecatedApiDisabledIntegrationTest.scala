// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.digitalasset.canton.LfPackageName
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.DeprecatedApiDisabled

/** Checks that the gRPC ledger API methods deprecated in 3.4 are rejected with
  * `DEPRECATED_API_DISABLED` unless the `canton.participants.<participant>.features.deprecated`
  * flag that covers them is set (none is set here). The JSON API counterpart is
  * `JsonV2DeprecatedApiDisabledTests`, the positive counterpart is
  * `GetPreferredPackageVersionAuthIT`, which enables the flag.
  */
// TODO(#35974) remove together with the deprecated endpoints in 3.7
sealed trait GrpcDeprecatedApiDisabledIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
    }

  "gRPC API with deprecated 3.4 APIs disabled" should {
    "reject GetPreferredPackageVersion" in { implicit env =>
      import env.*

      val alice = participant1.parties.enable("AliceGrpc")
      assertThrowsAndLogsCommandFailures(
        participant1.ledger_api.interactive_submission.preferred_package_version(
          parties = Set(alice),
          packageName = LfPackageName.assertFromString("SomePackage"),
        ),
        _.commandFailureMessage should include(DeprecatedApiDisabled.id),
      )
    }
  }
}

final class GrpcDeprecatedApiDisabledIntegrationTestH2
    extends GrpcDeprecatedApiDisabledIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
