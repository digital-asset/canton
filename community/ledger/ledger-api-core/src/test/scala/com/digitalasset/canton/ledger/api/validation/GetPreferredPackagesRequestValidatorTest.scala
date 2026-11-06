// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  GetPreferredPackagesRequest,
  PackageVettingRequirement,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.sizeconfig.v2.interactive.{
  GetPreferredPackagesRequestSizeConfig,
  PackageVettingRequirementSizeConfig,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import org.scalatest.wordspec.AnyWordSpec

class GetPreferredPackagesRequestValidatorTest extends AnyWordSpec with SizeLimitTestUtils {

  private implicit val errorLoggingContext: ErrorLoggingContext = NoLogging

  "validating the size limits of a get preferred packages request" should {
    // distinct limits, so that a check reading the limit of another field fails
    val maxPackageVettingRequirements = 2
    val maxParties = 3

    val sizeLimitsConfigWithLimits = sizeLimitsConfig.copy(
      interactiveLapiSizeConfig = sizeLimitsConfig.interactiveLapiSizeConfig.copy(
        getPreferredPackagesRequest = GetPreferredPackagesRequestSizeConfig(
          packageVettingRequirementsSizeLimit = PositiveInt.tryCreate(maxPackageVettingRequirements)
        ),
        packageVettingRequirement = PackageVettingRequirementSizeConfig(
          partiesSizeLimit = PositiveInt.tryCreate(maxParties)
        ),
      )
    )

    def requirement(packageName: String, numberOfParties: Int) =
      PackageVettingRequirement(parties = parties(numberOfParties), packageName = packageName)

    def validate(requirements: Seq[PackageVettingRequirement]) =
      GetPreferredPackagesRequestValidator.validate(
        GetPreferredPackagesRequest(
          packageVettingRequirements = requirements,
          synchronizerId = "",
          vettingValidAt = None,
        )
      )(errorLoggingContext, sizeLimitsConfigWithLimits)

    cardinalityLimit(
      "package_vetting_requirements/packageVettingRequirements",
      limit = maxPackageVettingRequirements,
    )(numberOfRequirements =>
      validate((1 to numberOfRequirements).map(i => requirement(s"package-$i", 1)))
    )

    cardinalityLimit("parties", limit = maxParties)(numberOfParties =>
      validate(Seq(requirement("package-1", numberOfParties)))
    )
  }
}
