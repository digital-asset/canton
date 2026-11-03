// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.package_service.{
  ListVettedPackagesRequest,
  PackageMetadataFilter,
  TopologyStateFilter,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.sizeconfig.v2.{
  PackageMetadataFilterSizeConfig,
  TopologyStateFilterSizeConfig,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import org.scalatest.wordspec.AnyWordSpec

class PackageServiceRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with SizeLimitTestUtils {
  private implicit val noLogging: ErrorLoggingContext = NoLogging

  private val maxPackageIds = 2
  private val maxPackageNamePrefixes = 2
  private val maxParticipantIds = 2
  private val maxSynchronizerIds = 2
  private val sizeLimitsConfigWithLimits = sizeLimitsConfig.copy(
    publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig.copy(
      packageMetadataFilter = PackageMetadataFilterSizeConfig(
        packageIdsSizeLimit = PositiveInt.tryCreate(maxPackageIds),
        packageNamePrefixesSizeLimit = PositiveInt.tryCreate(maxPackageNamePrefixes),
      ),
      topologyStateFilter = TopologyStateFilterSizeConfig(
        participantIdsSizeLimit = PositiveInt.tryCreate(maxParticipantIds),
        synchronizerIdsSizeLimit = PositiveInt.tryCreate(maxSynchronizerIds),
      ),
    )
  )

  private def values(prefix: String, number: Int): Seq[String] =
    (1 to number).map(i => s"$prefix$i")

  private def validate(
      packageMetadataFilter: Option[PackageMetadataFilter] = None,
      topologyStateFilter: Option[TopologyStateFilter] = None,
  ) =
    PackageServiceRequestValidator.validateListVettedPackagesRequest(
      ListVettedPackagesRequest(
        packageMetadataFilter = packageMetadataFilter,
        topologyStateFilter = topologyStateFilter,
        pageToken = "",
        pageSize = 0,
      )
    )(noLogging, sizeLimitsConfigWithLimits)

  "PackageServiceRequestValidator" when {
    "validating the size limits of list vetted packages requests" should {

      "accept a request without filters" in {
        validate() shouldBe a[Right[?, ?]]
      }

      cardinalityLimit("package_ids", limit = maxPackageIds)(numberOfPackageIds =>
        validate(packageMetadataFilter =
          Some(
            PackageMetadataFilter(
              packageIds = values("package", numberOfPackageIds),
              packageNamePrefixes = Nil,
            )
          )
        )
      )

      cardinalityLimit("package_name_prefixes", limit = maxPackageNamePrefixes)(numberOfPrefixes =>
        validate(packageMetadataFilter =
          Some(
            PackageMetadataFilter(
              packageIds = Nil,
              packageNamePrefixes = values("prefix", numberOfPrefixes),
            )
          )
        )
      )

      cardinalityLimit("participant_ids", limit = maxParticipantIds)(numberOfParticipantIds =>
        validate(topologyStateFilter =
          Some(
            TopologyStateFilter(
              participantIds = values("participant", numberOfParticipantIds),
              synchronizerIds = Nil,
            )
          )
        )
      )

      cardinalityLimit("synchronizer_ids", limit = maxSynchronizerIds)(numberOfSynchronizerIds =>
        validate(topologyStateFilter =
          Some(
            TopologyStateFilter(
              participantIds = Nil,
              synchronizerIds = values("synchronizer", numberOfSynchronizerIds),
            )
          )
        )
      )
    }
  }
}
