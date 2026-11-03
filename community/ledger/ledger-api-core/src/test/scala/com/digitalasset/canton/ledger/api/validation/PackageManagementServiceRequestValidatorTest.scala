// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.admin.package_management_service.{
  UpdateVettedPackagesForceFlag,
  UpdateVettedPackagesRequest,
  UploadDarFileRequest,
  ValidateDarFileRequest,
  VettedPackagesChange,
  VettedPackagesRef,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.sizeconfig.v2.admin.{
  UpdateVettedPackagesRequestSizeConfig,
  UploadDarFileRequestSizeConfig,
  ValidateDarFileRequestSizeConfig,
  VettedPackagesChangeUnvetSizeConfig,
  VettedPackagesChangeVetSizeConfig,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import org.scalatest.wordspec.AnyWordSpec

class PackageManagementServiceRequestValidatorTest extends AnyWordSpec with SizeLimitTestUtils {

  private implicit val errorLoggingContext: ErrorLoggingContext = NoLogging

  "validating the size limits of the package management requests" should {
    // distinct limits, so that a check reading the limit of another field fails
    val maxUploadDarFileBytes = 4
    val maxValidateDarFileBytes = 5
    val maxChanges = 2
    val maxForceFlags = 3
    val maxVetPackages = 4
    val maxUnvetPackages = 5

    val sizeLimitsConfigWithLimits: LedgerApiSizeLimitsConfig =
      sizeLimitsConfig.copy(
        adminLapiSizeConfig = sizeLimitsConfig.adminLapiSizeConfig.copy(
          uploadDarFileRequest = UploadDarFileRequestSizeConfig(darFileSizeLimit =
            PositiveInt.tryCreate(maxUploadDarFileBytes)
          ),
          validateDarFileRequest = ValidateDarFileRequestSizeConfig(darFileSizeLimit =
            PositiveInt.tryCreate(maxValidateDarFileBytes)
          ),
          updateVettedPackagesRequest = UpdateVettedPackagesRequestSizeConfig(
            changesSizeLimit = PositiveInt.tryCreate(maxChanges),
            updateVettedPackagesForceFlagsSizeLimit = PositiveInt.tryCreate(maxForceFlags),
          ),
          vettedPackagesChangeVet = VettedPackagesChangeVetSizeConfig(packagesSizeLimit =
            PositiveInt.tryCreate(maxVetPackages)
          ),
          vettedPackagesChangeUnvet = VettedPackagesChangeUnvetSizeConfig(packagesSizeLimit =
            PositiveInt.tryCreate(maxUnvetPackages)
          ),
        )
      )

    def packages(numberOfPackages: Int): Seq[VettedPackagesRef] =
      (1 to numberOfPackages).map(i => VettedPackagesRef(s"package$i", "", ""))

    def vet(numberOfPackages: Int): VettedPackagesChange =
      VettedPackagesChange(
        VettedPackagesChange.Operation.Vet(
          VettedPackagesChange.Vet(packages(numberOfPackages), None, None)
        )
      )

    def unvet(numberOfPackages: Int): VettedPackagesChange =
      VettedPackagesChange(
        VettedPackagesChange.Operation.Unvet(VettedPackagesChange.Unvet(packages(numberOfPackages)))
      )

    byteSizeLimit("dar_file", maxUploadDarFileBytes, limitName = "limit of the upload")(
      numberOfBytes =>
        PackageManagementServiceRequestValidator.validateUploadDarFileRequest(
          UploadDarFileRequest.defaultInstance.copy(darFile = bytes(numberOfBytes))
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    byteSizeLimit("dar_file", maxValidateDarFileBytes, limitName = "limit of the validation")(
      numberOfBytes =>
        PackageManagementServiceRequestValidator.validateValidateDarFileRequest(
          ValidateDarFileRequest.defaultInstance.copy(darFile = bytes(numberOfBytes))
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("changes", maxChanges)(numberOfChanges =>
      PackageManagementServiceRequestValidator.validateUpdateVettedPackagesRequest(
        UpdateVettedPackagesRequest.defaultInstance.copy(changes =
          Seq.fill(numberOfChanges)(vet(1))
        )
      )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("update_vetted_packages_force_flags", maxForceFlags)(numberOfForceFlags =>
      PackageManagementServiceRequestValidator.validateUpdateVettedPackagesRequest(
        UpdateVettedPackagesRequest.defaultInstance.copy(updateVettedPackagesForceFlags =
          Seq.fill(numberOfForceFlags)(
            UpdateVettedPackagesForceFlag.UPDATE_VETTED_PACKAGES_FORCE_FLAG_ALLOW_UNVETTED_DEPENDENCIES
          )
        )
      )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("packages", maxVetPackages, limitName = "limit of a vet change")(
      numberOfPackages =>
        PackageManagementServiceRequestValidator.validateUpdateVettedPackagesRequest(
          UpdateVettedPackagesRequest.defaultInstance.copy(changes = Seq(vet(numberOfPackages)))
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("packages", maxUnvetPackages, limitName = "limit of an unvet change")(
      numberOfPackages =>
        PackageManagementServiceRequestValidator.validateUpdateVettedPackagesRequest(
          UpdateVettedPackagesRequest.defaultInstance.copy(changes = Seq(unvet(numberOfPackages)))
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )
  }
}
