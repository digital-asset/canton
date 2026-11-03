// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.syntax.foldable.*
import com.daml.ledger.api.v2.admin.package_management_service.{
  UpdateVettedPackagesRequest,
  UploadDarFileRequest,
  ValidateDarFileRequest,
  VettedPackagesChange,
}
import com.digitalasset.canton.ledger.api.validation.FieldValidator.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import io.grpc.StatusRuntimeException

object PackageManagementServiceRequestValidator {

  def validateValidateDarFileRequest(request: ValidateDarFileRequest)(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] =
    validateByteSizeLimit(
      bytes = request.darFile,
      fieldName = "dar_file",
      limit = sizeLimitsConfig.adminLapiSizeConfig.validateDarFileRequest.darFileSizeLimit,
    )

  def validateUploadDarFileRequest(request: UploadDarFileRequest)(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] =
    validateByteSizeLimit(
      bytes = request.darFile,
      fieldName = "dar_file",
      limit = sizeLimitsConfig.adminLapiSizeConfig.uploadDarFileRequest.darFileSizeLimit,
    )

  def validateUpdateVettedPackagesRequest(request: UpdateVettedPackagesRequest)(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] =
    for {
      _ <- validateCardinality(
        values = request.changes,
        fieldName = "changes",
        limit = sizeLimitsConfig.adminLapiSizeConfig.updateVettedPackagesRequest.changesSizeLimit,
      )
      _ <- validateCardinality(
        values = request.updateVettedPackagesForceFlags,
        fieldName = "update_vetted_packages_force_flags",
        limit =
          sizeLimitsConfig.adminLapiSizeConfig.updateVettedPackagesRequest.updateVettedPackagesForceFlagsSizeLimit,
      )
      _ <- request.changes
        .traverse_(_.operation match {
          case VettedPackagesChange.Operation.Vet(vet) =>
            validateCardinality(
              values = vet.packages,
              fieldName = "packages",
              limit = sizeLimitsConfig.adminLapiSizeConfig.vettedPackagesChangeVet.packagesSizeLimit,
            )
          case VettedPackagesChange.Operation.Unvet(unvet) =>
            validateCardinality(
              values = unvet.packages,
              fieldName = "packages",
              limit =
                sizeLimitsConfig.adminLapiSizeConfig.vettedPackagesChangeUnvet.packagesSizeLimit,
            )
          // missing operations are rejected by UpdateVettedPackagesOpts.fromProto
          case VettedPackagesChange.Operation.Empty => Right(())
        })
    } yield ()
}
