// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.implicits.{toFoldableOps, toTraverseOps}
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  GetPreferredPackagesRequest,
  PackageVettingRequirement,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.{LfPackageName, LfPartyId}
import io.grpc.StatusRuntimeException

object GetPreferredPackagesRequestValidator {
  def validate(
      request: GetPreferredPackagesRequest
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[
    StatusRuntimeException,
    (PackageVettingRequirements, Option[SynchronizerId], Option[CantonTimestamp]),
  ] =
    for {
      _ <- FieldValidator.validateCardinality(
        values = request.packageVettingRequirements,
        fieldName = "package_vetting_requirements/packageVettingRequirements",
        limit =
          sizeLimitsConfig.interactiveLapiSizeConfig.getPreferredPackagesRequest.packageVettingRequirementsSizeLimit,
      )
      _ <- request.packageVettingRequirements.traverse_(requirement =>
        FieldValidator.validateCardinality(
          values = requirement.parties,
          fieldName = "parties",
          limit =
            sizeLimitsConfig.interactiveLapiSizeConfig.packageVettingRequirement.partiesSizeLimit,
        )
      )
      _ <- FieldValidator
        .requireNonEmpty(
          s = request.packageVettingRequirements,
          fieldName = "package_vetting_requirements/packageVettingRequirements",
        )
      packageVettingRequirements <- request.packageVettingRequirements
        .traverse(validatePackageVettingRequirement)
        .map(_.toMap)
      synchronizerIdO <- FieldValidator.optionalSynchronizerId(
        s = request.synchronizerId,
        fieldName = "synchronizer_id/synchronizerId",
      )
      vettingValidAtO <- request.vettingValidAt
        .traverse(
          CantonTimestamp
            .fromProtoTimestamp(_)
            .left
            .map(protoDeserializationError =>
              ValidationErrors.invalidField(
                fieldName = "vetting_valid_at/vettingValidAt",
                message = protoDeserializationError.message,
              )
            )
        )
    } yield (
      PackageVettingRequirements(packageVettingRequirements),
      synchronizerIdO,
      vettingValidAtO,
    )

  private def validatePackageVettingRequirement(
      requirement: PackageVettingRequirement
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, (LfPackageName, Set[LfPartyId])] =
    for {
      packageName <- FieldValidator
        .requirePackageName(
          s = requirement.packageName,
          fieldName = "package_name/packageName",
        )
      nonEmptyRawParties <- FieldValidator.requireNonEmpty(
        s = requirement.parties,
        fieldName = "parties",
      )
      parties <- FieldValidator.requireParties(nonEmptyRawParties.toSet)
    } yield packageName -> parties

  /** Defines which package-names must have commonly-vetted packages for the provided parties.
    */
  final case class PackageVettingRequirements(
      value: Map[LfPackageName, Set[LfPartyId]]
  ) {
    lazy val allPackageNames: Set[LfPackageName] = value.keySet
    lazy val allParties: Set[LfPartyId] = value.values.flatten.toSet
  }
}
