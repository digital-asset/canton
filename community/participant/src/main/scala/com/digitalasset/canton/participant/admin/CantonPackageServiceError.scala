// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.error.definitions.DamlError
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.PackageServiceErrorGroup
import com.digitalasset.canton.error.{CantonError, ParentCantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.admin.PackageService.DarDescriptor
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.topology.DomainId
import io.grpc.StatusRuntimeException

import scala.annotation.nowarn

object CantonPackageServiceError extends PackageServiceErrorGroup {
  @nowarn("msg=early initializers are deprecated")
  case class IdentityManagerParentError(parent: ParticipantTopologyManagerError)(implicit
      val loggingContext: ErrorLoggingContext,
      override val code: ErrorCode,
  ) extends {
        override val cause: String = parent.cause
      }
      with DamlError(parent.cause)
      with CantonError
      with ParentCantonError[ParticipantTopologyManagerError] {

    override def logOnCreation: Boolean = false

    override def asGrpcError: StatusRuntimeException = parent.asGrpcError

    override def mixinContext: Map[String, String] = Map("action" -> "package-vetting")
  }

  @Explanation(
    """Errors raised by the Package Service on package removal."""
  )
  object PackageRemovalErrorCode
      extends ErrorCode(
        id = "PACKAGE_OR_DAR_REMOVAL_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    abstract class PackageRemovalError(override val cause: String)(
        implicit override implicit val code: ErrorCode
    ) extends CantonError

    @Resolution(
      s"""To cleanly remove the package, you must archive all contracts from the package."""
    )
    class PackageInUse(val pkg: PackageId, val contract: ContractId, val domain: DomainId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(
          s"Package $pkg is currently in-use by contract $contract on domain $domain. " +
            s"It may also be in-use by other contracts."
        )

    @Resolution(
      s"""To cleanly remove the package, you must first revoke authorization for the package."""
    )
    class PackageVetted(pkg: PackageId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(s"Package $pkg is currently vetted and available to use.")

    @Resolution(
      s"""The DAR cannot be removed because a package in the DAR is in-use and is not found in any other DAR."""
    )
    class CannotRemoveOnlyDarForPackage(val pkg: PackageId, dar: DarDescriptor)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(
          cause =
            s"""The DAR $dar cannot be removed because it is the only DAR containing the used package $pkg.
               |Archive all contracts using the package $pkg,
               | or supply an alternative dar to $dar that contains $pkg.""".stripMargin
        )

    @Resolution(
      s"""Before removing a DAR, archive all contracts using its main package."""
    )
    class MainPackageInUse(
        val pkg: PackageId,
        dar: DarDescriptor,
        contractId: ContractId,
        domainId: DomainId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(
          s"""The DAR $dar cannot be removed because its main package $pkg is in-use by contract $contractId
         |on domain $domainId.""".stripMargin
        )

    @Resolution(
      resolution =
        s"Inspect the specific topology error, or manually revoke the package vetting transaction corresponding to" +
          s" the main package of the dar"
    )
    class DarUnvettingError(
        err: ParticipantTopologyManagerError,
        dar: DarDescriptor,
        mainPkg: PackageId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(
          s"An error was encountered whilst trying to unvet the DAR $dar with main package $mainPkg for DAR" +
            s" removal. Details: $err"
        )

  }

}
