// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.package_service.{
  ListVettedPackagesRequest,
  PackageMetadataFilter,
  TopologyStateFilter,
}
import com.digitalasset.canton.logging.ErrorLoggingContext
import io.grpc.StatusRuntimeException

object PackageServiceRequestValidator {

  import FieldValidator.*

  def validateListVettedPackagesRequest(
      request: ListVettedPackagesRequest
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, ListVettedPackagesRequest] =
    for {
      _ <- validateOptional(request.packageMetadataFilter)(validatePackageMetadataFilter)
      _ <- validateOptional(request.topologyStateFilter)(validateTopologyStateFilter)
    } yield request

  private def validatePackageMetadataFilter(
      filter: PackageMetadataFilter
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] =
    for {
      _ <- validateCardinality(
        values = filter.packageIds,
        fieldName = "package_ids",
        limit = sizeLimitsConfig.publicLapiSizeConfig.packageMetadataFilter.packageIdsSizeLimit,
      )
      _ <- validateCardinality(
        values = filter.packageNamePrefixes,
        fieldName = "package_name_prefixes",
        limit =
          sizeLimitsConfig.publicLapiSizeConfig.packageMetadataFilter.packageNamePrefixesSizeLimit,
      )
    } yield ()

  private def validateTopologyStateFilter(
      filter: TopologyStateFilter
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] =
    for {
      _ <- validateCardinality(
        values = filter.participantIds,
        fieldName = "participant_ids",
        limit = sizeLimitsConfig.publicLapiSizeConfig.topologyStateFilter.participantIdsSizeLimit,
      )
      _ <- validateCardinality(
        values = filter.synchronizerIds,
        fieldName = "synchronizer_ids",
        limit = sizeLimitsConfig.publicLapiSizeConfig.topologyStateFilter.synchronizerIdsSizeLimit,
      )
    } yield ()

}
