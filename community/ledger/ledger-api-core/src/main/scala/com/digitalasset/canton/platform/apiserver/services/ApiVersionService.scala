// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v2.experimental_features.*
import com.daml.ledger.api.v2.version_service.VersionServiceGrpc.VersionService
import com.daml.ledger.api.v2.version_service.{
  FeaturesDescriptor,
  GetLedgerApiVersionResponse,
  UserManagementFeature,
  *,
}
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.LedgerFeatures
import com.digitalasset.canton.platform.config.{
  PackageServiceConfig,
  PartyManagementServiceConfig,
  UserManagementServiceConfig,
}
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiVersionService(
    ledgerFeatures: LedgerFeatures,
    userManagementServiceConfig: UserManagementServiceConfig,
    partyManagementServiceConfig: PartyManagementServiceConfig,
    packageServiceConfig: PackageServiceConfig,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends VersionService
    with GrpcApiService
    with NamedLogging {

  private val apiVersion: String = BuildInfo.version

  private val featuresDescriptor =
    FeaturesDescriptor.of(
      userManagement = Some(
        if (userManagementServiceConfig.enabled) {
          UserManagementFeature(
            supported = true,
            maxRightsPerUser = userManagementServiceConfig.maxRightsPerUser,
            maxUsersPageSize = userManagementServiceConfig.maxUsersPageSize,
          )
        } else {
          UserManagementFeature(
            supported = false,
            maxRightsPerUser = 0,
            maxUsersPageSize = 0,
          )
        }
      ),
      partyManagement = Some(
        PartyManagementFeature(
          maxPartiesPageSize = partyManagementServiceConfig.maxPartiesPageSize.value
        )
      ),
      experimental = Some(
        ExperimentalFeatures.of(
          staticTime = Some(ExperimentalStaticTime(supported = ledgerFeatures.staticTime)),
          commandInspectionService = Some(ledgerFeatures.commandInspectionService),
        )
      ),
      offsetCheckpoint = Some(ledgerFeatures.offsetCheckpointFeature),
      packageFeature = Some(
        PackageFeature.of(
          maxVettedPackagesPageSize = packageServiceConfig.maxVettedPackagesPageSize.value
        )
      ),
    )

  override def getLedgerApiVersion(
      request: GetLedgerApiVersionRequest
  ): Future[GetLedgerApiVersionResponse] =
    Future.successful(apiVersionResponse(apiVersion))
  private def apiVersionResponse(version: String) =
    GetLedgerApiVersionResponse(version, Some(featuresDescriptor))

  override def bindService(): ServerServiceDefinition =
    VersionServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

}
