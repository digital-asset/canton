// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.experimental_features.*
import com.daml.ledger.api.v1.version_service.{
  FeaturesDescriptor,
  GetLedgerApiVersionResponse,
  UserManagementFeature,
}
import com.daml.ledger.api.v2.version_service.VersionServiceGrpc.VersionService
import com.daml.ledger.api.v2.version_service.*
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.error.{DamlContextualizedErrorLogger, LedgerApiErrors}
import com.digitalasset.canton.platform.apiserver.LedgerFeatures
import com.digitalasset.canton.platform.localstore.UserManagementConfig
import io.grpc.ServerServiceDefinition

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.Try
import scala.util.control.NonFatal

private[apiserver] final class ApiVersionServiceV2 private (
    ledgerFeatures: LedgerFeatures,
    userManagementConfig: UserManagementConfig,
)(implicit
    loggingContext: LoggingContext,
    executionContext: ExecutionContext,
) extends VersionService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private val versionFile: String = "VERSION"
  private val apiVersion: Try[String] = readVersion(versionFile)

  private val featuresDescriptor =
    FeaturesDescriptor.of(
      userManagement = Some(
        if (userManagementConfig.enabled) {
          UserManagementFeature(
            supported = true,
            maxRightsPerUser = userManagementConfig.maxRightsPerUser,
            maxUsersPageSize = userManagementConfig.maxUsersPageSize,
          )
        } else {
          UserManagementFeature(
            supported = false,
            maxRightsPerUser = 0,
            maxUsersPageSize = 0,
          )
        }
      ),
      experimental = Some(
        ExperimentalFeatures.of(
          selfServiceErrorCodes = Some(ExperimentalSelfServiceErrorCodes()): @nowarn(
            "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.experimental_features\\..*"
          ),
          staticTime = Some(ExperimentalStaticTime(supported = ledgerFeatures.staticTime)),
          commandDeduplication = Some(ledgerFeatures.commandDeduplicationFeatures),
          optionalLedgerId = Some(ExperimentalOptionalLedgerId()),
          contractIds = Some(ledgerFeatures.contractIdFeatures),
          committerEventLog = Some(ledgerFeatures.committerEventLog),
          explicitDisclosure = Some(ledgerFeatures.explicitDisclosure),
          userAndPartyLocalMetadataExtensions =
            Some(ExperimentalUserAndPartyLocalMetadataExtensions(supported = true)),
          acsActiveAtOffset = Some(AcsActiveAtOffsetFeature(supported = true)),
        )
      ),
    )

  override def getLedgerApiVersion(
      request: GetLedgerApiVersionRequest
  ): Future[GetLedgerApiVersionResponse] =
    Future
      .fromTry(apiVersion)
      .map(apiVersionResponse)
      .andThen(logger.logErrorsOnCall[GetLedgerApiVersionResponse])
      .recoverWith { case NonFatal(_) =>
        Future.failed(
          LedgerApiErrors.InternalError
            .VersionService(message = "Cannot read Ledger API version")
            .asGrpcError
        )
      }

  private def apiVersionResponse(version: String) =
    GetLedgerApiVersionResponse(version, Some(featuresDescriptor))

  private def readVersion(versionFileName: String): Try[String] =
    Try {
      Source
        .fromResource(versionFileName)
        .getLines()
        .toList
        .headOption
        .getOrElse(throw new IllegalStateException("Empty version file"))
    }

  override def bindService(): ServerServiceDefinition =
    VersionServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

}
