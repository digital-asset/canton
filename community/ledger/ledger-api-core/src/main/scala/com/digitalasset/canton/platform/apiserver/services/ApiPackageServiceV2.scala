// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.daml_lf_dev.DamlLf.{Archive, HashFunction}
import com.daml.ledger.api.v1.package_service.{
  GetPackageResponse,
  GetPackageStatusResponse,
  HashFunction as APIHashFunction,
  ListPackagesResponse,
  PackageStatus,
}
import com.daml.ledger.api.v2.package_service.PackageServiceGrpc.PackageService
import com.daml.ledger.api.v2.package_service.{
  GetPackageRequest,
  GetPackageStatusRequest,
  ListPackagesRequest,
  PackageServiceGrpc,
}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.grpc.Logging.traceId
import com.digitalasset.canton.ledger.api.validation.ValidationErrors
import com.digitalasset.canton.ledger.error.{DamlContextualizedErrorLogger, LedgerApiErrors}
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexPackagesService
import com.digitalasset.canton.logging.LoggingContextWithTrace
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiPackageServiceV2 private (
    backend: IndexPackagesService,
    telemetry: Telemetry,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends PackageService
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(telemetry)
    logger.info(s"Received request to list packages: $request")
    backend
      .listLfPackages()
      .map(p => ListPackagesResponse(p.keys.toSeq))
      .andThen(logger.logErrorsOnCall[ListPackagesResponse])
  }

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
    withEnrichedLoggingContext(
      logging.packageId(request.packageId),
      traceId(telemetry.traceIdFromGrpcContext),
    ) { implicit loggingContext =>
      logger.info(s"Received request for a package: $request")
      withValidatedPackageId(request.packageId, request) { packageId =>
        backend
          .getLfArchive(packageId)
          .flatMap {
            case None =>
              Future.failed[GetPackageResponse](
                LedgerApiErrors.RequestValidation.NotFound.Package
                  .Reject(packageId = packageId)(
                    createContextualizedErrorLogger
                  )
                  .asGrpcError
              )
            case Some(archive) => Future.successful(toGetPackageResponse(archive))
          }
          .andThen(logger.logErrorsOnCall[GetPackageResponse])
      }
    }

  override def getPackageStatus(
      request: GetPackageStatusRequest
  ): Future[GetPackageStatusResponse] =
    LoggingContextWithTrace.withEnrichedLoggingContext(telemetry)(
      logging.packageId(request.packageId)
    ) { implicit loggingContext =>
      logger.info(s"Received request for a package status: $request")
      withValidatedPackageId(request.packageId, request) { packageId =>
        backend
          .listLfPackages()
          .map { packages =>
            val result = if (packages.contains(packageId)) {
              PackageStatus.REGISTERED
            } else {
              PackageStatus.UNKNOWN
            }
            GetPackageStatusResponse(result)
          }
          .andThen(logger.logErrorsOnCall[GetPackageStatusResponse])
      }
    }

  private def withValidatedPackageId[T, R](packageId: String, request: R)(
      block: Ref.PackageId => Future[T]
  )(implicit loggingContext: LoggingContext): Future[T] =
    Ref.PackageId
      .fromString(packageId)
      .fold(
        errorMessage =>
          Future.failed[T](
            ValidationLogger.logFailure(
              request,
              ValidationErrors
                .invalidArgument(s"Invalid package id: $errorMessage")(
                  createContextualizedErrorLogger
                ),
            )
          ),
        packageId => block(packageId),
      )

  private def toGetPackageResponse(archive: Archive): GetPackageResponse = {
    val hashFunction = archive.getHashFunction match {
      case HashFunction.SHA256 => APIHashFunction.SHA256
      case _ => APIHashFunction.Unrecognized(-1)
    }
    GetPackageResponse(
      hashFunction = hashFunction,
      archivePayload = archive.getPayload,
      hash = archive.getHash,
    )
  }

  private def createContextualizedErrorLogger(implicit
      loggingContext: LoggingContext
  ): DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)
}
