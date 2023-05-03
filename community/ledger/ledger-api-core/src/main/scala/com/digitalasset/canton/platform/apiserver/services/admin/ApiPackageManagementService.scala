// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.api.util.TimestampConversion
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.{ContextualizedErrorLogger, DamlError}
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v1.admin.package_management_service.*
import com.daml.lf.archive.{Dar, DarParser, Decode, GenDarReader}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.tracing.{Telemetry, TelemetryContext}
import com.digitalasset.canton.ledger.api.domain.{LedgerOffset, PackageEntry}
import com.digitalasset.canton.ledger.error.PackageServiceError.Validation
import com.digitalasset.canton.ledger.error.{DamlContextualizedErrorLogger, LedgerApiErrors}
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  IndexPackagesService,
  IndexTransactionsService,
}
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.platform.api.grpc.GrpcApiService
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPackageManagementService.*
import com.digitalasset.canton.platform.apiserver.services.logging
import com.digitalasset.canton.platform.server.api.services.grpc.Logging.traceId
import com.google.protobuf.ByteString
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
import scalaz.std.either.*
import scalaz.std.list.*
import scalaz.syntax.traverse.*

import java.util.zip.ZipInputStream
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Try, Using}

private[apiserver] final class ApiPackageManagementService private (
    packagesIndex: IndexPackagesService,
    transactionsService: IndexTransactionsService,
    packagesWrite: state.WritePackagesService,
    managementServiceTimeout: FiniteDuration,
    engine: Engine,
    darReader: GenDarReader[Archive],
    submissionIdGenerator: String => Ref.SubmissionId,
    telemetry: Telemetry,
)(implicit
    materializer: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends PackageManagementService
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  private val synchronousResponse = new SynchronousResponse(
    new SynchronousResponseStrategy(
      packagesIndex,
      packagesWrite,
    )
  )

  override def close(): Unit = synchronousResponse.close()

  override def bindService(): ServerServiceDefinition =
    PackageManagementServiceGrpc.bindService(this, executionContext)

  override def listKnownPackages(
      request: ListKnownPackagesRequest
  ): Future[ListKnownPackagesResponse] =
    withEnrichedLoggingContext(traceId(telemetry.traceIdFromGrpcContext)) {
      implicit loggingContext =>
        logger.info("Listing known packages")
        packagesIndex
          .listLfPackages()
          .map { pkgs =>
            ListKnownPackagesResponse(pkgs.toSeq.map { case (pkgId, details) =>
              PackageDetails(
                pkgId.toString,
                details.size,
                Some(TimestampConversion.fromLf(details.knownSince)),
                details.sourceDescription.getOrElse(""),
              )
            })
          }
          .andThen(logger.logErrorsOnCall[ListKnownPackagesResponse])
    }

  private def decodeAndValidate(
      darFile: ByteString
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Dar[Archive]] = Future.delegate {
    // Triggering computation in `executionContext` as caller thread (from netty)
    // should not be busy with heavy computation
    val result = for {
      darArchive <- Using(new ZipInputStream(darFile.newInput())) { stream =>
        darReader.readArchive("package-upload", stream)
      }
      dar <- darArchive.handleError(Validation.handleLfArchiveError)
      packages <- dar.all
        .traverse(Decode.decodeArchive(_))
        .handleError(Validation.handleLfArchiveError)
      _ <- engine
        .validatePackages(packages.toMap)
        .handleError(Validation.handleLfEnginePackageError)
    } yield dar
    Future.fromTry(result)
  }

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] = {
    val submissionId = submissionIdGenerator(request.submissionId)
    withEnrichedLoggingContext(
      logging.submissionId(submissionId),
      traceId(telemetry.traceIdFromGrpcContext),
    ) { implicit loggingContext =>
      logger.info("Uploading DAR file")

      implicit val telemetryContext: TelemetryContext =
        telemetry.contextFromGrpcThreadLocalContext()

      implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
        new DamlContextualizedErrorLogger(
          logger,
          loggingContext,
          Some(submissionId),
        )

      val response = for {
        dar <- decodeAndValidate(request.darFile)
        ledgerEndbeforeRequest <- transactionsService.currentLedgerEnd().map(Some(_))
        _ <- synchronousResponse.submitAndWait(
          submissionId,
          dar,
          ledgerEndbeforeRequest,
          managementServiceTimeout,
        )
      } yield {
        for (archive <- dar.all) {
          logger.info(s"Package ${archive.getHash} successfully uploaded")
        }
        UploadDarFileResponse()
      }
      response.andThen(logger.logErrorsOnCall[UploadDarFileResponse])
    }
  }

  private implicit class ErrorValidations[E, R](result: Either[E, R]) {
    def handleError(toSelfServiceErrorCode: E => DamlError): Try[R] =
      result.left.map { err =>
        toSelfServiceErrorCode(err).asGrpcError
      }.toTry
  }
}

private[apiserver] object ApiPackageManagementService {

  def createApiService(
      readBackend: IndexPackagesService,
      transactionsService: IndexTransactionsService,
      writeBackend: state.WritePackagesService,
      managementServiceTimeout: FiniteDuration,
      engine: Engine,
      darReader: GenDarReader[Archive] = DarParser,
      submissionIdGenerator: String => Ref.SubmissionId = augmentSubmissionId,
      telemetry: Telemetry,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): PackageManagementServiceGrpc.PackageManagementService with GrpcApiService =
    new ApiPackageManagementService(
      readBackend,
      transactionsService,
      writeBackend,
      managementServiceTimeout,
      engine,
      darReader,
      submissionIdGenerator,
      telemetry,
    )

  private final class SynchronousResponseStrategy(
      packagesIndex: IndexPackagesService,
      packagesWrite: state.WritePackagesService,
  )(implicit loggingContext: LoggingContext)
      extends SynchronousResponse.Strategy[
        Dar[Archive],
        PackageEntry,
        PackageEntry.PackageUploadAccepted,
      ] {
    private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

    override def submit(submissionId: Ref.SubmissionId, dar: Dar[Archive])(implicit
        telemetryContext: TelemetryContext,
        loggingContext: LoggingContext,
    ): Future[state.SubmissionResult] =
      packagesWrite.uploadPackages(submissionId, dar.all, None).asScala

    override def entries(offset: Option[LedgerOffset.Absolute]): Source[PackageEntry, _] =
      packagesIndex.packageEntries(offset)

    override def accept(
        submissionId: Ref.SubmissionId
    ): PartialFunction[PackageEntry, PackageEntry.PackageUploadAccepted] = {
      case entry @ PackageEntry.PackageUploadAccepted(`submissionId`, _) => entry
    }

    override def reject(
        submissionId: Ref.SubmissionId
    ): PartialFunction[PackageEntry, StatusRuntimeException] = {
      case PackageEntry.PackageUploadRejected(`submissionId`, _, reason) =>
        LedgerApiErrors.Admin.PackageUploadRejected
          .Reject(reason)(
            new DamlContextualizedErrorLogger(logger, loggingContext, Some(submissionId))
          )
          .asGrpcError
    }
  }
}
