// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.ErrorsAssertions
import com.daml.error.utils.ErrorDetails
import com.daml.error.utils.ErrorDetails.RetryInfoDetail
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v1.admin.package_management_service.{
  PackageManagementServiceGrpc,
  UploadDarFileRequest,
  ValidateDarFileRequest,
  ValidateDarFileResponse,
}
import com.daml.lf.archive.testing.Encode
import com.daml.lf.archive.{Dar, GenDarReader}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.tracing.TelemetrySpecBase.*
import com.daml.tracing.{DefaultOpenTelemetry, NoOpTelemetry}
import com.digitalasset.canton.ledger.api.domain.LedgerOffset.Absolute
import com.digitalasset.canton.ledger.api.domain.PackageEntry
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  IndexPackagesService,
  IndexTransactionsService,
}
import com.digitalasset.canton.ledger.participant.state.v2.{ReadService, SubmissionResult}
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.{LoggingContextWithTrace, SuppressionRule}
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext}
import com.digitalasset.canton.{BaseTest, DiscardOps}
import com.google.protobuf.ByteString
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import org.apache.pekko.stream.scaladsl.Source
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level.DEBUG

import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.zip.ZipInputStream
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class ApiPackageManagementServiceSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with PekkoBeforeAndAfterAll
    with Eventually
    with ErrorsAssertions
    with BaseTest
    with BeforeAndAfterEach {

  import ApiPackageManagementServiceSpec.*

  var testTelemetrySetup: TestTelemetrySetup = _

  override def beforeEach(): Unit =
    testTelemetrySetup = new TestTelemetrySetup()

  override def afterEach(): Unit =
    testTelemetrySetup.close()

  "ApiPackageManagementService" should {
    "propagate trace context" in {
      val apiService = createApiService()
      val span = testTelemetrySetup.anEmptySpan()
      val scope = span.makeCurrent()
      apiService
        .uploadDarFile(UploadDarFileRequest(ByteString.EMPTY, aSubmissionId))
        .andThen { case _ =>
          scope.close()
          span.end()
        }
        .map { _ =>
          testTelemetrySetup.reportedSpanAttributes should contain(anApplicationIdSpanAttribute)
          succeed
        }
    }

    "have a tid" in {
      val apiService = createApiService()
      val span = testTelemetrySetup.anEmptySpan()
      val _ = span.makeCurrent()

      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(DEBUG))(
        within = {
          apiService
            .uploadDarFile(UploadDarFileRequest(ByteString.EMPTY, aSubmissionId))
            .map(_ => succeed)
        },
        { logEntries =>
          logEntries should not be empty

          val mdcs = logEntries.map(_.mdc)
          forEvery(mdcs)(_.getOrElse("trace-id", "") should not be empty)
        },
      )
    }

    "close while uploading dar" in {
      val writeService = mock[state.WritePackagesService]
      when(
        writeService.uploadPackages(any[Ref.SubmissionId], any[ByteString])(
          any[TraceContext]
        )
      ).thenReturn(CompletableFuture.completedFuture(SubmissionResult.Acknowledged))

      val (
        mockIndexTransactionsService,
        mockIndexPackagesService,
      ) =
        mockedServices()
      val promise = Promise[Unit]()

      when(
        mockIndexPackagesService.packageEntries(any[Option[Absolute]])(any[LoggingContextWithTrace])
      )
        .thenReturn(
          {
            promise.success(())
            Source.never
          }
        )

      val apiPackageManagementService = ApiPackageManagementService.createApiService(
        mockIndexPackagesService,
        mockIndexTransactionsService,
        writeService,
        mock[ReadService],
        Duration.Zero,
        _ => Ref.SubmissionId.assertFromString("aSubmission"),
        telemetry = NoOpTelemetry,
        loggerFactory = loggerFactory,
      )

      promise.future.map(_ => apiPackageManagementService.close()).discard

      apiPackageManagementService
        .uploadDarFile(
          UploadDarFileRequest(ByteString.EMPTY, aSubmissionId)
        )
        .transform {
          case Success(_) =>
            fail("Expected a failure, but received success")
          case Failure(err: StatusRuntimeException) =>
            assertError(
              actual = err,
              expectedStatusCode = Code.UNAVAILABLE,
              expectedMessage = "SERVER_IS_SHUTTING_DOWN(1,0): Server is shutting down",
              expectedDetails = List(
                ErrorDetails.ErrorInfoDetail(
                  "SERVER_IS_SHUTTING_DOWN",
                  Map(
                    "submissionId" -> s"'$aSubmissionId'",
                    "category" -> "1",
                    "definite_answer" -> "false",
                    "test" -> s"'${getClass.getSimpleName}'",
                  ),
                ),
                RetryInfoDetail(1.second),
              ),
              verifyEmptyStackTrace = true,
            )
            Success(succeed)
          case Failure(other) =>
            fail("Unexpected error", other)
        }
    }

    "validate a dar" in {
      val darPayload = ByteString.copyFrom("SomeDar".getBytes)
      val mockReadService = mock[ReadService]
      when(mockReadService.validateDar(eqTo(darPayload))(anyTraceContext))
        .thenReturn(Future.successful(SubmissionResult.Acknowledged))
      val apiService = createApiService(mockReadService)
      apiService
        .validateDarFile(ValidateDarFileRequest(darPayload, aSubmissionId))
        .map { case ValidateDarFileResponse() => succeed }
    }

  }

  private def mockedServices(): (
      IndexTransactionsService,
      IndexPackagesService,
  ) = {
    val mockDarReader = mock[GenDarReader[Archive]]
    when(mockDarReader.readArchive(any[String], any[ZipInputStream], any[Int]))
      .thenReturn(Right(new Dar[Archive](anArchive, List.empty)))

    val mockIndexTransactionsService = mock[IndexTransactionsService]
    when(mockIndexTransactionsService.currentLedgerEnd())
      .thenReturn(Future.successful(Absolute(Ref.LedgerString.assertFromString("0"))))

    val mockIndexPackagesService = mock[IndexPackagesService]
    when(
      mockIndexPackagesService.packageEntries(any[Option[Absolute]])(any[LoggingContextWithTrace])
    )
      .thenReturn(
        Source.single(
          PackageEntry.PackageUploadAccepted(aSubmissionId, Timestamp.Epoch)
        )
      )

    (
      mockIndexTransactionsService,
      mockIndexPackagesService,
    )
  }

  private def createApiService(
      readService: ReadService = mock[ReadService]
  ): PackageManagementServiceGrpc.PackageManagementService = {
    val (
      mockIndexTransactionsService,
      mockIndexPackagesService,
    ) =
      mockedServices()

    ApiPackageManagementService.createApiService(
      mockIndexPackagesService,
      mockIndexTransactionsService,
      TestWritePackagesService(testTelemetrySetup.tracer),
      readService,
      Duration.Zero,
      _ => Ref.SubmissionId.assertFromString("aSubmission"),
      telemetry = new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build()),
      loggerFactory = loggerFactory,
    )
  }
}

object ApiPackageManagementServiceSpec {
  private val aSubmissionId = "aSubmission"

  private val anArchive: Archive = {
    val pkg = Ast.GenPackage[Ast.Expr](
      modules = Map.empty,
      directDeps = Set.empty,
      languageVersion = LanguageVersion.default,
      metadata = Some(
        Ast.PackageMetadata(
          Ref.PackageName.assertFromString("aPackage"),
          Ref.PackageVersion.assertFromString("0.0.0"),
          None,
        )
      ),
      isUtilityPackage = true,
    )
    Encode.encodeArchive(
      Ref.PackageId.assertFromString("-pkgId-") -> pkg,
      LanguageVersion.default,
    )
  }

  private final case class TestWritePackagesService(tracer: Tracer)
      extends state.WritePackagesService {
    override def uploadPackages(
        submissionId: Ref.SubmissionId,
        dar: ByteString,
    )(implicit
        traceContext: TraceContext
    ): CompletionStage[state.SubmissionResult] = {
      val telemetryContext = traceContext.toDamlTelemetryContext(tracer)
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      CompletableFuture.completedFuture(state.SubmissionResult.Acknowledged)
    }
  }
}
