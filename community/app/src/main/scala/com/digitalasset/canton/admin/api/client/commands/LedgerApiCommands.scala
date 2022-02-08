// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsServiceStub
import com.daml.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse,
}
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementServiceStub
import com.daml.ledger.api.v1.admin.package_management_service._
import com.daml.ledger.api.v1.admin.participant_pruning_service.ParticipantPruningServiceGrpc.ParticipantPruningServiceStub
import com.daml.ledger.api.v1.admin.participant_pruning_service._
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementServiceStub
import com.daml.ledger.api.v1.admin.party_management_service._
import com.daml.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub
import com.daml.ledger.api.v1.command_completion_service._
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandServiceStub
import com.daml.ledger.api.v1.command_service.{
  CommandServiceGrpc,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest,
}
import com.daml.ledger.api.v1.commands.{Command, Commands => CommandsV1}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationServiceStub
import com.daml.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse,
  LedgerConfiguration,
  LedgerConfigurationServiceGrpc,
}
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityServiceStub
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionServiceStub
import com.daml.ledger.api.v1.transaction_service._
import com.daml.ledger.client.binding.{Primitive => P}
import com.daml.ledger.configuration.LedgerId
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  ServerEnforcedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.CommandCompletionService.GrpcErrorStatus
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedCreatedEvent
import com.digitalasset.canton.config.TimeoutDuration
import com.digitalasset.canton.ledger.api.client.LedgerConnection
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.grpc.{ForwardingStreamObserver, RecordingStreamObserver}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{DiscardOps, LfPartyId}
import com.google.protobuf.empty.Empty
import io.grpc._
import io.grpc.stub.StreamObserver

import java.time.Instant
import java.util.UUID
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.annotation.nowarn
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}

@nowarn(
  "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
) // TODO(i8518) properly deal with deprecation warning
object LedgerApiCommands {

  final val applicationId = "CantonConsole"

  object LedgerIdentityService {
    final case class GetLedgerIdentity()
        extends GrpcAdminCommand[GetLedgerIdentityRequest, GetLedgerIdentityResponse, String] {
      override type Svc = LedgerIdentityServiceStub
      override def createService(channel: ManagedChannel): LedgerIdentityServiceStub =
        LedgerIdentityServiceGrpc.stub(channel)
      override def createRequest(): Either[String, GetLedgerIdentityRequest] =
        Right(GetLedgerIdentityRequest())
      override def submitRequest(
          service: LedgerIdentityServiceStub,
          request: GetLedgerIdentityRequest,
      ): Future[GetLedgerIdentityResponse] =
        service.getLedgerIdentity(request)

      override def handleResponse(response: GetLedgerIdentityResponse): Either[String, String] =
        Right(response.ledgerId)
    }
  }

  object TransactionService {

    trait BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = TransactionServiceStub
      override def createService(channel: ManagedChannel): TransactionServiceStub =
        TransactionServiceGrpc.stub(channel)
    }

    final case class GetLedgerEnd(ledgerId: LedgerId)
        extends BaseCommand[GetLedgerEndRequest, GetLedgerEndResponse, LedgerOffset] {
      override def createRequest(): Either[String, GetLedgerEndRequest] =
        Right(GetLedgerEndRequest(ledgerId = ledgerId))
      override def submitRequest(
          service: TransactionServiceStub,
          request: GetLedgerEndRequest,
      ): Future[GetLedgerEndResponse] =
        service.getLedgerEnd(request)
      override def handleResponse(response: GetLedgerEndResponse): Either[String, LedgerOffset] =
        response.offset.toRight("Received empty response without offset")
    }

    trait SubscribeBase[Resp, Res]
        extends BaseCommand[GetTransactionsRequest, AutoCloseable, AutoCloseable] {
      // The subscription should never be cut short because of a gRPC timeout
      override def timeoutType: TimeoutType = ServerEnforcedTimeout

      def ledgerId: LedgerId

      def observer: StreamObserver[Res]

      def begin: LedgerOffset

      def end: Option[LedgerOffset]

      def filter: TransactionFilter

      def verbose: Boolean

      def doRequest(
          service: TransactionServiceStub,
          request: GetTransactionsRequest,
          rawObserver: StreamObserver[Resp],
      ): Unit

      def extractResults(response: Resp): IterableOnce[Res]

      implicit def loggingContext: ErrorLoggingContext

      override def createRequest(): Either[String, GetTransactionsRequest] = Right {
        GetTransactionsRequest(
          ledgerId = ledgerId,
          begin = Some(begin),
          end = end,
          verbose = verbose,
          filter = Some(filter),
        )
      }

      override def submitRequest(
          service: TransactionServiceStub,
          request: GetTransactionsRequest,
      ): Future[AutoCloseable] = {
        val rawObserver = new ForwardingStreamObserver[Resp, Res](observer, extractResults)
        val context = Context.current().withCancellation()
        context.run(() => doRequest(service, request, rawObserver))
        Future.successful(context)
      }

      override def handleResponse(response: AutoCloseable): Either[String, AutoCloseable] = Right(
        response
      )
    }

    final case class SubscribeTrees(
        override val ledgerId: LedgerId,
        override val observer: StreamObserver[TransactionTree],
        override val begin: LedgerOffset,
        override val end: Option[LedgerOffset],
        override val filter: TransactionFilter,
        override val verbose: Boolean,
    )(override implicit val loggingContext: ErrorLoggingContext)
        extends SubscribeBase[GetTransactionTreesResponse, TransactionTree] {
      override def doRequest(
          service: TransactionServiceStub,
          request: GetTransactionsRequest,
          rawObserver: StreamObserver[GetTransactionTreesResponse],
      ): Unit =
        service.getTransactionTrees(request, rawObserver)

      override def extractResults(
          response: GetTransactionTreesResponse
      ): IterableOnce[TransactionTree] =
        response.transactions
    }

    final case class SubscribeFlat(
        override val ledgerId: LedgerId,
        override val observer: StreamObserver[Transaction],
        override val begin: LedgerOffset,
        override val end: Option[LedgerOffset],
        override val filter: TransactionFilter,
        override val verbose: Boolean,
    )(override implicit val loggingContext: ErrorLoggingContext)
        extends SubscribeBase[GetTransactionsResponse, Transaction] {
      override def doRequest(
          service: TransactionServiceStub,
          request: GetTransactionsRequest,
          rawObserver: StreamObserver[GetTransactionsResponse],
      ): Unit =
        service.getTransactions(request, rawObserver)

      override def extractResults(response: GetTransactionsResponse): IterableOnce[Transaction] =
        response.transactions
    }

    final case class GetTransactionById(ledgerId: LedgerId, parties: Set[LfPartyId], id: String)(
        implicit ec: ExecutionContext
    ) extends BaseCommand[GetTransactionByIdRequest, GetTransactionResponse, Option[
          TransactionTree
        ]]
        with PrettyPrinting {
      override def createRequest(): Either[String, GetTransactionByIdRequest] = Right {
        GetTransactionByIdRequest(
          ledgerId = ledgerId,
          transactionId = id,
          requestingParties = parties.toSeq,
        )
      }

      override def submitRequest(
          service: TransactionServiceStub,
          request: GetTransactionByIdRequest,
      ): Future[GetTransactionResponse] = {
        // The Ledger API will throw an error if it can't find a transaction by ID.
        // However, as Canton is distributed, a transaction ID might show up later, so we don't treat this as
        // an error and change it to a None
        service.getTransactionById(request).recover {
          case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.NOT_FOUND =>
            GetTransactionResponse(None)
        }
      }

      override def handleResponse(
          response: GetTransactionResponse
      ): Either[String, Option[TransactionTree]] =
        Right(response.transaction)

      override def pretty: Pretty[GetTransactionById] =
        prettyOfClass(
          param("id", _.id.unquoted),
          param("ledgerId", _.ledgerId.singleQuoted),
          param("parties", _.parties),
        )
    }

  }

  object PartyManagementService {
    final case class AllocateParty(partyIdHint: String, displayName: String)
        extends GrpcAdminCommand[AllocatePartyRequest, AllocatePartyResponse, PartyDetails] {
      override type Svc = PartyManagementServiceStub
      override def createService(channel: ManagedChannel): PartyManagementServiceStub =
        PartyManagementServiceGrpc.stub(channel)
      override def createRequest(): Either[String, AllocatePartyRequest] =
        Right(AllocatePartyRequest(partyIdHint, displayName))
      override def submitRequest(
          service: PartyManagementServiceStub,
          request: AllocatePartyRequest,
      ): Future[AllocatePartyResponse] =
        service.allocateParty(request)
      override def handleResponse(response: AllocatePartyResponse): Either[String, PartyDetails] =
        response.partyDetails.toRight("Party could not be created")
    }

    final case class ListKnownParties()
        extends GrpcAdminCommand[ListKnownPartiesRequest, ListKnownPartiesResponse, Seq[
          PartyDetails
        ]] {
      override type Svc = PartyManagementServiceStub
      override def createService(channel: ManagedChannel): PartyManagementServiceStub =
        PartyManagementServiceGrpc.stub(channel)
      override def createRequest(): Either[String, ListKnownPartiesRequest] =
        Right(ListKnownPartiesRequest())
      override def submitRequest(
          service: PartyManagementServiceStub,
          request: ListKnownPartiesRequest,
      ): Future[ListKnownPartiesResponse] =
        service.listKnownParties(request)
      override def handleResponse(
          response: ListKnownPartiesResponse
      ): Either[String, Seq[PartyDetails]] =
        Right(response.partyDetails)
    }
  }

  object PackageService {

    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = PackageManagementServiceStub
      override def createService(channel: ManagedChannel): PackageManagementServiceStub =
        PackageManagementServiceGrpc.stub(channel)
    }

    final case class UploadDarFile(darPath: String)
        extends BaseCommand[UploadDarFileRequest, UploadDarFileResponse, Unit] {

      override def createRequest(): Either[String, UploadDarFileRequest] =
        for {
          bytes <- BinaryFileUtil.readByteStringFromFile(darPath)
        } yield UploadDarFileRequest(bytes)
      override def submitRequest(
          service: PackageManagementServiceStub,
          request: UploadDarFileRequest,
      ): Future[UploadDarFileResponse] =
        service.uploadDarFile(request)
      override def handleResponse(response: UploadDarFileResponse): Either[String, Unit] =
        Right(())

      // package upload time might take long if it is a big package
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ListKnownPackages(limit: Option[Int])
        extends BaseCommand[ListKnownPackagesRequest, ListKnownPackagesResponse, Seq[
          PackageDetails
        ]] {

      override def createRequest(): Either[String, ListKnownPackagesRequest] = Right(
        ListKnownPackagesRequest()
      )

      override def submitRequest(
          service: PackageManagementServiceStub,
          request: ListKnownPackagesRequest,
      ): Future[ListKnownPackagesResponse] =
        service.listKnownPackages(request)

      override def handleResponse(
          response: ListKnownPackagesResponse
      ): Either[String, Seq[PackageDetails]] =
        Right(response.packageDetails.take(limit.getOrElse(Int.MaxValue)))
    }

  }

  object CommandCompletionService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = CommandCompletionServiceStub
      override def createService(channel: ManagedChannel): CommandCompletionServiceStub =
        CommandCompletionServiceGrpc.stub(channel)
    }

    final case class CompletionEnd(ledgerId: LedgerId)
        extends BaseCommand[CompletionEndRequest, CompletionEndResponse, LedgerOffset] {

      override def createRequest(): Either[String, CompletionEndRequest] =
        Right(CompletionEndRequest(ledgerId = ledgerId))

      override def submitRequest(
          service: CommandCompletionServiceStub,
          request: CompletionEndRequest,
      ): Future[CompletionEndResponse] =
        service.completionEnd(request)
      override def handleResponse(response: CompletionEndResponse): Either[String, LedgerOffset] =
        response.offset.toRight("Empty CompletionEndResponse received without offset")
    }

    final case class CompletionRequest(
        ledgerId: LedgerId,
        partyId: LfPartyId,
        offset: LedgerOffset,
        expectedCompletions: Int,
        timeout: java.time.Duration,
        applicationId: String,
    )(filter: Completion => Boolean, scheduler: ScheduledExecutorService)
        extends BaseCommand[CompletionStreamRequest, Seq[Completion], Seq[Completion]] {

      override def createRequest(): Either[String, CompletionStreamRequest] =
        Right(
          CompletionStreamRequest(
            ledgerId = ledgerId,
            applicationId = applicationId,
            parties = Seq(partyId),
            offset = Some(offset),
          )
        )

      override def submitRequest(
          service: CommandCompletionServiceStub,
          request: CompletionStreamRequest,
      ): Future[Seq[Completion]] = {
        import scala.jdk.DurationConverters._
        streamedResponse[CompletionStreamRequest, CompletionStreamResponse, Completion](
          service.completionStream,
          _.completions.filter(filter),
          request,
          expectedCompletions,
          timeout.toScala,
          scheduler,
        )
      }

      override def handleResponse(response: Seq[Completion]): Either[String, Seq[Completion]] =
        Right(response)

      override def timeoutType: TimeoutType = ServerEnforcedTimeout
    }

    final case class CompletionCheckpointRequest(
        ledgerId: LedgerId,
        partyId: LfPartyId,
        offset: LedgerOffset,
        expectedCompletions: Int,
        timeout: TimeoutDuration,
        applicationId: String,
    )(filter: Completion => Boolean, scheduler: ScheduledExecutorService)
        extends BaseCommand[CompletionStreamRequest, Seq[(Completion, Option[Checkpoint])], Seq[
          (Completion, Option[Checkpoint])
        ]] {

      override def createRequest(): Either[String, CompletionStreamRequest] =
        Right(
          CompletionStreamRequest(
            ledgerId = ledgerId,
            applicationId = applicationId,
            parties = Seq(partyId),
            offset = Some(offset),
          )
        )

      override def submitRequest(
          service: CommandCompletionServiceStub,
          request: CompletionStreamRequest,
      ): Future[Seq[(Completion, Option[Checkpoint])]] = {
        def extract(response: CompletionStreamResponse): Seq[(Completion, Option[Checkpoint])] = {
          val checkpoint = response.checkpoint
          response.completions.filter(filter).map(_ -> checkpoint)
        }

        streamedResponse[
          CompletionStreamRequest,
          CompletionStreamResponse,
          (Completion, Option[Checkpoint]),
        ](
          service.completionStream,
          extract,
          request,
          expectedCompletions,
          timeout.asFiniteApproximation,
          scheduler,
        )
      }

      override def handleResponse(
          response: Seq[(Completion, Option[Checkpoint])]
      ): Either[String, Seq[(Completion, Option[Checkpoint])]] =
        Right(response)

      override def timeoutType: TimeoutType = ServerEnforcedTimeout
    }

    object GrpcErrorStatus {
      def unapply(ex: Throwable): Option[Status] = ex match {
        case e: StatusException => Some(e.getStatus)
        case re: StatusRuntimeException => Some(re.getStatus)
        case _ => None
      }
    }

  }

  object LedgerConfigurationService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = LedgerConfigurationServiceStub
      override def createService(channel: ManagedChannel): LedgerConfigurationServiceStub =
        LedgerConfigurationServiceGrpc.stub(channel)
    }

    final case class GetLedgerConfiguration(
        ledgerId: LedgerId,
        expectedConfigs: Int,
        timeout: FiniteDuration,
    )(scheduler: ScheduledExecutorService)
        extends BaseCommand[GetLedgerConfigurationRequest, Seq[LedgerConfiguration], Seq[
          LedgerConfiguration
        ]] {

      override def createRequest(): Either[String, GetLedgerConfigurationRequest] =
        Right(GetLedgerConfigurationRequest(ledgerId = ledgerId))

      override def submitRequest(
          service: LedgerConfigurationServiceStub,
          request: GetLedgerConfigurationRequest,
      ): Future[Seq[LedgerConfiguration]] =
        streamedResponse[
          GetLedgerConfigurationRequest,
          GetLedgerConfigurationResponse,
          LedgerConfiguration,
        ](
          service.getLedgerConfiguration,
          _.ledgerConfiguration.toList,
          request,
          expectedConfigs,
          timeout,
          scheduler,
        )

      override def handleResponse(
          response: Seq[LedgerConfiguration]
      ): Either[String, Seq[LedgerConfiguration]] =
        Right(response)

      override def timeoutType: TimeoutType = ServerEnforcedTimeout
    }
  }

  private[commands] trait SubmitCommand extends PrettyPrinting {
    def ledgerId: LedgerId
    def actAs: Seq[LfPartyId]
    def commands: Seq[Command]
    def workflowId: String
    def commandId: String
    def deduplicationPeriod: Option[DeduplicationPeriod]
    def submissionId: String
    def minLedgerTimeAbs: Option[Instant]

    protected def mkCommand: CommandsV1 = CommandsV1(
      ledgerId = ledgerId,
      workflowId = workflowId,
      applicationId = applicationId,
      commandId = if (commandId.isEmpty) UUID.randomUUID().toString else commandId,
      actAs = actAs,
      commands = commands,
      deduplicationPeriod = deduplicationPeriod.fold(
        CommandsV1.DeduplicationPeriod.Empty: CommandsV1.DeduplicationPeriod
      ) {
        case DeduplicationPeriod.DeduplicationDuration(duration) =>
          CommandsV1.DeduplicationPeriod.DeduplicationDuration(
            ProtoConverter.DurationConverter.toProtoPrimitive(duration)
          )
        case DeduplicationPeriod.DeduplicationOffset(offset) =>
          CommandsV1.DeduplicationPeriod.DeduplicationOffset(
            offset.toHexString
          )
      },
      minLedgerTimeAbs =
        minLedgerTimeAbs.map(t => ProtoConverter.InstantConverter.toProtoPrimitive(t)),
      submissionId = submissionId,
    )

    override def pretty: Pretty[this.type] =
      prettyOfClass(
        param("actAs", _.actAs),
        param("commandId", _.commandId.singleQuoted),
        param("workflowId", _.workflowId.singleQuoted),
        param("submissionId", _.submissionId.singleQuoted),
        param("deduplicationPeriod", _.deduplicationPeriod),
        param("ledgerId", _.ledgerId.singleQuoted),
        paramIfDefined("minLedgerTimeAbs", _.minLedgerTimeAbs),
        paramWithoutValue("commands"),
      )
  }

  object CommandSubmissionService {
    trait BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = CommandSubmissionServiceStub
      override def createService(channel: ManagedChannel): CommandSubmissionServiceStub =
        CommandSubmissionServiceGrpc.stub(channel)
    }

    final case class Submit(
        override val ledgerId: LedgerId,
        override val actAs: Seq[LfPartyId],
        override val commands: Seq[Command],
        override val workflowId: String,
        override val commandId: String,
        override val deduplicationPeriod: Option[DeduplicationPeriod],
        override val submissionId: String,
        override val minLedgerTimeAbs: Option[Instant],
    ) extends SubmitCommand
        with BaseCommand[SubmitRequest, Empty, Unit] {
      override def createRequest(): Either[String, SubmitRequest] = Right(
        SubmitRequest(commands = Some(mkCommand))
      )

      override def submitRequest(
          service: CommandSubmissionServiceStub,
          request: SubmitRequest,
      ): Future[Empty] = {
        service.submit(request)
      }

      override def handleResponse(response: Empty): Either[String, Unit] = Right(())
    }
  }

  object CommandService {
    trait BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = CommandServiceStub
      override def createService(channel: ManagedChannel): CommandServiceStub =
        CommandServiceGrpc.stub(channel)
    }

    final case class SubmitAndWaitTransactionTree(
        override val ledgerId: LedgerId,
        override val actAs: Seq[LfPartyId],
        override val commands: Seq[Command],
        override val workflowId: String,
        override val commandId: String,
        override val deduplicationPeriod: Option[DeduplicationPeriod],
        override val submissionId: String,
        override val minLedgerTimeAbs: Option[Instant],
    ) extends SubmitCommand
        with BaseCommand[
          SubmitAndWaitRequest,
          SubmitAndWaitForTransactionTreeResponse,
          TransactionTree,
        ] {

      override def createRequest(): Either[String, SubmitAndWaitRequest] =
        Right(SubmitAndWaitRequest(commands = Some(mkCommand)))

      override def submitRequest(
          service: CommandServiceStub,
          request: SubmitAndWaitRequest,
      ): Future[SubmitAndWaitForTransactionTreeResponse] =
        service.submitAndWaitForTransactionTree(request)

      override def handleResponse(
          response: SubmitAndWaitForTransactionTreeResponse
      ): Either[String, TransactionTree] =
        response.transaction.toRight("Received response without any transaction tree")

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class SubmitAndWaitTransaction(
        override val ledgerId: LedgerId,
        override val actAs: Seq[LfPartyId],
        override val commands: Seq[Command],
        override val workflowId: String,
        override val commandId: String,
        override val deduplicationPeriod: Option[DeduplicationPeriod],
        override val submissionId: String,
        override val minLedgerTimeAbs: Option[Instant],
    ) extends SubmitCommand
        with BaseCommand[SubmitAndWaitRequest, SubmitAndWaitForTransactionResponse, Transaction] {

      override def createRequest(): Either[String, SubmitAndWaitRequest] =
        Right(SubmitAndWaitRequest(commands = Some(mkCommand)))

      override def submitRequest(
          service: CommandServiceStub,
          request: SubmitAndWaitRequest,
      ): Future[SubmitAndWaitForTransactionResponse] =
        service.submitAndWaitForTransaction(request)

      override def handleResponse(
          response: SubmitAndWaitForTransactionResponse
      ): Either[String, Transaction] =
        response.transaction.toRight("Received response without any transaction")

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }
  }

  object AcsService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = ActiveContractsServiceStub
      override def createService(channel: ManagedChannel): ActiveContractsServiceStub =
        ActiveContractsServiceGrpc.stub(channel)
    }

    final case class GetActiveContracts(
        ledgerId: LedgerId,
        parties: Set[LfPartyId],
        limit: Option[Int] = None,
        templateFilter: Seq[P.TemplateId[_]] = Seq.empty,
        verbose: Boolean = true,
    ) extends BaseCommand[GetActiveContractsRequest, Seq[GetActiveContractsResponse], Seq[
          WrappedCreatedEvent
        ]] {

      override def createRequest(): Either[String, GetActiveContractsRequest] = {
        import scalaz.syntax.tag._
        val filter =
          if (templateFilter.nonEmpty) {
            Filters(
              Some(
                InclusiveFilters(templateFilter.map(LedgerConnection.mapTemplateIds).map(_.unwrap))
              )
            )
          } else Filters.defaultInstance
        Right(
          GetActiveContractsRequest(
            ledgerId = ledgerId,
            filter = Some(TransactionFilter(parties.map((_, filter)).toMap)),
            verbose = verbose,
          )
        )
      }

      override def submitRequest(
          service: ActiveContractsServiceStub,
          request: GetActiveContractsRequest,
      ): Future[Seq[GetActiveContractsResponse]] = {
        val promise = Promise[Seq[GetActiveContractsResponse]]()

        val observer =
          new RecordingStreamObserver[GetActiveContractsResponse](limit.getOrElse(Int.MaxValue)) {
            override def onCompleted(): Unit = promise.success(this.responses)
          }

        service.getActiveContracts(
          request,
          observer,
        )
        promise.future
      }

      override def handleResponse(
          response: Seq[GetActiveContractsResponse]
      ): Either[String, Seq[WrappedCreatedEvent]] = {
        Right(response.flatMap(_.activeContracts).map(WrappedCreatedEvent))
      }

      // fetching ACS might take long if we fetch a lot of data
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

  }

  object ParticipantPruningService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = ParticipantPruningServiceStub
      override def createService(channel: ManagedChannel): ParticipantPruningServiceStub =
        ParticipantPruningServiceGrpc.stub(channel)

      // all pruning commands will take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class Prune(pruneUpTo: LedgerOffset)
        extends BaseCommand[PruneRequest, PruneResponse, Unit] {

      override def timeoutType: TimeoutType =
        DefaultUnboundedTimeout // pruning can take a very long time

      override def createRequest(): Either[String, PruneRequest] =
        pruneUpTo.value.absolute
          .toRight("The pruneUpTo ledger offset needs to be absolute")
          .map(
            PruneRequest(
              _,
              // canton always prunes divulged contracts both in the ledger api index-db and in canton stores
              pruneAllDivulgedContracts = true,
            )
          )

      override def submitRequest(
          service: ParticipantPruningServiceStub,
          request: PruneRequest,
      ): Future[PruneResponse] =
        service.prune(request)

      override def handleResponse(response: PruneResponse): Either[String, Unit] = Right(())
    }
  }

  private def streamedResponse[Request, Response, Result](
      service: (Request, StreamObserver[Response]) => Unit,
      extract: Response => Seq[Result],
      request: Request,
      expected: Int,
      timeout: FiniteDuration,
      scheduler: ScheduledExecutorService,
  ): Future[Seq[Result]] = {
    val promise = Promise[Seq[Result]]()
    val buffer = ListBuffer[Result]()
    val context = Context.ROOT.withCancellation()
    def success(): Unit = buffer.synchronized {
      context.close()
      promise.trySuccess(buffer.toList).discard[Boolean]
    }

    context.run(() =>
      service(
        request,
        new StreamObserver[Response]() {
          override def onNext(value: Response): Unit = {
            val extracted = extract(value)
            buffer.synchronized {
              if (buffer.lengthCompare(expected) < 0) {
                buffer ++= extracted
                if (buffer.lengthCompare(expected) >= 0) {
                  success()
                }
              }
            }
          }

          override def onError(t: Throwable): Unit = {
            t match {
              case GrpcErrorStatus(status) if status.getCode == Status.CANCELLED.getCode =>
                success()
              case _ =>
                val _ = promise.tryFailure(t)
            }
          }

          override def onCompleted(): Unit = {
            success()
          }
        },
      )
    )
    scheduler.schedule(
      new Runnable() {
        override def run(): Unit = {
          val _ = context.cancel(Status.CANCELLED.asException())
        }
      },
      timeout.toMillis,
      TimeUnit.MILLISECONDS,
    )
    promise.future
  }
}
