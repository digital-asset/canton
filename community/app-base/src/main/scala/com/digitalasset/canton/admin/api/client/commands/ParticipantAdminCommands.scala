// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.implicits.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  ServerEnforcedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.{
  DarMetadata,
  InFlightCount,
  ListConnectedDomainsResult,
  NodeStatus,
  ParticipantPruningSchedule,
  ParticipantStatus,
}
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.admin.participant.v30.DomainConnectivityServiceGrpc.DomainConnectivityServiceStub
import com.digitalasset.canton.admin.participant.v30.EnterpriseParticipantReplicationServiceGrpc.EnterpriseParticipantReplicationServiceStub
import com.digitalasset.canton.admin.participant.v30.InspectionServiceGrpc.InspectionServiceStub
import com.digitalasset.canton.admin.participant.v30.PackageServiceGrpc.PackageServiceStub
import com.digitalasset.canton.admin.participant.v30.ParticipantRepairServiceGrpc.ParticipantRepairServiceStub
import com.digitalasset.canton.admin.participant.v30.ParticipantStatusServiceGrpc.ParticipantStatusServiceStub
import com.digitalasset.canton.admin.participant.v30.PartyManagementServiceGrpc.PartyManagementServiceStub
import com.digitalasset.canton.admin.participant.v30.PingServiceGrpc.PingServiceStub
import com.digitalasset.canton.admin.participant.v30.PruningServiceGrpc.PruningServiceStub
import com.digitalasset.canton.admin.participant.v30.ResourceManagementServiceGrpc.ResourceManagementServiceStub
import com.digitalasset.canton.admin.participant.v30.{ResourceLimits as _, *}
import com.digitalasset.canton.admin.pruning
import com.digitalasset.canton.admin.pruning.v30.WaitCommitmentsSetup
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.participant.admin.traffic.TrafficStateAdmin
import com.digitalasset.canton.participant.domain.DomainConnectionConfig as CDomainConnectionConfig
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.{
  ReceivedCmtState,
  SentCmtState,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.protocol.messages.{AcsCommitment, CommitmentPeriod}
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{BinaryFileUtil, GrpcStreamingUtils}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SequencerCounter, SynchronizerAlias, config}
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ManagedChannel}

import java.io.IOException
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MILLISECONDS}

object ParticipantAdminCommands {

  /** Daml Package Management Commands
    */
  object Package {

    sealed trait PackageCommand[Req, Res, Result] extends GrpcAdminCommand[Req, Res, Result] {
      override type Svc = PackageServiceStub

      override def createService(channel: ManagedChannel): PackageServiceStub =
        PackageServiceGrpc.stub(channel)
    }

    final case class List(limit: PositiveInt)
        extends PackageCommand[ListPackagesRequest, ListPackagesResponse, Seq[PackageDescription]] {
      override protected def createRequest() = Right(ListPackagesRequest(limit.value))

      override protected def submitRequest(
          service: PackageServiceStub,
          request: ListPackagesRequest,
      ): Future[ListPackagesResponse] =
        service.listPackages(request)

      override protected def handleResponse(
          response: ListPackagesResponse
      ): Either[String, Seq[PackageDescription]] =
        Right(response.packageDescriptions)
    }

    final case class ListContents(packageId: String)
        extends PackageCommand[ListPackageContentsRequest, ListPackageContentsResponse, Seq[
          ModuleDescription
        ]] {
      override protected def createRequest() = Right(ListPackageContentsRequest(packageId))

      override protected def submitRequest(
          service: PackageServiceStub,
          request: ListPackageContentsRequest,
      ): Future[ListPackageContentsResponse] =
        service.listPackageContents(request)

      override protected def handleResponse(
          response: ListPackageContentsResponse
      ): Either[String, Seq[ModuleDescription]] =
        Right(response.modules)
    }

    final case class UploadDar(
        darPath: Option[String],
        vetAllPackages: Boolean,
        synchronizeVetting: Boolean,
        logger: TracedLogger,
    ) extends PackageCommand[UploadDarRequest, UploadDarResponse, String] {

      override protected def createRequest(): Either[String, UploadDarRequest] =
        for {
          pathValue <- darPath.toRight("DAR path not provided")
          nonEmptyPathValue <- Either.cond(
            pathValue.nonEmpty,
            pathValue,
            "Provided DAR path is empty",
          )
          filename = Paths.get(nonEmptyPathValue).getFileName.toString
          darData <- BinaryFileUtil.readByteStringFromFile(nonEmptyPathValue)
        } yield UploadDarRequest(
          darData,
          filename,
          vetAllPackages = vetAllPackages,
          synchronizeVetting = synchronizeVetting,
        )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: UploadDarRequest,
      ): Future[UploadDarResponse] =
        service.uploadDar(request)

      override protected def handleResponse(response: UploadDarResponse): Either[String, String] =
        response.value match {
          case UploadDarResponse.Value.Success(UploadDarResponse.Success(hash)) => Right(hash)
          case UploadDarResponse.Value.Failure(UploadDarResponse.Failure(msg)) => Left(msg)
          case UploadDarResponse.Value.Empty => Left("unexpected empty response")
        }

      // file can be big. checking & vetting might take a while
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ValidateDar(
        darPath: Option[String],
        logger: TracedLogger,
    ) extends PackageCommand[ValidateDarRequest, ValidateDarResponse, String] {

      override protected def createRequest(): Either[String, ValidateDarRequest] =
        for {
          pathValue <- darPath.toRight("DAR path not provided")
          nonEmptyPathValue <- Either.cond(
            pathValue.nonEmpty,
            pathValue,
            "Provided DAR path is empty",
          )
          filename = Paths.get(nonEmptyPathValue).getFileName.toString
          darData <- BinaryFileUtil.readByteStringFromFile(nonEmptyPathValue)
        } yield ValidateDarRequest(
          darData,
          filename,
        )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: ValidateDarRequest,
      ): Future[ValidateDarResponse] =
        service.validateDar(request)

      override protected def handleResponse(response: ValidateDarResponse): Either[String, String] =
        response match {
          case ValidateDarResponse(hash) => Right(hash)
        }

      // file can be big. checking & vetting might take a while
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class RemovePackage(
        packageId: String,
        force: Boolean,
    ) extends PackageCommand[RemovePackageRequest, RemovePackageResponse, Unit] {

      override protected def createRequest() = Right(RemovePackageRequest(packageId, force))

      override protected def submitRequest(
          service: PackageServiceStub,
          request: RemovePackageRequest,
      ): Future[RemovePackageResponse] =
        service.removePackage(request)

      override protected def handleResponse(
          response: RemovePackageResponse
      ): Either[String, Unit] =
        response.success match {
          case None => Left("unexpected empty response")
          case Some(_success) => Right(())
        }

    }

    final case class GetDar(
        darHash: Option[String],
        destinationDirectory: Option[String],
        logger: TracedLogger,
    ) extends PackageCommand[GetDarRequest, GetDarResponse, Path] {
      override protected def createRequest(): Either[String, GetDarRequest] =
        for {
          _ <- destinationDirectory.toRight("DAR destination directory not provided")
          hash <- darHash.toRight("DAR hash not provided")
        } yield GetDarRequest(hash)

      override protected def submitRequest(
          service: PackageServiceStub,
          request: GetDarRequest,
      ): Future[GetDarResponse] =
        service.getDar(request)

      override protected def handleResponse(response: GetDarResponse): Either[String, Path] =
        for {
          directory <- destinationDirectory.toRight("DAR directory not provided")
          data <- if (response.data.isEmpty) Left("DAR was not found") else Right(response.data)
          path <-
            try {
              val path = Paths.get(directory, s"${response.name}.dar")
              Files.write(path, data.toByteArray)
              Right(path)
            } catch {
              case ex: IOException =>
                // the trace context for admin commands is started by the submit-request call
                // however we can't get at it here
                logger.debug(s"Error saving DAR to $directory: $ex")(TraceContext.empty)
                Left(s"Error saving DAR to $directory")
            }
        } yield path

      // might be a big file to download
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ListDarContents(darId: String)
        extends PackageCommand[ListDarContentsRequest, ListDarContentsResponse, DarMetadata] {
      override protected def createRequest() = Right(ListDarContentsRequest(darId))

      override protected def submitRequest(
          service: PackageServiceStub,
          request: ListDarContentsRequest,
      ): Future[ListDarContentsResponse] =
        service.listDarContents(request)

      override protected def handleResponse(
          response: ListDarContentsResponse
      ): Either[String, DarMetadata] =
        DarMetadata.fromProtoV30(response).leftMap(_.toString)

    }

    final case class RemoveDar(
        darHash: String
    ) extends PackageCommand[RemoveDarRequest, RemoveDarResponse, Unit] {

      override protected def createRequest(): Either[String, RemoveDarRequest] = Right(
        RemoveDarRequest(darHash)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: RemoveDarRequest,
      ): Future[RemoveDarResponse] =
        service.removeDar(request)

      override protected def handleResponse(
          response: RemoveDarResponse
      ): Either[String, Unit] =
        response.success match {
          case None => Left("unexpected empty response")
          case Some(success) => Right(())
        }

    }

    final case class VetDar(darDash: String, synchronize: Boolean)
        extends PackageCommand[VetDarRequest, VetDarResponse, Unit] {
      override protected def createRequest(): Either[String, VetDarRequest] = Right(
        VetDarRequest(darDash, synchronize)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: VetDarRequest,
      ): Future[VetDarResponse] = service.vetDar(request)

      override protected def handleResponse(response: VetDarResponse): Either[String, Unit] =
        Either.unit
    }

    // TODO(#14432): Add `synchronize` flag which makes the call block until the unvetting operation
    //               is observed by the participant on all connected domains.
    final case class UnvetDar(darDash: String)
        extends PackageCommand[UnvetDarRequest, UnvetDarResponse, Unit] {

      override protected def createRequest(): Either[String, UnvetDarRequest] = Right(
        UnvetDarRequest(darDash)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: UnvetDarRequest,
      ): Future[UnvetDarResponse] = service.unvetDar(request)

      override protected def handleResponse(response: UnvetDarResponse): Either[String, Unit] =
        Either.unit
    }

    final case class ListDars(limit: PositiveInt)
        extends PackageCommand[ListDarsRequest, ListDarsResponse, Seq[DarDescription]] {
      override protected def createRequest(): Either[String, ListDarsRequest] = Right(
        ListDarsRequest(limit.value)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: ListDarsRequest,
      ): Future[ListDarsResponse] =
        service.listDars(request)

      override protected def handleResponse(
          response: ListDarsResponse
      ): Either[String, Seq[DarDescription]] =
        Right(response.dars)
    }

  }

  object PartyManagement {

    final case class StartPartyReplication(
        id: Option[String],
        party: PartyId,
        sourceParticipant: ParticipantId,
        synchronizerId: SynchronizerId,
    ) extends GrpcAdminCommand[StartPartyReplicationRequest, StartPartyReplicationResponse, Unit] {
      override type Svc = PartyManagementServiceStub

      override def createService(channel: ManagedChannel): PartyManagementServiceStub =
        PartyManagementServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, StartPartyReplicationRequest] =
        Right(
          StartPartyReplicationRequest(
            id = id,
            partyUid = party.uid.toProtoPrimitive,
            sourceParticipantUid = sourceParticipant.uid.toProtoPrimitive,
            domainUid = synchronizerId.toProtoPrimitive,
          )
        )

      override protected def submitRequest(
          service: PartyManagementServiceStub,
          request: StartPartyReplicationRequest,
      ): Future[StartPartyReplicationResponse] =
        service.startPartyReplication(request)

      override protected def handleResponse(
          response: StartPartyReplicationResponse
      ): Either[String, Unit] =
        Either.unit
    }
  }

  object ParticipantRepairManagement {

    final case class ExportAcs(
        parties: Set[PartyId],
        partiesOffboarding: Boolean,
        filterSynchronizerId: Option[SynchronizerId],
        timestamp: Option[Instant],
        observer: StreamObserver[ExportAcsResponse],
        contractDomainRenames: Map[SynchronizerId, (SynchronizerId, ProtocolVersion)],
        force: Boolean,
    ) extends GrpcAdminCommand[
          ExportAcsRequest,
          CancellableContext,
          CancellableContext,
        ] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, ExportAcsRequest] =
        Right(
          ExportAcsRequest(
            parties.map(_.toLf).toSeq,
            filterSynchronizerId.map(_.toProtoPrimitive).getOrElse(""),
            timestamp.map(Timestamp.apply),
            contractDomainRenames.map {
              case (source, (targetSynchronizerId, targetProtocolVersion)) =>
                val targetDomain = ExportAcsRequest.TargetDomain(
                  synchronizerId = targetSynchronizerId.toProtoPrimitive,
                  protocolVersion = targetProtocolVersion.toProtoPrimitive,
                )

                (source.toProtoPrimitive, targetDomain)
            },
            force = force,
            partiesOffboarding = partiesOffboarding,
          )
        )

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: ExportAcsRequest,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.exportAcs(request, observer))
        Future.successful(context)
      }

      override protected def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] = Right(response)

      override def timeoutType: GrpcAdminCommand.TimeoutType =
        GrpcAdminCommand.DefaultUnboundedTimeout
    }

    final case class ImportAcs(
        acsChunk: ByteString,
        workflowIdPrefix: String,
        allowContractIdSuffixRecomputation: Boolean,
    ) extends GrpcAdminCommand[
          ImportAcsRequest,
          ImportAcsResponse,
          Map[LfContractId, LfContractId],
        ] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, ImportAcsRequest] =
        Right(
          ImportAcsRequest(
            acsChunk,
            workflowIdPrefix,
            allowContractIdSuffixRecomputation,
          )
        )

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: ImportAcsRequest,
      ): Future[ImportAcsResponse] =
        GrpcStreamingUtils.streamToServer(
          service.importAcs,
          (bytes: Array[Byte]) =>
            ImportAcsRequest(
              ByteString.copyFrom(bytes),
              workflowIdPrefix,
              allowContractIdSuffixRecomputation,
            ),
          request.acsSnapshot,
        )

      override protected def handleResponse(
          response: ImportAcsResponse
      ): Either[String, Map[LfContractId, LfContractId]] =
        response.contractIdMapping.toSeq
          .traverse { case (oldCid, newCid) =>
            for {
              oldCidParsed <- LfContractId.fromString(oldCid)
              newCidParsed <- LfContractId.fromString(newCid)
            } yield oldCidParsed -> newCidParsed
          }
          .map(_.toMap)
    }

    final case class PurgeContracts(
        synchronizerAlias: SynchronizerAlias,
        contracts: Seq[LfContractId],
        ignoreAlreadyPurged: Boolean,
    ) extends GrpcAdminCommand[PurgeContractsRequest, PurgeContractsResponse, Unit] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, PurgeContractsRequest] =
        Right(
          PurgeContractsRequest(
            synchronizerAlias = synchronizerAlias.toProtoPrimitive,
            contractIds = contracts.map(_.coid),
            ignoreAlreadyPurged = ignoreAlreadyPurged,
          )
        )

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: PurgeContractsRequest,
      ): Future[PurgeContractsResponse] = service.purgeContracts(request)

      override protected def handleResponse(
          response: PurgeContractsResponse
      ): Either[String, Unit] =
        Either.unit
    }

    final case class MigrateDomain(
        sourceSynchronizerAlias: SynchronizerAlias,
        targetDomainConfig: CDomainConnectionConfig,
        force: Boolean,
    ) extends GrpcAdminCommand[MigrateDomainRequest, MigrateDomainResponse, Unit] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: MigrateDomainRequest,
      ): Future[MigrateDomainResponse] = service.migrateDomain(request)

      override protected def createRequest(): Either[String, MigrateDomainRequest] =
        Right(
          MigrateDomainRequest(
            sourceSynchronizerAlias.toProtoPrimitive,
            Some(targetDomainConfig.toProtoV30),
            force = force,
          )
        )

      override protected def handleResponse(response: MigrateDomainResponse): Either[String, Unit] =
        Either.unit

      // migration command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class PurgeDeactivatedDomain(synchronizerAlias: SynchronizerAlias)
        extends GrpcAdminCommand[
          PurgeDeactivatedDomainRequest,
          PurgeDeactivatedDomainResponse,
          Unit,
        ] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: PurgeDeactivatedDomainRequest,
      ): Future[PurgeDeactivatedDomainResponse] =
        service.purgeDeactivatedDomain(request)

      override protected def createRequest(): Either[String, PurgeDeactivatedDomainRequest] =
        Right(PurgeDeactivatedDomainRequest(synchronizerAlias.toProtoPrimitive))

      override protected def handleResponse(
          response: PurgeDeactivatedDomainResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class IgnoreEvents(
        synchronizerId: SynchronizerId,
        fromInclusive: SequencerCounter,
        toInclusive: SequencerCounter,
        force: Boolean,
    ) extends GrpcAdminCommand[
          IgnoreEventsRequest,
          IgnoreEventsResponse,
          Unit,
        ] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: IgnoreEventsRequest,
      ): Future[IgnoreEventsResponse] =
        service.ignoreEvents(request)

      override protected def createRequest(): Either[String, IgnoreEventsRequest] =
        Right(
          IgnoreEventsRequest(
            synchronizerId = synchronizerId.toProtoPrimitive,
            fromInclusive = fromInclusive.toProtoPrimitive,
            toInclusive = toInclusive.toProtoPrimitive,
            force = force,
          )
        )

      override protected def handleResponse(response: IgnoreEventsResponse): Either[String, Unit] =
        Either.unit
    }

    final case class UnignoreEvents(
        synchronizerId: SynchronizerId,
        fromInclusive: SequencerCounter,
        toInclusive: SequencerCounter,
        force: Boolean,
    ) extends GrpcAdminCommand[
          UnignoreEventsRequest,
          UnignoreEventsResponse,
          Unit,
        ] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: UnignoreEventsRequest,
      ): Future[UnignoreEventsResponse] =
        service.unignoreEvents(request)

      override protected def createRequest(): Either[String, UnignoreEventsRequest] =
        Right(
          UnignoreEventsRequest(
            synchronizerId = synchronizerId.toProtoPrimitive,
            fromInclusive = fromInclusive.toProtoPrimitive,
            toInclusive = toInclusive.toProtoPrimitive,
            force = force,
          )
        )

      override protected def handleResponse(
          response: UnignoreEventsResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class RollbackUnassignment(
        unassignId: String,
        source: SynchronizerId,
        target: SynchronizerId,
    ) extends GrpcAdminCommand[RollbackUnassignmentRequest, RollbackUnassignmentResponse, Unit] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, RollbackUnassignmentRequest] =
        Right(
          RollbackUnassignmentRequest(
            unassignId = unassignId,
            source = source.toProtoPrimitive,
            target = target.toProtoPrimitive,
          )
        )

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: RollbackUnassignmentRequest,
      ): Future[RollbackUnassignmentResponse] =
        service.rollbackUnassignment(request)

      override protected def handleResponse(
          response: RollbackUnassignmentResponse
      ): Either[String, Unit] = Either.unit
    }
  }

  object Ping {

    final case class Ping(
        targets: Set[String],
        validators: Set[String],
        timeout: config.NonNegativeDuration,
        levels: Int,
        synchronizerId: Option[SynchronizerId],
        workflowId: String,
        id: String,
    ) extends GrpcAdminCommand[PingRequest, PingResponse, Either[String, Duration]] {
      override type Svc = PingServiceStub

      override def createService(channel: ManagedChannel): PingServiceStub =
        PingServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, PingRequest] =
        Right(
          PingRequest(
            targets.toSeq,
            validators.toSeq,
            Some(timeout.toProtoPrimitive),
            levels,
            synchronizerId.map(_.toProtoPrimitive).getOrElse(""),
            workflowId,
            id,
          )
        )

      override protected def submitRequest(
          service: PingServiceStub,
          request: PingRequest,
      ): Future[PingResponse] =
        service.ping(request)

      override protected def handleResponse(
          response: PingResponse
      ): Either[String, Either[String, Duration]] =
        response.response match {
          case PingResponse.Response.Success(PingSuccess(pingTime, responder)) =>
            Right(Right(Duration(pingTime, MILLISECONDS)))
          case PingResponse.Response.Failure(failure) => Right(Left(failure.reason))
          case PingResponse.Response.Empty => Left("Ping client: unexpected empty response")
        }

      override def timeoutType: TimeoutType = ServerEnforcedTimeout
    }

  }

  object DomainConnectivity {

    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = DomainConnectivityServiceStub

      override def createService(channel: ManagedChannel): DomainConnectivityServiceStub =
        DomainConnectivityServiceGrpc.stub(channel)
    }

    final case class ReconnectDomains(ignoreFailures: Boolean)
        extends Base[ReconnectDomainsRequest, ReconnectDomainsResponse, Unit] {

      override protected def createRequest(): Either[String, ReconnectDomainsRequest] =
        Right(ReconnectDomainsRequest(ignoreFailures = ignoreFailures))

      override protected def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ReconnectDomainsRequest,
      ): Future[ReconnectDomainsResponse] =
        service.reconnectDomains(request)

      override protected def handleResponse(
          response: ReconnectDomainsResponse
      ): Either[String, Unit] = Either.unit

      // depending on the confirmation timeout and the load, this might take a bit longer
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ReconnectDomain(synchronizerAlias: SynchronizerAlias, retry: Boolean)
        extends Base[ReconnectDomainRequest, ReconnectDomainResponse, Boolean] {

      override protected def createRequest(): Either[String, ReconnectDomainRequest] =
        Right(ReconnectDomainRequest(synchronizerAlias.toProtoPrimitive, retry))

      override protected def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ReconnectDomainRequest,
      ): Future[ReconnectDomainResponse] =
        service.reconnectDomain(request)

      override protected def handleResponse(
          response: ReconnectDomainResponse
      ): Either[String, Boolean] =
        Right(response.connectedSuccessfully)

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class GetSynchronizerId(synchronizerAlias: SynchronizerAlias)
        extends Base[GetSynchronizerIdRequest, GetSynchronizerIdResponse, SynchronizerId] {

      override protected def createRequest(): Either[String, GetSynchronizerIdRequest] =
        Right(GetSynchronizerIdRequest(synchronizerAlias.toProtoPrimitive))

      override protected def submitRequest(
          service: DomainConnectivityServiceStub,
          request: GetSynchronizerIdRequest,
      ): Future[GetSynchronizerIdResponse] =
        service.getSynchronizerId(request)

      override protected def handleResponse(
          response: GetSynchronizerIdResponse
      ): Either[String, SynchronizerId] =
        SynchronizerId
          .fromProtoPrimitive(response.synchronizerId, "synchronizer_id")
          .leftMap(_.toString)
    }

    final case class DisconnectDomain(synchronizerAlias: SynchronizerAlias)
        extends Base[DisconnectDomainRequest, DisconnectDomainResponse, Unit] {

      override protected def createRequest(): Either[String, DisconnectDomainRequest] =
        Right(DisconnectDomainRequest(synchronizerAlias.toProtoPrimitive))

      override protected def submitRequest(
          service: DomainConnectivityServiceStub,
          request: DisconnectDomainRequest,
      ): Future[DisconnectDomainResponse] =
        service.disconnectDomain(request)

      override protected def handleResponse(
          response: DisconnectDomainResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class DisconnectAllDomains()
        extends Base[DisconnectAllDomainsRequest, DisconnectAllDomainsResponse, Unit] {

      override protected def createRequest(): Either[String, DisconnectAllDomainsRequest] =
        Right(DisconnectAllDomainsRequest())

      override protected def submitRequest(
          service: DomainConnectivityServiceStub,
          request: DisconnectAllDomainsRequest,
      ): Future[DisconnectAllDomainsResponse] =
        service.disconnectAllDomains(request)

      override protected def handleResponse(
          response: DisconnectAllDomainsResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class ListConnectedDomains()
        extends Base[ListConnectedDomainsRequest, ListConnectedDomainsResponse, Seq[
          ListConnectedDomainsResult
        ]] {

      override protected def createRequest(): Either[String, ListConnectedDomainsRequest] = Right(
        ListConnectedDomainsRequest()
      )

      override protected def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ListConnectedDomainsRequest,
      ): Future[ListConnectedDomainsResponse] =
        service.listConnectedDomains(request)

      override protected def handleResponse(
          response: ListConnectedDomainsResponse
      ): Either[String, Seq[ListConnectedDomainsResult]] =
        response.connectedDomains.traverse(
          ListConnectedDomainsResult.fromProtoV30(_).leftMap(_.toString)
        )

    }

    final case object ListRegisteredDomains
        extends Base[ListRegisteredDomainsRequest, ListRegisteredDomainsResponse, Seq[
          (CDomainConnectionConfig, Boolean)
        ]] {

      override protected def createRequest(): Either[String, ListRegisteredDomainsRequest] = Right(
        ListRegisteredDomainsRequest()
      )

      override protected def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ListRegisteredDomainsRequest,
      ): Future[ListRegisteredDomainsResponse] =
        service.listRegisteredDomains(request)

      override protected def handleResponse(
          response: ListRegisteredDomainsResponse
      ): Either[String, Seq[(CDomainConnectionConfig, Boolean)]] = {

        def mapRes(
            result: ListRegisteredDomainsResponse.Result
        ): Either[String, (CDomainConnectionConfig, Boolean)] =
          for {
            configP <- result.config.toRight("Server has sent empty config")
            config <- CDomainConnectionConfig.fromProtoV30(configP).leftMap(_.toString)
          } yield (config, result.connected)

        response.results.traverse(mapRes)
      }
    }

    final case class ConnectDomain(
        config: CDomainConnectionConfig,
        sequencerConnectionValidation: SequencerConnectionValidation,
    ) extends Base[ConnectDomainRequest, ConnectDomainResponse, Unit] {

      override protected def createRequest(): Either[String, ConnectDomainRequest] =
        Right(
          ConnectDomainRequest(
            config = Some(config.toProtoV30),
            sequencerConnectionValidation = sequencerConnectionValidation.toProtoV30,
          )
        )

      override protected def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ConnectDomainRequest,
      ): Future[ConnectDomainResponse] =
        service.connectDomain(request)

      override protected def handleResponse(
          response: ConnectDomainResponse
      ): Either[String, Unit] = Either.unit

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class RegisterDomain(
        config: CDomainConnectionConfig,
        performHandshake: Boolean,
        sequencerConnectionValidation: SequencerConnectionValidation,
    ) extends Base[RegisterDomainRequest, RegisterDomainResponse, Unit] {

      override protected def createRequest(): Either[String, RegisterDomainRequest] = {
        val domainConnection =
          if (performHandshake) RegisterDomainRequest.DomainConnection.DOMAIN_CONNECTION_HANDSHAKE
          else RegisterDomainRequest.DomainConnection.DOMAIN_CONNECTION_NONE

        Right(
          RegisterDomainRequest(
            config = Some(config.toProtoV30),
            domainConnection = domainConnection,
            sequencerConnectionValidation = sequencerConnectionValidation.toProtoV30,
          )
        )
      }

      override protected def submitRequest(
          service: DomainConnectivityServiceStub,
          request: RegisterDomainRequest,
      ): Future[RegisterDomainResponse] =
        service.registerDomain(request)

      override protected def handleResponse(
          response: RegisterDomainResponse
      ): Either[String, Unit] = Either.unit

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ModifyDomainConnection(
        config: CDomainConnectionConfig,
        sequencerConnectionValidation: SequencerConnectionValidation,
    ) extends Base[ModifyDomainRequest, ModifyDomainResponse, Unit] {

      override protected def createRequest(): Either[String, ModifyDomainRequest] =
        Right(
          ModifyDomainRequest(
            newConfig = Some(config.toProtoV30),
            sequencerConnectionValidation = sequencerConnectionValidation.toProtoV30,
          )
        )

      override protected def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ModifyDomainRequest,
      ): Future[ModifyDomainResponse] =
        service.modifyDomain(request)

      override protected def handleResponse(response: ModifyDomainResponse): Either[String, Unit] =
        Either.unit
    }

    final case class Logout(synchronizerAlias: SynchronizerAlias)
        extends Base[LogoutRequest, LogoutResponse, Unit] {

      override protected def createRequest(): Either[String, LogoutRequest] =
        Right(LogoutRequest(synchronizerAlias.toProtoPrimitive))

      override protected def submitRequest(
          service: DomainConnectivityServiceStub,
          request: LogoutRequest,
      ): Future[LogoutResponse] =
        service.logout(request)

      override protected def handleResponse(response: LogoutResponse): Either[String, Unit] =
        Either.unit
    }
  }

  object Resources {
    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = ResourceManagementServiceStub

      override def createService(channel: ManagedChannel): ResourceManagementServiceStub =
        ResourceManagementServiceGrpc.stub(channel)
    }

    final case class GetResourceLimits() extends Base[Empty, v30.ResourceLimits, ResourceLimits] {
      override protected def createRequest(): Either[String, Empty] = Right(Empty())

      override protected def submitRequest(
          service: ResourceManagementServiceStub,
          request: Empty,
      ): Future[v30.ResourceLimits] =
        service.getResourceLimits(request)

      override protected def handleResponse(
          response: v30.ResourceLimits
      ): Either[String, ResourceLimits] =
        Right(ResourceLimits.fromProtoV30(response))
    }

    final case class SetResourceLimits(limits: ResourceLimits)
        extends Base[v30.ResourceLimits, Empty, Unit] {
      override protected def createRequest(): Either[String, v30.ResourceLimits] = Right(
        limits.toProtoV30
      )

      override protected def submitRequest(
          service: ResourceManagementServiceStub,
          request: v30.ResourceLimits,
      ): Future[Empty] =
        service.updateResourceLimits(request)

      override protected def handleResponse(response: Empty): Either[String, Unit] = Either.unit
    }
  }

  object Inspection {

    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = InspectionServiceStub

      override def createService(channel: ManagedChannel): InspectionServiceStub =
        InspectionServiceGrpc.stub(channel)
    }

    final case class LookupOffsetByTime(ts: Timestamp)
        extends Base[
          v30.LookupOffsetByTime.Request,
          v30.LookupOffsetByTime.Response,
          Option[Long],
        ] {
      override protected def createRequest() = Right(v30.LookupOffsetByTime.Request(Some(ts)))

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.LookupOffsetByTime.Request,
      ): Future[v30.LookupOffsetByTime.Response] =
        service.lookupOffsetByTime(request)

      override protected def handleResponse(
          response: v30.LookupOffsetByTime.Response
      ): Either[String, Option[Long]] =
        Right(response.offset)
    }

    // TODO(#9557) R2 The code below should be sufficient
    final case class OpenCommitment(
        observer: StreamObserver[v30.OpenCommitment.Response],
        commitment: AcsCommitment.CommitmentType,
        synchronizerId: SynchronizerId,
        computedForCounterParticipant: ParticipantId,
        toInclusive: CantonTimestamp,
    ) extends Base[
          v30.OpenCommitment.Request,
          CancellableContext,
          CancellableContext,
        ] {
      override protected def createRequest() = Right(
        v30.OpenCommitment
          .Request(
            AcsCommitment.commitmentTypeToProto(commitment),
            synchronizerId.toProtoPrimitive,
            computedForCounterParticipant.toProtoPrimitive,
            Some(toInclusive.toProtoTimestamp),
          )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.OpenCommitment.Request,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.openCommitment(request, observer))
        Future.successful(context)
      }

      override protected def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] =
        Right(response)

      //  command might take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    // TODO(#9557) R2 The code below should be sufficient
    final case class CommitmentContracts(
        observer: StreamObserver[v30.InspectCommitmentContracts.Response],
        contracts: Seq[LfContractId],
        expectedDomainId: SynchronizerId,
        timestamp: CantonTimestamp,
        downloadPayload: Boolean,
    ) extends Base[
          v30.InspectCommitmentContracts.Request,
          CancellableContext,
          CancellableContext,
        ] {
      override protected def createRequest() = Right(
        v30.InspectCommitmentContracts.Request(
          contracts.map(_.toBytes.toByteString),
          expectedDomainId.toProtoPrimitive,
          Some(timestamp.toProtoTimestamp),
          downloadPayload,
        )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.InspectCommitmentContracts.Request,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.inspectCommitmentContracts(request, observer))
        Future.successful(context)
      }

      override protected def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] =
        Right(response)

      //  command might take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class LookupReceivedAcsCommitments(
        domainTimeRanges: Seq[DomainTimeRange],
        counterParticipants: Seq[ParticipantId],
        commitmentState: Seq[ReceivedCmtState],
        verboseMode: Boolean,
    ) extends Base[
          v30.LookupReceivedAcsCommitments.Request,
          v30.LookupReceivedAcsCommitments.Response,
          Map[SynchronizerId, Seq[ReceivedAcsCmt]],
        ] {

      override protected def createRequest() = Right(
        v30.LookupReceivedAcsCommitments
          .Request(
            domainTimeRanges.map { case domainTimeRange =>
              v30.DomainTimeRange(
                domainTimeRange.synchronizerId.toProtoPrimitive,
                domainTimeRange.timeRange.map { timeRange =>
                  v30.TimeRange(
                    Some(timeRange.startExclusive.toProtoTimestamp),
                    Some(timeRange.endInclusive.toProtoTimestamp),
                  )
                },
              )
            },
            counterParticipants.map(_.toProtoPrimitive),
            commitmentState.map(_.toProtoV30),
            verboseMode,
          )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.LookupReceivedAcsCommitments.Request,
      ): Future[v30.LookupReceivedAcsCommitments.Response] =
        service.lookupReceivedAcsCommitments(request)

      override protected def handleResponse(
          response: v30.LookupReceivedAcsCommitments.Response
      ): Either[
        String,
        Map[SynchronizerId, Seq[ReceivedAcsCmt]],
      ] =
        if (response.received.sizeIs != response.received.map(_.synchronizerId).toSet.size)
          Left(s"Some domains are not unique in the response: ${response.received}")
        else
          response.received
            .traverse(receivedCmtPerDomain =>
              for {
                synchronizerId <- SynchronizerId.fromString(receivedCmtPerDomain.synchronizerId)
                receivedCmts <- receivedCmtPerDomain.received
                  .map(fromProtoToReceivedAcsCmt)
                  .sequence
              } yield synchronizerId -> receivedCmts
            )
            .map(_.toMap)
    }

    final case class TimeRange(startExclusive: CantonTimestamp, endInclusive: CantonTimestamp)

    final case class DomainTimeRange(synchronizerId: SynchronizerId, timeRange: Option[TimeRange])

    final case class ReceivedAcsCmt(
        receivedCmtPeriod: CommitmentPeriod,
        originCounterParticipant: ParticipantId,
        receivedCommitment: Option[AcsCommitment.CommitmentType],
        localCommitment: Option[AcsCommitment.CommitmentType],
        state: ReceivedCmtState,
    )

    private def fromIntervalToCommitmentPeriod(
        interval: Option[v30.Interval]
    ): Either[String, CommitmentPeriod] =
      interval match {
        case None => Left("Interval is missing")
        case Some(v) =>
          for {
            from <- v.startTickExclusive
              .traverse(CantonTimestamp.fromProtoTimestamp)
              .leftMap(_.toString)
            fromSecond <- CantonTimestampSecond.fromCantonTimestamp(
              from.getOrElse(CantonTimestamp.MinValue)
            )
            to <- v.endTickInclusive
              .traverse(CantonTimestamp.fromProtoTimestamp)
              .leftMap(_.toString)
            toSecond <- CantonTimestampSecond.fromCantonTimestamp(
              to.getOrElse(CantonTimestamp.MinValue)
            )
            len <- PositiveSeconds.create(
              java.time.Duration.ofSeconds(
                toSecond.minusSeconds(fromSecond.getEpochSecond).getEpochSecond
              )
            )
          } yield CommitmentPeriod(fromSecond, len)
      }

    private def fromProtoToReceivedAcsCmt(
        cmt: v30.ReceivedAcsCommitment
    ): Either[String, ReceivedAcsCmt] =
      for {
        state <- ReceivedCmtState.fromProtoV30(cmt.state).leftMap(_.toString)
        period <- fromIntervalToCommitmentPeriod(cmt.interval)
        participantId <- ParticipantId
          .fromProtoPrimitive(cmt.originCounterParticipantUid, "")
          .leftMap(_.toString)
      } yield ReceivedAcsCmt(
        period,
        participantId,
        Option
          .when(cmt.receivedCommitment.isDefined)(
            cmt.receivedCommitment.map(AcsCommitment.commitmentTypeFromByteString)
          )
          .flatten,
        Option
          .when(cmt.ownCommitment.isDefined)(
            cmt.ownCommitment.map(AcsCommitment.commitmentTypeFromByteString)
          )
          .flatten,
        state,
      )

    final case class LookupSentAcsCommitments(
        domainTimeRanges: Seq[DomainTimeRange],
        counterParticipants: Seq[ParticipantId],
        commitmentState: Seq[SentCmtState],
        verboseMode: Boolean,
    ) extends Base[
          v30.LookupSentAcsCommitments.Request,
          v30.LookupSentAcsCommitments.Response,
          Map[SynchronizerId, Seq[SentAcsCmt]],
        ] {

      override protected def createRequest() = Right(
        v30.LookupSentAcsCommitments
          .Request(
            domainTimeRanges.map { case domainTimeRange =>
              v30.DomainTimeRange(
                domainTimeRange.synchronizerId.toProtoPrimitive,
                domainTimeRange.timeRange.map { timeRange =>
                  v30.TimeRange(
                    Some(timeRange.startExclusive.toProtoTimestamp),
                    Some(timeRange.endInclusive.toProtoTimestamp),
                  )
                },
              )
            },
            counterParticipants.map(_.toProtoPrimitive),
            commitmentState.map(_.toProtoV30),
            verboseMode,
          )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.LookupSentAcsCommitments.Request,
      ): Future[v30.LookupSentAcsCommitments.Response] =
        service.lookupSentAcsCommitments(request)

      override protected def handleResponse(
          response: v30.LookupSentAcsCommitments.Response
      ): Either[
        String,
        Map[SynchronizerId, Seq[SentAcsCmt]],
      ] =
        if (response.sent.sizeIs != response.sent.map(_.synchronizerId).toSet.size)
          Left(
            s"Some domains are not unique in the response: ${response.sent}"
          )
        else
          response.sent
            .traverse(sentCmtPerDomain =>
              for {
                synchronizerId <- SynchronizerId.fromString(sentCmtPerDomain.synchronizerId)
                sentCmts <- sentCmtPerDomain.sent.map(fromProtoToSentAcsCmt).sequence
              } yield synchronizerId -> sentCmts
            )
            .map(_.toMap)
    }

    final case class SentAcsCmt(
        receivedCmtPeriod: CommitmentPeriod,
        destCounterParticipant: ParticipantId,
        sentCommitment: Option[AcsCommitment.CommitmentType],
        receivedCommitment: Option[AcsCommitment.CommitmentType],
        state: SentCmtState,
    )

    private def fromProtoToSentAcsCmt(
        cmt: v30.SentAcsCommitment
    ): Either[String, SentAcsCmt] =
      for {
        state <- SentCmtState.fromProtoV30(cmt.state).leftMap(_.toString)
        period <- fromIntervalToCommitmentPeriod(cmt.interval)
        participantId <- ParticipantId
          .fromProtoPrimitive(cmt.destCounterParticipantUid, "")
          .leftMap(_.toString)
      } yield SentAcsCmt(
        period,
        participantId,
        Option
          .when(cmt.ownCommitment.isDefined)(
            cmt.ownCommitment.map(AcsCommitment.commitmentTypeFromByteString)
          )
          .flatten,
        Option
          .when(cmt.receivedCommitment.isDefined)(
            cmt.receivedCommitment.map(AcsCommitment.commitmentTypeFromByteString)
          )
          .flatten,
        state,
      )

    final case class SetConfigForSlowCounterParticipants(
        configs: Seq[SlowCounterParticipantDomainConfig]
    ) extends Base[
          v30.SetConfigForSlowCounterParticipants.Request,
          v30.SetConfigForSlowCounterParticipants.Response,
          Unit,
        ] {

      override protected def createRequest() = Right(
        v30.SetConfigForSlowCounterParticipants
          .Request(
            configs.map(_.toProtoV30)
          )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.SetConfigForSlowCounterParticipants.Request,
      ): Future[v30.SetConfigForSlowCounterParticipants.Response] =
        service.setConfigForSlowCounterParticipants(request)

      override protected def handleResponse(
          response: v30.SetConfigForSlowCounterParticipants.Response
      ): Either[String, Unit] = Either.unit
    }

    final case class SlowCounterParticipantDomainConfig(
        synchronizerIds: Seq[SynchronizerId],
        distinguishedParticipants: Seq[ParticipantId],
        thresholdDistinguished: NonNegativeInt,
        thresholdDefault: NonNegativeInt,
        participantsMetrics: Seq[ParticipantId],
    ) {
      def toProtoV30: v30.SlowCounterParticipantDomainConfig =
        v30.SlowCounterParticipantDomainConfig(
          synchronizerIds.map(_.toProtoPrimitive),
          distinguishedParticipants.map(_.toProtoPrimitive),
          thresholdDistinguished.value.toLong,
          thresholdDefault.value.toLong,
          participantsMetrics.map(_.toProtoPrimitive),
        )
    }

    object SlowCounterParticipantDomainConfig {
      def fromProtoV30(
          config: v30.SlowCounterParticipantDomainConfig
      ): Either[String, SlowCounterParticipantDomainConfig] = {
        val thresholdDistinguished = NonNegativeInt.tryCreate(config.thresholdDistinguished.toInt)
        val thresholdDefault = NonNegativeInt.tryCreate(config.thresholdDefault.toInt)
        val distinguishedParticipants =
          config.distinguishedParticipantUids.map(ParticipantId.tryFromProtoPrimitive)
        val participantsMetrics =
          config.participantUidsMetrics.map(ParticipantId.tryFromProtoPrimitive)
        for {
          synchronizerIds <- config.synchronizerIds.map(SynchronizerId.fromString).sequence
        } yield SlowCounterParticipantDomainConfig(
          synchronizerIds,
          distinguishedParticipants,
          thresholdDistinguished,
          thresholdDefault,
          participantsMetrics,
        )
      }
    }

    final case class GetConfigForSlowCounterParticipants(
        synchronizerIds: Seq[SynchronizerId]
    ) extends Base[
          v30.GetConfigForSlowCounterParticipants.Request,
          v30.GetConfigForSlowCounterParticipants.Response,
          Seq[SlowCounterParticipantDomainConfig],
        ] {

      override protected def createRequest() = Right(
        v30.GetConfigForSlowCounterParticipants
          .Request(
            synchronizerIds.map(_.toProtoPrimitive)
          )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.GetConfigForSlowCounterParticipants.Request,
      ): Future[v30.GetConfigForSlowCounterParticipants.Response] =
        service.getConfigForSlowCounterParticipants(request)

      override protected def handleResponse(
          response: v30.GetConfigForSlowCounterParticipants.Response
      ): Either[String, Seq[SlowCounterParticipantDomainConfig]] =
        response.configs.map(SlowCounterParticipantDomainConfig.fromProtoV30).sequence
    }

    final case class CounterParticipantInfo(
        participantId: ParticipantId,
        synchronizerId: SynchronizerId,
        intervalsBehind: NonNegativeLong,
        asOfSequencingTimestamp: Instant,
    )

    final case class GetIntervalsBehindForCounterParticipants(
        counterParticipants: Seq[ParticipantId],
        synchronizerIds: Seq[SynchronizerId],
        threshold: NonNegativeInt,
    ) extends Base[
          v30.GetIntervalsBehindForCounterParticipants.Request,
          v30.GetIntervalsBehindForCounterParticipants.Response,
          Seq[CounterParticipantInfo],
        ] {

      override protected def createRequest() = Right(
        v30.GetIntervalsBehindForCounterParticipants
          .Request(
            counterParticipants.map(_.toProtoPrimitive),
            synchronizerIds.map(_.toProtoPrimitive),
            Some(threshold.value.toLong),
          )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.GetIntervalsBehindForCounterParticipants.Request,
      ): Future[v30.GetIntervalsBehindForCounterParticipants.Response] =
        service.getIntervalsBehindForCounterParticipants(request)

      override protected def handleResponse(
          response: v30.GetIntervalsBehindForCounterParticipants.Response
      ): Either[String, Seq[CounterParticipantInfo]] =
        response.intervalsBehind.map { info =>
          for {
            synchronizerId <- SynchronizerId.fromString(info.synchronizerId)
            participantId <- ParticipantId
              .fromProtoPrimitive(info.counterParticipantUid, "")
              .leftMap(_.toString)
            asOf <- ProtoConverter
              .parseRequired(
                CantonTimestamp.fromProtoTimestamp,
                "as_of",
                info.asOfSequencingTimestamp,
              )
              .leftMap(_.toString)
            intervalsBehind <- NonNegativeLong.create(info.intervalsBehind).leftMap(_.toString)
          } yield CounterParticipantInfo(
            participantId,
            synchronizerId,
            intervalsBehind,
            asOf.toInstant,
          )
        }.sequence
    }

    final case class CountInFlight(synchronizerId: SynchronizerId)
        extends Base[
          v30.CountInFlight.Request,
          v30.CountInFlight.Response,
          InFlightCount,
        ] {

      override protected def createRequest(): Either[String, v30.CountInFlight.Request] =
        Right(v30.CountInFlight.Request(synchronizerId.toProtoPrimitive))

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.CountInFlight.Request,
      ): Future[v30.CountInFlight.Response] =
        service.countInFlight(request)

      override protected def handleResponse(
          response: v30.CountInFlight.Response
      ): Either[String, InFlightCount] =
        for {
          pendingSubmissions <- ProtoConverter
            .parseNonNegativeInt("CountInFlight.pending_submissions", response.pendingSubmissions)
            .leftMap(_.toString)
          pendingTransactions <- ProtoConverter
            .parseNonNegativeInt("CountInFlight.pending_transactions", response.pendingTransactions)
            .leftMap(_.toString)
        } yield {
          InFlightCount(pendingSubmissions, pendingTransactions)
        }
    }

  }

  object Pruning {
    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = PruningServiceStub

      override def createService(channel: ManagedChannel): PruningServiceStub =
        PruningServiceGrpc.stub(channel)
    }

    final case class GetSafePruningOffsetCommand(beforeOrAt: Instant, ledgerEnd: Long)
        extends Base[v30.GetSafePruningOffsetRequest, v30.GetSafePruningOffsetResponse, Option[
          Long
        ]] {

      override protected def createRequest(): Either[String, v30.GetSafePruningOffsetRequest] =
        for {
          beforeOrAt <- CantonTimestamp.fromInstant(beforeOrAt)
        } yield v30.GetSafePruningOffsetRequest(Some(beforeOrAt.toProtoTimestamp), ledgerEnd)

      override protected def submitRequest(
          service: PruningServiceStub,
          request: v30.GetSafePruningOffsetRequest,
      ): Future[v30.GetSafePruningOffsetResponse] = service.getSafePruningOffset(request)

      override protected def handleResponse(
          response: v30.GetSafePruningOffsetResponse
      ): Either[String, Option[Long]] = response.response match {
        case v30.GetSafePruningOffsetResponse.Response.Empty => Left("Unexpected empty response")

        case v30.GetSafePruningOffsetResponse.Response.SafePruningOffset(offset) =>
          Right(Some(offset))

        case v30.GetSafePruningOffsetResponse.Response.NoSafePruningOffset(_) => Right(None)
      }
    }

    final case class PruneInternallyCommand(pruneUpTo: Long)
        extends Base[v30.PruneRequest, v30.PruneResponse, Unit] {
      override protected def createRequest(): Either[String, PruneRequest] =
        Right(v30.PruneRequest(pruneUpTo))

      override protected def submitRequest(
          service: PruningServiceStub,
          request: v30.PruneRequest,
      ): Future[v30.PruneResponse] =
        service.prune(request)

      override protected def handleResponse(response: v30.PruneResponse): Either[String, Unit] =
        Either.unit
    }

    final case class SetParticipantScheduleCommand(
        cron: String,
        maxDuration: config.PositiveDurationSeconds,
        retention: config.PositiveDurationSeconds,
        pruneInternallyOnly: Boolean,
    ) extends Base[
          pruning.v30.SetParticipantSchedule.Request,
          pruning.v30.SetParticipantSchedule.Response,
          Unit,
        ] {
      override protected def createRequest()
          : Right[String, pruning.v30.SetParticipantSchedule.Request] =
        Right(
          pruning.v30.SetParticipantSchedule.Request(
            Some(
              pruning.v30.ParticipantPruningSchedule(
                Some(
                  pruning.v30.PruningSchedule(
                    cron,
                    Some(maxDuration.toProtoPrimitive),
                    Some(retention.toProtoPrimitive),
                  )
                ),
                pruneInternallyOnly,
              )
            )
          )
        )

      override protected def submitRequest(
          service: Svc,
          request: pruning.v30.SetParticipantSchedule.Request,
      ): Future[pruning.v30.SetParticipantSchedule.Response] =
        service.setParticipantSchedule(request)

      override protected def handleResponse(
          response: pruning.v30.SetParticipantSchedule.Response
      ): Either[String, Unit] =
        response match {
          case pruning.v30.SetParticipantSchedule.Response() => Either.unit
        }
    }

    final case class SetNoWaitCommitmentsFrom(
        counterParticipants: Seq[ParticipantId],
        synchronizerIds: Seq[SynchronizerId],
    ) extends Base[
          pruning.v30.SetNoWaitCommitmentsFrom.Request,
          pruning.v30.SetNoWaitCommitmentsFrom.Response,
          Unit,
        ] {
      override def createRequest(): Either[String, pruning.v30.SetNoWaitCommitmentsFrom.Request] =
        Right(
          pruning.v30.SetNoWaitCommitmentsFrom.Request(
            counterParticipants.map(_.toProtoPrimitive),
            synchronizerIds.map(_.toProtoPrimitive),
          )
        )

      override protected def submitRequest(
          service: Svc,
          request: pruning.v30.SetNoWaitCommitmentsFrom.Request,
      ): Future[pruning.v30.SetNoWaitCommitmentsFrom.Response] =
        service.setNoWaitCommitmentsFrom(request)

      override protected def handleResponse(
          response: pruning.v30.SetNoWaitCommitmentsFrom.Response
      ): Either[String, Unit] = Right(())
    }

    final case class NoWaitCommitments(
        counterParticipant: ParticipantId,
        domains: Seq[SynchronizerId],
    )

    object NoWaitCommitments {
      def fromSetup(setup: Seq[WaitCommitmentsSetup]): Either[String, Seq[NoWaitCommitments]] = {
        val s = setup.map(setup =>
          for {
            domains <- setup.domains.traverse(_.synchronizerIds.traverse(SynchronizerId.fromString))
            participantId <- ParticipantId
              .fromProtoPrimitive(setup.counterParticipantUid, "")
              .leftMap(_.toString)
          } yield NoWaitCommitments(
            participantId,
            domains.getOrElse(Seq.empty),
          )
        )
        if (s.forall(_.isRight)) {
          Right(
            s.map(
              _.getOrElse(
                NoWaitCommitments(
                  ParticipantId.tryFromProtoPrimitive("PAR::participant::error"),
                  Seq.empty,
                )
              )
            )
          )
        } else
          Left("Error parsing response of getNoWaitCommitmentsFrom")
      }
    }

    final case class SetWaitCommitmentsFrom(
        counterParticipants: Seq[ParticipantId],
        synchronizerIds: Seq[SynchronizerId],
    ) extends Base[
          pruning.v30.ResetNoWaitCommitmentsFrom.Request,
          pruning.v30.ResetNoWaitCommitmentsFrom.Response,
          Unit,
        ] {
      override protected def createRequest()
          : Right[String, pruning.v30.ResetNoWaitCommitmentsFrom.Request] =
        Right(
          pruning.v30.ResetNoWaitCommitmentsFrom.Request(
            counterParticipants.map(_.toProtoPrimitive),
            synchronizerIds.map(_.toProtoPrimitive),
          )
        )

      override protected def submitRequest(
          service: Svc,
          request: pruning.v30.ResetNoWaitCommitmentsFrom.Request,
      ): Future[pruning.v30.ResetNoWaitCommitmentsFrom.Response] =
        service.resetNoWaitCommitmentsFrom(request)

      override protected def handleResponse(
          response: pruning.v30.ResetNoWaitCommitmentsFrom.Response
      ): Either[String, Unit] = Right(())
    }

    final case class WaitCommitments(
        counterParticipant: ParticipantId,
        domains: Seq[SynchronizerId],
    )

    object WaitCommitments {
      def fromSetup(setup: Seq[WaitCommitmentsSetup]): Either[String, Seq[WaitCommitments]] = {
        val s = setup.map(setup =>
          for {
            domains <- setup.domains.traverse(_.synchronizerIds.traverse(SynchronizerId.fromString))
          } yield WaitCommitments(
            ParticipantId.tryFromProtoPrimitive(setup.counterParticipantUid),
            domains.getOrElse(Seq.empty),
          )
        )
        if (s.forall(_.isRight)) {
          Right(
            s.map(
              _.getOrElse(
                WaitCommitments(
                  ParticipantId.tryFromProtoPrimitive("PAR::participant::error"),
                  Seq.empty,
                )
              )
            )
          )
        } else
          Left("Error parsing response of getNoWaitCommitmentsFrom")
      }
    }

    final case class GetNoWaitCommitmentsFrom(
        domains: Seq[SynchronizerId],
        counterParticipants: Seq[ParticipantId],
    ) extends Base[
          pruning.v30.GetNoWaitCommitmentsFrom.Request,
          pruning.v30.GetNoWaitCommitmentsFrom.Response,
          (Seq[NoWaitCommitments], Seq[WaitCommitments]),
        ] {

      override protected def createRequest()
          : Right[String, pruning.v30.GetNoWaitCommitmentsFrom.Request] =
        Right(
          pruning.v30.GetNoWaitCommitmentsFrom.Request(
            domains.map(_.toProtoPrimitive),
            counterParticipants.map(_.toProtoPrimitive),
          )
        )

      override protected def submitRequest(
          service: Svc,
          request: pruning.v30.GetNoWaitCommitmentsFrom.Request,
      ): Future[pruning.v30.GetNoWaitCommitmentsFrom.Response] =
        service.getNoWaitCommitmentsFrom(request)

      override protected def handleResponse(
          response: pruning.v30.GetNoWaitCommitmentsFrom.Response
      ): Either[String, (Seq[NoWaitCommitments], Seq[WaitCommitments])] = {
        val ignoredCounterParticipants = NoWaitCommitments.fromSetup(response.ignoredParticipants)
        val nonIgnoredCounterParticipants =
          WaitCommitments.fromSetup(response.notIgnoredParticipants)
        if (ignoredCounterParticipants.isLeft || nonIgnoredCounterParticipants.isLeft) {
          Left("Error parsing response of getNoWaitCommitmentsFrom")
        } else {
          Right(
            (
              ignoredCounterParticipants.getOrElse(Seq.empty),
              nonIgnoredCounterParticipants.getOrElse(Seq.empty),
            )
          )
        }
      }
    }

    final case class GetParticipantScheduleCommand()
        extends Base[
          pruning.v30.GetParticipantSchedule.Request,
          pruning.v30.GetParticipantSchedule.Response,
          Option[ParticipantPruningSchedule],
        ] {
      override protected def createRequest()
          : Right[String, pruning.v30.GetParticipantSchedule.Request] =
        Right(
          pruning.v30.GetParticipantSchedule.Request()
        )

      override protected def submitRequest(
          service: Svc,
          request: pruning.v30.GetParticipantSchedule.Request,
      ): Future[pruning.v30.GetParticipantSchedule.Response] =
        service.getParticipantSchedule(request)

      override protected def handleResponse(
          response: pruning.v30.GetParticipantSchedule.Response
      ): Either[String, Option[ParticipantPruningSchedule]] =
        response.schedule.fold(
          Right(None): Either[String, Option[ParticipantPruningSchedule]]
        )(ParticipantPruningSchedule.fromProtoV30(_).bimap(_.message, Some(_)))
    }
  }

  object Replication {

    final case class SetPassiveCommand()
        extends GrpcAdminCommand[SetPassive.Request, SetPassive.Response, Unit] {
      override type Svc = EnterpriseParticipantReplicationServiceStub

      override def createService(
          channel: ManagedChannel
      ): EnterpriseParticipantReplicationServiceStub =
        EnterpriseParticipantReplicationServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, SetPassive.Request] =
        Right(SetPassive.Request())

      override protected def submitRequest(
          service: EnterpriseParticipantReplicationServiceStub,
          request: SetPassive.Request,
      ): Future[SetPassive.Response] =
        service.setPassive(request)

      override protected def handleResponse(
          response: SetPassive.Response
      ): Either[String, Unit] =
        response match {
          case SetPassive.Response() => Either.unit
        }
    }
  }

  object TrafficControl {
    final case class GetTrafficControlState(synchronizerId: SynchronizerId)
        extends GrpcAdminCommand[
          TrafficControlStateRequest,
          TrafficControlStateResponse,
          TrafficState,
        ] {
      override type Svc = TrafficControlServiceGrpc.TrafficControlServiceStub

      override def createService(
          channel: ManagedChannel
      ): TrafficControlServiceGrpc.TrafficControlServiceStub =
        TrafficControlServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: TrafficControlServiceGrpc.TrafficControlServiceStub,
          request: TrafficControlStateRequest,
      ): Future[TrafficControlStateResponse] =
        service.trafficControlState(request)

      override protected def createRequest(): Either[String, TrafficControlStateRequest] = Right(
        TrafficControlStateRequest(synchronizerId.toProtoPrimitive)
      )

      override protected def handleResponse(
          response: TrafficControlStateResponse
      ): Either[String, TrafficState] =
        response.trafficState
          .map { trafficStatus =>
            TrafficStateAdmin
              .fromProto(trafficStatus)
              .leftMap(_.message)
          }
          .getOrElse(Left("No traffic state available"))
    }
  }

  object Health {
    final case class ParticipantStatusCommand()
        extends GrpcAdminCommand[
          ParticipantStatusRequest,
          ParticipantStatusResponse,
          NodeStatus[ParticipantStatus],
        ] {

      override type Svc = ParticipantStatusServiceStub

      override def createService(channel: ManagedChannel): ParticipantStatusServiceStub =
        ParticipantStatusServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantStatusServiceStub,
          request: ParticipantStatusRequest,
      ): Future[ParticipantStatusResponse] =
        service.participantStatus(request)

      override protected def createRequest(): Either[String, ParticipantStatusRequest] = Right(
        ParticipantStatusRequest()
      )

      override protected def handleResponse(
          response: ParticipantStatusResponse
      ): Either[String, NodeStatus[ParticipantStatus]] =
        ParticipantStatus.fromProtoV30(response).leftMap(_.message)
    }
  }

}
