// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.implicits._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.canton.admin.api.client.PathUtils
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  ServerEnforcedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.ListConnectedDomainsResult
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.admin.grpc.TransferSearchResult
import com.digitalasset.canton.participant.admin.v0.DomainConnectivityServiceGrpc.DomainConnectivityServiceStub
import com.digitalasset.canton.participant.admin.v0.EnterpriseParticipantReplicationServiceGrpc.EnterpriseParticipantReplicationServiceStub
import com.digitalasset.canton.participant.admin.v0.InspectionServiceGrpc.InspectionServiceStub
import com.digitalasset.canton.participant.admin.v0.PackageServiceGrpc.PackageServiceStub
import com.digitalasset.canton.participant.admin.v0.PartyNameManagementServiceGrpc.PartyNameManagementServiceStub
import com.digitalasset.canton.participant.admin.v0.PingServiceGrpc.PingServiceStub
import com.digitalasset.canton.participant.admin.v0.PruningServiceGrpc.PruningServiceStub
import com.digitalasset.canton.participant.admin.v0.ResourceManagementServiceGrpc.ResourceManagementServiceStub
import com.digitalasset.canton.participant.admin.v0.TransferServiceGrpc.TransferServiceStub
import com.digitalasset.canton.participant.admin.v0.{ResourceLimits => _, _}
import com.digitalasset.canton.participant.admin.{ResourceLimits, v0}
import com.digitalasset.canton.participant.domain.{
  DomainConnectionConfig => CDomainConnectionConfig
}
import com.digitalasset.canton.protocol.{LfContractId, TransferId, v0 => v0proto}
import com.digitalasset.canton.serialization.ProtoConverter.InstantConverter
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{DomainAlias, LedgerTransactionId}
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp
import io.grpc.ManagedChannel

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

    final case class List(limit: Option[Int])
        extends PackageCommand[ListPackagesRequest, ListPackagesResponse, Seq[PackageDescription]] {
      override def createRequest() = Right(ListPackagesRequest(limit.getOrElse(0)))

      override def submitRequest(
          service: PackageServiceStub,
          request: ListPackagesRequest,
      ): Future[ListPackagesResponse] =
        service.listPackages(request)

      override def handleResponse(
          response: ListPackagesResponse
      ): Either[String, Seq[PackageDescription]] =
        Right(response.packageDescriptions)
    }

    final case class ListContents(packageId: String)
        extends PackageCommand[ListPackageContentsRequest, ListPackageContentsResponse, Seq[
          ModuleDescription
        ]] {
      override def createRequest() = Right(ListPackageContentsRequest(packageId))

      override def submitRequest(
          service: PackageServiceStub,
          request: ListPackageContentsRequest,
      ): Future[ListPackageContentsResponse] =
        service.listPackageContents(request)

      override def handleResponse(
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

      override def createRequest(): Either[String, UploadDarRequest] =
        for {
          pathValue <- darPath.toRight("DAR path not provided")
          nonEmptyPathValue <- Either.cond(
            pathValue.nonEmpty,
            pathValue,
            "Provided DAR path is empty",
          )
          filename = PathUtils.getFilenameWithoutExtension(Paths.get(nonEmptyPathValue))
          darData <- BinaryFileUtil.readByteStringFromFile(nonEmptyPathValue)
        } yield UploadDarRequest(
          darData,
          filename,
          vetAllPackages = vetAllPackages,
          synchronizeVetting = synchronizeVetting,
        )

      override def submitRequest(
          service: PackageServiceStub,
          request: UploadDarRequest,
      ): Future[UploadDarResponse] =
        service.uploadDar(request)

      override def handleResponse(response: UploadDarResponse): Either[String, String] =
        response.value match {
          case UploadDarResponse.Value.Success(UploadDarResponse.Success(hash)) => Right(hash)
          case UploadDarResponse.Value.Failure(UploadDarResponse.Failure(msg)) => Left(msg)
          case UploadDarResponse.Value.Empty => Left("unexpected empty response")
        }

      // file can be big. checking & vetting might take a while
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class RemovePackage(
        packageId: String,
        force: Boolean,
    ) extends PackageCommand[RemovePackageRequest, RemovePackageResponse, Unit] {

      override def createRequest() = Right(RemovePackageRequest(packageId, force))

      override def submitRequest(
          service: PackageServiceStub,
          request: RemovePackageRequest,
      ): Future[RemovePackageResponse] =
        service.removePackage(request)

      override def handleResponse(
          response: RemovePackageResponse
      ): Either[String, Unit] = {
        response.success match {
          case None => Left("unexpected empty response")
          case Some(success) => Right(())
        }
      }

    }

    final case class GetDar(
        darHash: Option[String],
        destinationDirectory: Option[String],
        logger: TracedLogger,
    ) extends PackageCommand[GetDarRequest, GetDarResponse, Path] {
      override def createRequest(): Either[String, GetDarRequest] =
        for {
          _ <- destinationDirectory.toRight("DAR destination directory not provided")
          hash <- darHash.toRight("DAR hash not provided")
        } yield GetDarRequest(hash)

      override def submitRequest(
          service: PackageServiceStub,
          request: GetDarRequest,
      ): Future[GetDarResponse] =
        service.getDar(request)

      override def handleResponse(response: GetDarResponse): Either[String, Path] =
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

    final case class RemoveDar(
        darHash: String
    ) extends PackageCommand[RemoveDarRequest, RemoveDarResponse, Unit] {

      override def createRequest() = Right(RemoveDarRequest(darHash))

      override def submitRequest(
          service: PackageServiceStub,
          request: RemoveDarRequest,
      ): Future[RemoveDarResponse] =
        service.removeDar(request)

      override def handleResponse(
          response: RemoveDarResponse
      ): Either[String, Unit] = {
        response.success match {
          case None => Left("unexpected empty response")
          case Some(success) => Right(())
        }
      }

    }

    final case class ListDars(limit: Option[Int])
        extends PackageCommand[ListDarsRequest, ListDarsResponse, Seq[DarDescription]] {
      override def createRequest(): Either[String, ListDarsRequest] = Right(
        ListDarsRequest(limit.getOrElse(0))
      )

      override def submitRequest(
          service: PackageServiceStub,
          request: ListDarsRequest,
      ): Future[ListDarsResponse] =
        service.listDars(request)

      override def handleResponse(response: ListDarsResponse): Either[String, Seq[DarDescription]] =
        Right(response.dars)
    }

    final case class ShareDar(hash: String, recipient: String)
        extends PackageCommand[ShareRequest, Empty, Unit] {
      override def createRequest(): Either[String, ShareRequest] =
        Right(ShareRequest(hash, recipient))

      override def submitRequest(
          service: PackageServiceStub,
          request: ShareRequest,
      ): Future[Empty] =
        service.share(request)

      override def handleResponse(response: Empty): Either[String, Unit] =
        Right(())
    }

    final object ListShareRequests
        extends PackageCommand[Empty, ListShareRequestsResponse, Seq[
          ListShareRequestsResponse.Item
        ]] {
      override def createRequest(): Either[String, Empty] = Right(Empty())

      override def submitRequest(
          service: PackageServiceStub,
          request: Empty,
      ): Future[ListShareRequestsResponse] =
        service.listShareRequests(request)

      override def handleResponse(
          response: ListShareRequestsResponse
      ): Either[String, Seq[ListShareRequestsResponse.Item]] =
        Right(response.shareRequests)
    }

    final object ListShareOffers
        extends PackageCommand[Empty, ListShareOffersResponse, Seq[ListShareOffersResponse.Item]] {
      override def createRequest(): Either[String, Empty] = Right(Empty())

      override def submitRequest(
          service: PackageServiceStub,
          request: Empty,
      ): Future[ListShareOffersResponse] =
        service.listShareOffers(request)

      override def handleResponse(
          response: ListShareOffersResponse
      ): Either[String, Seq[ListShareOffersResponse.Item]] =
        Right(response.shareOffers)
    }

    final case class AcceptShareOffer(shareId: String)
        extends PackageCommand[AcceptShareOfferRequest, Empty, Unit] {
      override def createRequest(): Either[String, AcceptShareOfferRequest] =
        Right(AcceptShareOfferRequest(shareId))

      override def submitRequest(
          service: PackageServiceStub,
          request: AcceptShareOfferRequest,
      ): Future[Empty] =
        service.acceptShareOffer(request)

      override def handleResponse(response: Empty): Either[String, Unit] = Right(())
    }

    final case class RejectShareOffer(shareId: String, reason: String)
        extends PackageCommand[RejectShareOfferRequest, Empty, Unit] {
      override def createRequest(): Either[String, RejectShareOfferRequest] =
        Right(RejectShareOfferRequest(shareId, reason))

      override def submitRequest(
          service: PackageServiceStub,
          request: RejectShareOfferRequest,
      ): Future[Empty] =
        service.rejectShareOffer(request)

      override def handleResponse(response: Empty): Either[String, Unit] = Right(())
    }

    final case class WhitelistAdd(partyId: String)
        extends PackageCommand[WhitelistChangeRequest, Empty, Unit] {
      override def createRequest(): Either[String, WhitelistChangeRequest] = Right(
        WhitelistChangeRequest(partyId)
      )

      override def submitRequest(
          service: PackageServiceStub,
          request: WhitelistChangeRequest,
      ): Future[Empty] =
        service.whitelistAdd(request)

      override def handleResponse(response: Empty): Either[String, Unit] = Right(())
    }

    final case class WhitelistRemove(partyId: String)
        extends PackageCommand[WhitelistChangeRequest, Empty, Unit] {
      override def createRequest(): Either[String, WhitelistChangeRequest] = Right(
        WhitelistChangeRequest(partyId)
      )

      override def submitRequest(
          service: PackageServiceStub,
          request: WhitelistChangeRequest,
      ): Future[Empty] =
        service.whitelistRemove(request)

      override def handleResponse(response: Empty): Either[String, Unit] = Right(())
    }

    final case object WhitelistList
        extends PackageCommand[Empty, WhitelistListResponse, Seq[String]] {
      override def createRequest(): Either[String, Empty] = Right(Empty())

      override def submitRequest(
          service: PackageServiceStub,
          request: Empty,
      ): Future[WhitelistListResponse] =
        service.whitelistList(request)

      override def handleResponse(response: WhitelistListResponse): Either[String, Seq[String]] =
        Right(response.partyIds)
    }

  }

  object PartyNameManagement {

    final case class SetPartyDisplayName(partyId: PartyId, displayName: String)
        extends GrpcAdminCommand[SetPartyDisplayNameRequest, SetPartyDisplayNameResponse, Unit] {
      override type Svc = PartyNameManagementServiceStub
      override def createService(channel: ManagedChannel): PartyNameManagementServiceStub =
        PartyNameManagementServiceGrpc.stub(channel)

      override def createRequest(): Either[String, SetPartyDisplayNameRequest] =
        Right(
          SetPartyDisplayNameRequest(
            partyId = partyId.uid.toProtoPrimitive,
            displayName = displayName,
          )
        )

      override def submitRequest(
          service: PartyNameManagementServiceStub,
          request: SetPartyDisplayNameRequest,
      ): Future[SetPartyDisplayNameResponse] =
        service.setPartyDisplayName(request)

      override def handleResponse(response: SetPartyDisplayNameResponse): Either[String, Unit] =
        Right(())

    }

  }

  object Ping {

    final case class Ping(
        targets: Set[String],
        validators: Set[String],
        timeoutMillis: Long,
        levels: Long,
        gracePeriodMillis: Long,
        workflowId: String,
        id: String,
    ) extends GrpcAdminCommand[PingRequest, PingResponse, Option[Duration]] {
      override type Svc = PingServiceStub

      override def createService(channel: ManagedChannel): PingServiceStub =
        PingServiceGrpc.stub(channel)

      override def createRequest(): Either[String, PingRequest] =
        Right(
          PingRequest(
            targets.toSeq,
            validators.toSeq,
            timeoutMillis,
            levels,
            gracePeriodMillis,
            workflowId,
            id,
          )
        )

      override def submitRequest(
          service: PingServiceStub,
          request: PingRequest,
      ): Future[PingResponse] =
        service.ping(request)

      override def handleResponse(response: PingResponse): Either[String, Option[Duration]] =
        response.response match {
          case PingResponse.Response.Success(PingSuccess(pingTime, responder)) =>
            Right(Some(Duration(pingTime, MILLISECONDS)))
          case PingResponse.Response.Failure(_ex) => Right(None)
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

      override def createRequest(): Either[String, ReconnectDomainsRequest] =
        Right(ReconnectDomainsRequest(ignoreFailures = ignoreFailures))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ReconnectDomainsRequest,
      ): Future[ReconnectDomainsResponse] =
        service.reconnectDomains(request)

      override def handleResponse(response: ReconnectDomainsResponse): Either[String, Unit] = Right(
        ()
      )

      // depending on the confirmation timeout and the load, this might take a bit longer
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ConnectDomain(domainAlias: DomainAlias, retry: Boolean)
        extends Base[ConnectDomainRequest, ConnectDomainResponse, Boolean] {

      override def createRequest(): Either[String, ConnectDomainRequest] =
        Right(ConnectDomainRequest(domainAlias.toProtoPrimitive, retry))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ConnectDomainRequest,
      ): Future[ConnectDomainResponse] =
        service.connectDomain(request)

      override def handleResponse(response: ConnectDomainResponse): Either[String, Boolean] =
        Right(response.connectedSuccessfully)

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class GetDomainId(domainAlias: DomainAlias)
        extends Base[GetDomainIdRequest, GetDomainIdResponse, DomainId] {

      override def createRequest(): Either[String, GetDomainIdRequest] =
        Right(GetDomainIdRequest(domainAlias.toProtoPrimitive))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: GetDomainIdRequest,
      ): Future[GetDomainIdResponse] =
        service.getDomainId(request)

      override def handleResponse(response: GetDomainIdResponse): Either[String, DomainId] =
        DomainId.fromProtoPrimitive(response.domainId, "domain_id").leftMap(_.toString)
    }

    final case class DisconnectDomain(domainAlias: DomainAlias)
        extends Base[DisconnectDomainRequest, DisconnectDomainResponse, Unit] {

      override def createRequest(): Either[String, DisconnectDomainRequest] =
        Right(DisconnectDomainRequest(domainAlias.toProtoPrimitive))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: DisconnectDomainRequest,
      ): Future[DisconnectDomainResponse] =
        service.disconnectDomain(request)

      override def handleResponse(response: DisconnectDomainResponse): Either[String, Unit] = Right(
        ()
      )
    }

    final case class ListConnectedDomains()
        extends Base[ListConnectedDomainsRequest, ListConnectedDomainsResponse, Seq[
          ListConnectedDomainsResult
        ]] {

      override def createRequest(): Either[String, ListConnectedDomainsRequest] = Right(
        ListConnectedDomainsRequest()
      )

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ListConnectedDomainsRequest,
      ): Future[ListConnectedDomainsResponse] =
        service.listConnectedDomains(request)

      override def handleResponse(
          response: ListConnectedDomainsResponse
      ): Either[String, Seq[ListConnectedDomainsResult]] =
        response.connectedDomains.traverse(
          ListConnectedDomainsResult.fromProtoV0(_).leftMap(_.toString)
        )

    }

    final case object ListConfiguredDomains
        extends Base[ListConfiguredDomainsRequest, ListConfiguredDomainsResponse, Seq[
          (CDomainConnectionConfig, Boolean)
        ]] {

      override def createRequest(): Either[String, ListConfiguredDomainsRequest] = Right(
        ListConfiguredDomainsRequest()
      )

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ListConfiguredDomainsRequest,
      ): Future[ListConfiguredDomainsResponse] =
        service.listConfiguredDomains(request)

      override def handleResponse(
          response: ListConfiguredDomainsResponse
      ): Either[String, Seq[(CDomainConnectionConfig, Boolean)]] = {

        def mapRes(
            result: ListConfiguredDomainsResponse.Result
        ): Either[String, (CDomainConnectionConfig, Boolean)] =
          for {
            configP <- result.config.toRight("Server has sent empty config")
            config <- CDomainConnectionConfig.fromProtoV0(configP).leftMap(_.toString)
          } yield (config, result.connected)

        response.results.traverse(mapRes)
      }
    }

    final case class RegisterDomain(config: CDomainConnectionConfig)
        extends Base[RegisterDomainRequest, RegisterDomainResponse, Unit] {

      override def createRequest(): Either[String, RegisterDomainRequest] =
        Right(RegisterDomainRequest(add = Some(config.toProtoV0)))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: RegisterDomainRequest,
      ): Future[RegisterDomainResponse] =
        service.registerDomain(request)

      override def handleResponse(response: RegisterDomainResponse): Either[String, Unit] = Right(
        ()
      )

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ModifyDomainConnection(config: CDomainConnectionConfig)
        extends Base[ModifyDomainRequest, ModifyDomainResponse, Unit] {

      override def createRequest(): Either[String, ModifyDomainRequest] =
        Right(ModifyDomainRequest(modify = Some(config.toProtoV0)))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ModifyDomainRequest,
      ): Future[ModifyDomainResponse] =
        service.modifyDomain(request)

      override def handleResponse(response: ModifyDomainResponse): Either[String, Unit] = Right(())

    }

    final case class GetAgreement(domainAlias: DomainAlias)
        extends Base[GetAgreementRequest, GetAgreementResponse, Option[(Agreement, Boolean)]] {

      override def createRequest(): Either[String, GetAgreementRequest] = Right(
        GetAgreementRequest(domainAlias.unwrap)
      )

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: GetAgreementRequest,
      ): Future[GetAgreementResponse] =
        service.getAgreement(request)

      override def handleResponse(
          response: GetAgreementResponse
      ): Either[String, Option[(Agreement, Boolean)]] =
        Right(response.agreement.map(ag => (ag, response.accepted)))
    }

    final case class AcceptAgreement(domainAlias: DomainAlias, agreementId: String)
        extends Base[AcceptAgreementRequest, AcceptAgreementResponse, Unit] {

      override def createRequest(): Either[String, AcceptAgreementRequest] =
        Right(AcceptAgreementRequest(domainAlias.unwrap, agreementId))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: AcceptAgreementRequest,
      ): Future[AcceptAgreementResponse] =
        service.acceptAgreement(request)

      override def handleResponse(response: AcceptAgreementResponse): Either[String, Unit] = Right(
        ()
      )
    }

  }

  object Transfer {

    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = TransferServiceStub
      override def createService(channel: ManagedChannel): TransferServiceStub =
        TransferServiceGrpc.stub(channel)
    }

    final case class TransferOut(
        submittingParty: PartyId,
        contractId: LfContractId,
        originDomain: DomainAlias,
        targetDomain: DomainAlias,
    ) extends Base[AdminTransferOutRequest, AdminTransferOutResponse, TransferId] {
      override def createRequest(): Either[String, AdminTransferOutRequest] =
        Right(
          AdminTransferOutRequest(
            submittingParty = submittingParty.toLf,
            originDomain = originDomain.toProtoPrimitive,
            targetDomain = targetDomain.toProtoPrimitive,
            contractId = contractId.coid,
          )
        )

      override def submitRequest(
          service: TransferServiceStub,
          request: AdminTransferOutRequest,
      ): Future[AdminTransferOutResponse] =
        service.transferOut(request)

      override def handleResponse(response: AdminTransferOutResponse): Either[String, TransferId] =
        response match {
          case AdminTransferOutResponse(Some(transferIdP)) =>
            TransferId.fromProtoV0(transferIdP).leftMap(_.toString)
          case AdminTransferOutResponse(None) => Left("Empty TransferOutResponse")
        }
    }

    final case class TransferIn(
        submittingParty: PartyId,
        transferId: v0proto.TransferId,
        targetDomain: DomainAlias,
    ) extends Base[AdminTransferInRequest, AdminTransferInResponse, Unit] {

      override def createRequest(): Either[String, AdminTransferInRequest] =
        Right(
          AdminTransferInRequest(
            submittingPartyId = submittingParty.toLf,
            transferId = Some(transferId),
            targetDomain = targetDomain.toProtoPrimitive,
          )
        )

      override def submitRequest(
          service: TransferServiceStub,
          request: AdminTransferInRequest,
      ): Future[AdminTransferInResponse] =
        service.transferIn(request)

      override def handleResponse(response: AdminTransferInResponse): Either[String, Unit] = Right(
        ()
      )

    }

    final case class TransferSearch(
        targetDomain: DomainAlias,
        originDomainFilter: Option[DomainAlias],
        timestampFilter: Option[Instant],
        submittingPartyFilter: Option[PartyId],
        limit0: Int,
    ) extends Base[
          AdminTransferSearchQuery,
          AdminTransferSearchResponse,
          Seq[TransferSearchResult],
        ] {

      override def createRequest(): Either[String, AdminTransferSearchQuery] =
        Right(
          AdminTransferSearchQuery(
            searchDomain = targetDomain.toProtoPrimitive,
            filterOriginDomain = originDomainFilter.map(_.toProtoPrimitive).getOrElse(""),
            filterTimestamp =
              timestampFilter.map((value: Instant) => InstantConverter.toProtoPrimitive(value)),
            filterSubmittingParty = submittingPartyFilter.fold("")(_.toLf),
            limit = limit0.toLong,
          )
        )

      override def submitRequest(
          service: TransferServiceStub,
          request: AdminTransferSearchQuery,
      ): Future[AdminTransferSearchResponse] =
        service.transferSearch(request)

      override def handleResponse(
          response: AdminTransferSearchResponse
      ): Either[String, Seq[TransferSearchResult]] =
        response match {
          case AdminTransferSearchResponse(results) =>
            results.traverse(TransferSearchResult.fromProtoV0).leftMap(_.toString)
        }

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }
  }

  object Resources {
    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = ResourceManagementServiceStub
      override def createService(channel: ManagedChannel): ResourceManagementServiceStub =
        ResourceManagementServiceGrpc.stub(channel)
    }

    final case class GetResourceLimits() extends Base[Empty, v0.ResourceLimits, ResourceLimits] {
      override def createRequest(): Either[String, Empty] = Right(Empty())

      override def submitRequest(
          service: ResourceManagementServiceStub,
          request: Empty,
      ): Future[v0.ResourceLimits] =
        service.getResourceLimits(request)

      override def handleResponse(response: v0.ResourceLimits): Either[String, ResourceLimits] = {
        Right(ResourceLimits.fromProtoV0(response))
      }
    }

    final case class SetResourceLimits(limits: ResourceLimits)
        extends Base[v0.ResourceLimits, Empty, Unit] {
      override def createRequest(): Either[String, v0.ResourceLimits] = Right(limits.toProtoV0)

      override def submitRequest(
          service: ResourceManagementServiceStub,
          request: v0.ResourceLimits,
      ): Future[Empty] =
        service.updateResourceLimits(request)

      override def handleResponse(response: Empty): Either[String, Unit] = Right(())
    }
  }

  object Inspection {

    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = InspectionServiceStub
      override def createService(channel: ManagedChannel): InspectionServiceStub =
        InspectionServiceGrpc.stub(channel)
    }

    final case class LookupContractDomain(contractIds: Set[LfContractId])
        extends Base[
          v0.LookupContractDomain.Request,
          v0.LookupContractDomain.Response,
          Map[LfContractId, String],
        ] {
      override def createRequest() = Right(
        v0.LookupContractDomain.Request(contractIds.toSeq.map(_.coid))
      )

      override def submitRequest(
          service: InspectionServiceStub,
          request: v0.LookupContractDomain.Request,
      ): Future[v0.LookupContractDomain.Response] =
        service.lookupContractDomain(request)

      override def handleResponse(
          response: v0.LookupContractDomain.Response
      ): Either[String, Map[LfContractId, String]] = Right(
        response.results.map { case (id, domain) =>
          LfContractId.assertFromString(id) -> domain
        }
      )

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class LookupTransactionDomain(transactionId: LedgerTransactionId)
        extends Base[
          v0.LookupTransactionDomain.Request,
          v0.LookupTransactionDomain.Response,
          DomainId,
        ] {
      override def createRequest() = Right(v0.LookupTransactionDomain.Request(transactionId))

      override def submitRequest(
          service: InspectionServiceStub,
          request: v0.LookupTransactionDomain.Request,
      ): Future[v0.LookupTransactionDomain.Response] =
        service.lookupTransactionDomain(request)

      override def handleResponse(
          response: v0.LookupTransactionDomain.Response
      ): Either[String, DomainId] =
        DomainId.fromString(response.domainId)

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class LookupOffsetByTime(ts: Timestamp)
        extends Base[v0.LookupOffsetByTime.Request, v0.LookupOffsetByTime.Response, String] {
      override def createRequest() = Right(v0.LookupOffsetByTime.Request(Some(ts)))

      override def submitRequest(
          service: InspectionServiceStub,
          request: v0.LookupOffsetByTime.Request,
      ): Future[v0.LookupOffsetByTime.Response] =
        service.lookupOffsetByTime(request)

      override def handleResponse(
          response: v0.LookupOffsetByTime.Response
      ): Either[String, String] =
        Right(response.offset)
    }

    final case class LookupOffsetByIndex(index: Long)
        extends Base[v0.LookupOffsetByIndex.Request, v0.LookupOffsetByIndex.Response, String] {
      override def createRequest() = Right(v0.LookupOffsetByIndex.Request(index))

      override def submitRequest(
          service: InspectionServiceStub,
          request: v0.LookupOffsetByIndex.Request,
      ): Future[v0.LookupOffsetByIndex.Response] =
        service.lookupOffsetByIndex(request)

      override def handleResponse(
          response: v0.LookupOffsetByIndex.Response
      ): Either[String, String] =
        Right(response.offset)
    }

  }

  object Pruning {
    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = PruningServiceStub
      override def createService(channel: ManagedChannel): PruningServiceStub =
        PruningServiceGrpc.stub(channel)
    }

    final case class PruneInternallyCommand(pruneUpTo: LedgerOffset)
        extends Base[PruneRequest, PruneResponse, Unit] {
      override def createRequest() =
        pruneUpTo.value.absolute
          .toRight("The pruneUpTo ledger offset needs to be absolute")
          .map(v0.PruneRequest(_))

      override def submitRequest(
          service: PruningServiceStub,
          request: v0.PruneRequest,
      ): Future[v0.PruneResponse] =
        service.prune(request)

      override def handleResponse(response: v0.PruneResponse): Either[String, Unit] = Right(())
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

      override def createRequest(): Either[String, SetPassive.Request] =
        Right(SetPassive.Request())

      override def submitRequest(
          service: EnterpriseParticipantReplicationServiceStub,
          request: SetPassive.Request,
      ): Future[SetPassive.Response] =
        service.setPassive(request)

      override def handleResponse(response: SetPassive.Response): Either[String, Unit] =
        response match {
          case SetPassive.Response() => Right(())
        }
    }
  }

}
