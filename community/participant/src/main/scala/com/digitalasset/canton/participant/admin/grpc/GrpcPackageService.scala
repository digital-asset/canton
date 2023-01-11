// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.client.binding.{Contract, Primitive as P}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService.DarDescriptor
import com.digitalasset.canton.participant.admin.ShareError.DarNotFound
import com.digitalasset.canton.participant.admin.*
import com.digitalasset.canton.participant.admin.v0.{DarDescription as ProtoDarDescription, *}
import com.digitalasset.canton.participant.admin.workflows.DarDistribution as M
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, OptionUtil}
import com.digitalasset.canton.{LfPackageId, protocol}
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class GrpcPackageService(
    service: PackageService,
    darDistribution: DarDistribution,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends PackageServiceGrpc.PackageService
    with NamedLogging {

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      activePackages <- service.listPackages(OptionUtil.zeroAsNone(request.limit))
    } yield ListPackagesResponse(activePackages.map {
      case protocol.PackageDescription(pid, sourceDescription) =>
        v0.PackageDescription(pid, sourceDescription.unwrap)
    })
  }

  override def uploadDar(request: UploadDarRequest): Future[UploadDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      hash <- service.appendDarFromByteString(
        request.data,
        request.filename,
        request.vetAllPackages,
        request.synchronizeVetting,
      )
    } yield UploadDarResponse(
      UploadDarResponse.Value.Success(UploadDarResponse.Success(hash.toHexString))
    )
    EitherTUtil.toFuture(ret.leftMap(err => err.code.asGrpcError(err)))
  }

  override def removePackage(request: RemovePackageRequest): Future[RemovePackageResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val packageIdE: Either[StatusRuntimeException, LfPackageId] =
      LfPackageId
        .fromString(request.packageId)
        .left
        .map(_ =>
          Status.INVALID_ARGUMENT
            .withDescription(s"Invalid package ID: ${request.packageId}")
            .asRuntimeException()
        )

    val ret = {
      for {
        packageId <- EitherT.fromEither[Future](packageIdE)
        _unit <- service
          .removePackage(
            packageId,
            request.force,
          )
          .leftMap(err => err.code.asGrpcError(err))
      } yield {
        RemovePackageResponse(success = Some(Empty()))
      }
    }

    EitherTUtil.toFuture(ret)
  }

  override def removeDar(request: RemoveDarRequest): Future[RemoveDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val hashE = Hash
      .fromHexString(request.darHash)
      .left
      .map(err =>
        Status.INVALID_ARGUMENT
          .withDescription(s"Invalid dar hash: ${request.darHash} [$err]")
          .asRuntimeException()
      )
    val ret = {
      for {
        hash <- EitherT.fromEither[Future](hashE)
        _unit <- service.removeDar(hash).leftMap(_.asGrpcError)
      } yield {
        RemoveDarResponse(success = Some(Empty()))
      }
    }

    EitherTUtil.toFuture(ret)
  }

  override def getDar(request: GetDarRequest): Future[GetDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val darHash = Hash.tryFromHexString(request.hash)
    for {
      maybeDar <- service.getDar(darHash)
    } yield maybeDar.fold(GetDarResponse(data = ByteString.EMPTY, name = "")) { dar =>
      GetDarResponse(ByteString.copyFrom(dar.bytes), dar.descriptor.name.toProtoPrimitive)
    }
  }

  override def listDars(request: ListDarsRequest): Future[ListDarsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      dars <- service.listDars(OptionUtil.zeroAsNone(request.limit))
    } yield ListDarsResponse(dars.map { case DarDescriptor(hash, name) =>
      ProtoDarDescription(hash.toHexString, name.toProtoPrimitive)
    })
  }

  override def listDarContents(request: ListDarContentsRequest): Future[ListDarContentsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      hash <- EitherT.fromEither[Future](Hash.fromHexString(request.darId)).leftMap(_.toString)
      result <- service.listDarContents(hash)
    } yield {
      val (description, archive) = result
      ListDarContentsResponse(
        description = description.name.toProtoPrimitive,
        main = archive.main.getHash,
        packages = archive.all.map(_.getHash),
        dependencies = archive.dependencies.map(_.getHash),
      )
    }
    EitherTUtil.toFuture(res.leftMap(Status.NOT_FOUND.withDescription(_).asRuntimeException()))
  }

  override def listPackageContents(
      request: ListPackageContentsRequest
  ): Future[ListPackageContentsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      optModules <- service.getPackage(LfPackageId.assertFromString(request.packageId))
      modules = optModules.map(_.modules).getOrElse(Map.empty)
    } yield {
      ListPackageContentsResponse(modules.toSeq.map { case (moduleName, _) =>
        ModuleDescription(moduleName.dottedName)
      })
    }
  }

  override def share(request: ShareRequest): Future[Empty] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val darHash = Hash.tryFromHexString(request.darHash)
    darDistribution
      .share(darHash, Converters.toParty(request.recipientId))
      .map(_.left.map {
        case DarNotFound =>
          Status.NOT_FOUND.withDescription("Dar with matching hash was not found")
        case ShareError.SubmissionFailed(_) =>
          Status.INTERNAL.withDescription("Submission of share request to ledger failed")
      })
      .flatMap {
        case Left(status) => Future.failed(status.asException())
        case Right(_) => Future.successful(Empty())
      }
  }

  override def listShareRequests(request: Empty): Future[ListShareRequestsResponse] =
    darDistribution
      .listRequests()
      .map { requests =>
        ListShareRequestsResponse(requests.map(shareRequestItem))
      }

  override def listShareOffers(request: Empty): Future[ListShareOffersResponse] =
    darDistribution
      .listOffers()
      .map { offers =>
        ListShareOffersResponse(offers.map(shareOfferItem))
      }

  override def acceptShareOffer(request: AcceptShareOfferRequest): Future[Empty] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    darDistribution
      .accept(Converters.toContractId[M.ShareDar](request.id))
      .map(acceptRejectResultToResponse("accept"))
      .flatMap(Future.fromTry)
  }

  override def rejectShareOffer(request: RejectShareOfferRequest): Future[Empty] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    darDistribution
      .reject(Converters.toContractId[M.ShareDar](request.id), request.reason)
      .map(acceptRejectResultToResponse("reject"))
      .flatMap(Future.fromTry)
  }

  private def acceptRejectResultToResponse(
      name: String
  )(result: Either[AcceptRejectError, Unit]): Try[Empty] = {
    import cats.syntax.either.*

    def errorToStatus: AcceptRejectError => Status = {
      case AcceptRejectError.OfferNotFound =>
        Status.NOT_FOUND.withDescription("Offer for dar was not found")
      case AcceptRejectError.SubmissionFailed(_) =>
        Status.INTERNAL.withDescription(s"Submission of $name to ledger failed")
      case AcceptRejectError.FailedToAppendDar(error) =>
        error.asGrpcError.getStatus
      case AcceptRejectError.InvalidOffer(error) =>
        Status.INTERNAL.withDescription(s"Invalid offer: $error")
    }

    result
      .bimap(errorToStatus, _ => Empty())
      .leftMap(_.asException())
      .toTry
  }

  override def whitelistAdd(request: WhitelistChangeRequest): Future[Empty] =
    for {
      _ <- darDistribution.whitelistAdd(Converters.toParty(request.partyId))
    } yield Empty()

  override def whitelistRemove(request: WhitelistChangeRequest): Future[Empty] =
    for {
      _ <- darDistribution.whitelistRemove(Converters.toParty(request.partyId))
    } yield Empty()

  override def whitelistList(request: Empty): Future[WhitelistListResponse] =
    for {
      parties <- darDistribution.whitelistList()
    } yield WhitelistListResponse(parties.map(Converters.toString))

  private def shareRequestItem(share: Contract[M.ShareDar]): v0.ListShareRequestsResponse.Item =
    v0.ListShareRequestsResponse.Item(
      contractIdToString(share.contractId),
      share.value.hash,
      Converters.toString(share.value.recipient),
      share.value.name,
    )

  private def shareOfferItem(share: Contract[M.ShareDar]): v0.ListShareOffersResponse.Item =
    v0.ListShareOffersResponse.Item(
      contractIdToString(share.contractId),
      share.value.hash,
      Converters.toString(share.value.owner),
      share.value.name,
    )

  private def contractIdToString(id: P.ContractId[M.ShareDar]): String =
    ApiTypes.ContractId.unwrap(id)

}
