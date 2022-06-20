// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.traverse._
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.admin.TransferService
import com.digitalasset.canton.participant.admin.v0._
import com.digitalasset.canton.participant.protocol.transfer.TransferData
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.protocol.{LfContractId, TransferId}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, OptionUtil}
import com.digitalasset.canton.version.HasProtoV0

import scala.concurrent.{ExecutionContext, Future}

class GrpcTransferService(service: TransferService)(implicit ec: ExecutionContext)
    extends TransferServiceGrpc.TransferService {

  import com.digitalasset.canton.networking.grpc.CantonGrpcUtil._

  override def transferOut(request: AdminTransferOutRequest): Future[AdminTransferOutResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      request match {
        case AdminTransferOutRequest(submittingPartyP, contractIdP, originDomainP, targetDomainP) =>
          val res = for {
            originDomain <- mapErr(DomainAlias.create(originDomainP))
            targetDomain <- mapErr(DomainAlias.create(targetDomainP))
            contractId <- mapErr(LfContractId.fromProtoPrimitive(contractIdP))
            submittingParty <- mapErr(
              EitherT.fromEither[Future](ProtoConverter.parseLfPartyId(submittingPartyP))
            )
            transferId <- mapErr(
              service.transferOut(submittingParty, contractId, originDomain, targetDomain)
            )
          } yield AdminTransferOutResponse(transferId = Some(transferId.toProtoV0))
          EitherTUtil.toFuture(res)
      }
    }

  override def transferIn(request: AdminTransferInRequest): Future[AdminTransferInResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      request match {
        case AdminTransferInRequest(submittingPartyId, targetDomainP, Some(transferIdP)) =>
          val res = for {
            targetDomain <- mapErr(EitherT.fromEither[Future](DomainAlias.create(targetDomainP)))
            submittingParty <- mapErr(
              EitherT.fromEither[Future](ProtoConverter.parseLfPartyId(submittingPartyId))
            )
            transferId <- mapErr(EitherT.fromEither[Future](TransferId.fromProtoV0(transferIdP)))
            _result <- mapErr(service.transferIn(submittingParty, targetDomain, transferId))
          } yield AdminTransferInResponse()
          EitherTUtil.toFuture(res)
        case AdminTransferInRequest(_, _, None) =>
          Future.failed(invalidArgument("TransferId not set in transfer-in request"))
      }
    }

  override def transferSearch(
      searchRequest: AdminTransferSearchQuery
  ): Future[AdminTransferSearchResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      searchRequest match {
        case AdminTransferSearchQuery(
              searchDomainP,
              filterOriginDomainP,
              filterTimestampP,
              filterSubmitterP,
              limit,
            ) =>
          val res = for {
            filterOriginDomain <- mapErr(DomainAlias.create(filterOriginDomainP))
            filterDomain = if (filterOriginDomainP == "") None else Some(filterOriginDomain)
            searchDomain <- mapErr(DomainAlias.create(searchDomainP))
            filterSubmitterO <- mapErr(
              OptionUtil
                .emptyStringAsNone(filterSubmitterP)
                .map(ProtoConverter.parseLfPartyId)
                .sequence
            )
            filterTimestampO <- mapErr(
              filterTimestampP.map(CantonTimestamp.fromProtoPrimitive).sequence
            )
            transferData <- mapErr(
              service.transferSearch(
                searchDomain,
                filterDomain,
                filterTimestampO,
                filterSubmitterO,
                limit.toInt,
              )
            )
          } yield {
            val searchResultsP = transferData.map(TransferSearchResult(_).toProtoV0)
            AdminTransferSearchResponse(results = searchResultsP)
          }
          EitherTUtil.toFuture(res)
      }
    }
}

case class TransferSearchResult(
    transferId: TransferId,
    submittingParty: String,
    targetDomain: String,
    originDomain: String,
    contractId: LfContractId,
    readyForTransferIn: Boolean,
) extends HasProtoV0[AdminTransferSearchResponse.TransferSearchResult] {
  override def toProtoV0: AdminTransferSearchResponse.TransferSearchResult =
    AdminTransferSearchResponse.TransferSearchResult(
      contractId = contractId.toProtoPrimitive,
      transferId = Some(transferId.toProtoV0),
      originDomain = originDomain,
      targetDomain = targetDomain,
      submittingParty = submittingParty,
      readyForTransferIn = readyForTransferIn,
    )
}

object TransferSearchResult {
  def fromProtoV0(
      resultP: AdminTransferSearchResponse.TransferSearchResult
  ): ParsingResult[TransferSearchResult] =
    resultP match {
      case AdminTransferSearchResponse
            .TransferSearchResult(
              contractIdP,
              transferIdP,
              originDomain,
              targetDomain,
              submitter,
              ready,
            ) =>
        for {
          _ <- Either.cond(!contractIdP.isEmpty, (), FieldNotSet("contractId"))
          contractId <- LfContractId.fromProtoPrimitive(contractIdP)
          transferId <- ProtoConverter
            .required("transferId", transferIdP)
            .flatMap(TransferId.fromProtoV0)
          _ <- Either.cond(!originDomain.isEmpty, (), FieldNotSet("originDomain"))
          _ <- Either.cond(!targetDomain.isEmpty, (), FieldNotSet("targetDomain"))
          _ <- Either.cond(!submitter.isEmpty, (), FieldNotSet("submitter"))
        } yield TransferSearchResult(
          transferId,
          submitter,
          targetDomain,
          originDomain,
          contractId,
          ready,
        )
    }

  def apply(transferData: TransferData): TransferSearchResult =
    TransferSearchResult(
      transferData.transferId,
      transferData.transferOutRequest.submitter,
      transferData.targetDomain.toProtoPrimitive,
      transferData.originDomain.toProtoPrimitive,
      transferData.contract.contractId,
      transferData.transferOutResult.isDefined,
    )
}
