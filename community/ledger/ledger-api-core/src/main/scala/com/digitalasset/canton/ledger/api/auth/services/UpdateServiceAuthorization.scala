// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.transaction_filter.UpdateFormat
import com.daml.ledger.api.v2.update_service.*
import com.daml.ledger.api.v2.update_service.UpdateServiceGrpc.UpdateService
import com.digitalasset.canton.auth.{Authorizer, ClaimApiArea, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import com.digitalasset.canton.ledger.api.auth.services.UpdateServiceAuthorization.getUpdateFormatClaimsAndFillReadAsParties
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.platform.apiserver.services.{
  ApiUpdateService,
  RequestWithReadAsParty,
}
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

final class UpdateServiceAuthorization(
    protected val service: ApiUpdateService,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends UpdateService
    with ProxyCloseable
    with GrpcApiService {

  override def bindService(): ServerServiceDefinition =
    UpdateServiceGrpc.bindService(this, executionContext)

  override def getUpdates(
      request: GetUpdatesRequest,
      responseObserver: StreamObserver[GetUpdatesResponse],
  ): Unit =
    authorizer.stream[RequestWithReadAsParty[GetUpdatesRequest], GetUpdatesResponse](
      service.getUpdates
    )(
      getUpdateFormatClaimsAndFillReadAsParties(request)(
        Lens.unit[GetUpdatesRequest].optionalUpdateFormat
      ).prepended(
        RequiredClaim.ApiArea(ClaimApiArea.ledgerRead.area)
      )*
    )(RequestWithReadAsParty(request, None), responseObserver)

  override def getUpdateByOffset(
      request: GetUpdateByOffsetRequest
  ): Future[GetUpdateResponse] =
    authorizer.rpc(service.getUpdateByOffset)(
      request.updateFormat.toList
        .flatMap(
          RequiredClaims.updateFormatClaims[GetUpdateByOffsetRequest]
        )
        .prepended(
          RequiredClaim.ApiArea(ClaimApiArea.ledgerRead.area)
        )*
    )(request)

  override def getUpdateById(
      request: GetUpdateByIdRequest
  ): Future[GetUpdateResponse] =
    authorizer.rpc(service.getUpdateById)(
      request.updateFormat.toList
        .flatMap(
          RequiredClaims.updateFormatClaims[GetUpdateByIdRequest]
        )
        .prepended(
          RequiredClaim.ApiArea(ClaimApiArea.ledgerRead.area)
        )*
    )(request)

  override def getUpdateByHash(request: GetUpdateByHashRequest): Future[GetUpdateResponse] =
    authorizer.rpc(service.getUpdateByHash)(
      request.updateFormat.toList
        .flatMap(
          RequiredClaims.updateFormatClaims[GetUpdateByHashRequest]
        )
        .prepended(
          RequiredClaim.ApiArea(ClaimApiArea.ledgerRead.area)
        )*
    )(request)

  override def getUpdatesPage(request: GetUpdatesPageRequest): Future[GetUpdatesPageResponse] =
    authorizer.rpc(service.getUpdatesPage)(
      getUpdateFormatClaimsAndFillReadAsParties(request)(
        Lens.unit[GetUpdatesPageRequest].optionalUpdateFormat
      ).prepended(
        RequiredClaim.ApiArea(ClaimApiArea.ledgerRead.area)
      )*
    )(RequestWithReadAsParty(request, None))
}

object UpdateServiceAuthorization {
  def getUpdateFormatClaimsAndFillReadAsParties[R](request: R)(
      updateFormatGetter: scalapb.lenses.Lens[R, Option[UpdateFormat]]
  ): Seq[RequiredClaim[RequestWithReadAsParty[R]]] =
    (if (needsReadAsParties(updateFormatGetter.get(request)))
       List(
         RequiredClaim.MatchReadAsParties(
           RequestWithReadAsParty.readAsPartyLens[R]
         )
       )
     else List()) ++ getUpdateFormatClaims[RequestWithReadAsParty[R]](
      updateFormatGetter
        .get(request)
    )

  def getUpdateFormatClaims[R](updateFormat: Option[UpdateFormat]): List[RequiredClaim[R]] =
    updateFormat.toList
      .flatMap(RequiredClaims.updateFormatClaims[R])

  private def needsReadAsParties(updateFormat: Option[UpdateFormat]) = updateFormat
    .map(uf =>
      uf.includeTransactions
        .flatMap(_.eventFormat)
        .exists(_.filtersByContractId.nonEmpty) || uf.includeReassignments
        .exists(_.filtersByContractId.nonEmpty) || uf.includeTransactionsAndReassignments
        .flatMap(_.eventFormat)
        .exists(_.filtersByContractId.nonEmpty)
    )
    .getOrElse(false)
}
