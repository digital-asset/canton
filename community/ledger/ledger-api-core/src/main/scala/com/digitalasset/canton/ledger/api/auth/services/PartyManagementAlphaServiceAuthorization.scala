// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.admin.party_management_alpha_service.*
import com.daml.ledger.api.v2.admin.party_management_alpha_service.PartyManagementAlphaServiceGrpc.PartyManagementAlphaService
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

final class PartyManagementAlphaServiceAuthorization(
    protected val service: PartyManagementAlphaService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends PartyManagementAlphaService
    with ProxyCloseable
    with GrpcApiService {
  import PartyManagementAlphaServiceAuthorization.*

  override def bindService(): ServerServiceDefinition =
    PartyManagementAlphaServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()

  override def exportPartyAcs(
      request: ExportPartyAcsRequest,
      responseObserver: StreamObserver[ExportPartyAcsResponse],
  ): Unit =
    authorizer.stream(service.exportPartyAcs)(exportPartyAcsClaims(request)*)(
      request,
      responseObserver,
    )

  // TODO(#31414): Figure out auth for OnPR requests including requests with
  //  grpc input/request stream. `AddPartyWithAcs` is unauthenticated atm,
  //  and the stabs at `ExportPartyAcs` and `GetAddPartyStatus` might be wrong
  //  or incomplete.
  override def addPartyWithAcs(
      responseObserver: StreamObserver[AddPartyWithAcsResponse]
  ): StreamObserver[AddPartyWithAcsRequest] = service.addPartyWithAcs(responseObserver)

  override def getAddPartyStatus(
      request: GetAddPartyStatusRequest
  ): Future[GetAddPartyStatusResponse] =
    authorizer.rpc(service.getAddPartyStatus)(getAddPartyStatusClaims*)(request)
}

object PartyManagementAlphaServiceAuthorization {
  def exportPartyAcsClaims(
      request: ExportPartyAcsRequest
  ): List[RequiredClaim[ExportPartyAcsRequest]] =
    RequiredClaims(
      RequiredClaim.AdminOrIdpAdminOrOperateAsParty[ExportPartyAcsRequest](Seq(request.partyId))
    )

  def getAddPartyStatusClaims: List[RequiredClaim[GetAddPartyStatusRequest]] =
    RequiredClaims(
      RequiredClaim.AdminOrIdpAdmin[GetAddPartyStatusRequest]()
    )
}
