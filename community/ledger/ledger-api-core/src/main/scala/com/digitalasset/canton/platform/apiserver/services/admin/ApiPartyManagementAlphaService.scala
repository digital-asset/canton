// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.v2.admin.party_management_alpha_service.PartyManagementAlphaServiceGrpc.PartyManagementAlphaService
import com.daml.ledger.api.v2.admin.party_management_alpha_service.{
  AddPartyWithAcsRequest,
  AddPartyWithAcsResponse,
  ExportPartyAcsRequest,
  ExportPartyAcsResponse,
  GetAddPartyStatusRequest,
  GetAddPartyStatusResponse,
  PartyManagementAlphaServiceGrpc,
}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiPartyManagementAlphaService(
    partyReplicationEndpoints: PartyReplicationEndpoints
)(implicit
    executionContext: ExecutionContext
) extends PartyManagementAlphaService
    with GrpcApiService
    with AuthenticatedUserContextResolver {

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    PartyManagementAlphaServiceGrpc.bindService(this, executionContext)

  override def exportPartyAcs(
      request: ExportPartyAcsRequest,
      responseObserver: StreamObserver[ExportPartyAcsResponse],
  ): Unit = partyReplicationEndpoints.exportPartyAcs(request, responseObserver)

  override def addPartyWithAcs(
      responseObserver: StreamObserver[AddPartyWithAcsResponse]
  ): StreamObserver[AddPartyWithAcsRequest] =
    partyReplicationEndpoints.addPartyWithAcs(responseObserver)

  override def getAddPartyStatus(
      request: GetAddPartyStatusRequest
  ): Future[GetAddPartyStatusResponse] =
    partyReplicationEndpoints.getAddPartyStatus(request)
}

private[apiserver] object ApiPartyManagementAlphaService {
  def createApiService(partyReplicationEndpoints: PartyReplicationEndpoints)(implicit
      executionContext: ExecutionContext
  ): PartyManagementAlphaService & GrpcApiService =
    new ApiPartyManagementAlphaService(partyReplicationEndpoints)
}
