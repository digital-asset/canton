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

  override def getAddPartyStatus(
      request: GetAddPartyStatusRequest
  ): Future[GetAddPartyStatusResponse] =
    authorizer.rpc(service.getAddPartyStatus)(getAddPartyStatusClaims*)(request)

  /** Generates a PartyToParticipant mapping topology transaction to onboard an already hosted
    * party.
    */
  override def generatePartyTopologyUpdate(
      request: GeneratePartyTopologyUpdateRequest
  ): Future[GeneratePartyTopologyUpdateResponse] =
    authorizer.rpc(service.generatePartyTopologyUpdate)(generatePartyTopologyUpdateClaims*)(request)

  /** Submits an optionally signed PartyToParticipant mapping topology transaction to onboard a
    * party.
    */
  override def authorizePartyUpdate(
      request: AuthorizePartyUpdateRequest
  ): Future[AuthorizePartyUpdateResponse] =
    authorizer.rpc(service.authorizePartyUpdate)(authorizePartyUpdateClaims*)(request)
}

object PartyManagementAlphaServiceAuthorization {

  def getAddPartyStatusClaims: List[RequiredClaim[GetAddPartyStatusRequest]] =
    RequiredClaims(
      RequiredClaim.AdminOrIdpAdmin[GetAddPartyStatusRequest]()
    )

  def generatePartyTopologyUpdateClaims: List[RequiredClaim[GeneratePartyTopologyUpdateRequest]] =
    RequiredClaims(
      RequiredClaim.AdminOrIdpAdmin[GeneratePartyTopologyUpdateRequest]()
    )

  def authorizePartyUpdateClaims: List[RequiredClaim[AuthorizePartyUpdateRequest]] =
    RequiredClaims(
      RequiredClaim.AdminOrIdpAdmin[AuthorizePartyUpdateRequest]()
    )
}
