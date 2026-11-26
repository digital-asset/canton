// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.admin.party_management_service.*
import com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.digitalasset.canton.auth.{Authorizer, ClaimApiArea, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

final class PartyManagementServiceAuthorization(
    protected val service: PartyManagementService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends PartyManagementService
    with ProxyCloseable
    with GrpcApiService {
  import PartyManagementServiceAuthorization.*

  override def getParticipantId(
      request: GetParticipantIdRequest
  ): Future[GetParticipantIdResponse] =
    authorizer.rpc(service.getParticipantId)(
      RequiredClaim.ApiArea(ClaimApiArea.baseRead.area)
    )(request)

  override def getParties(request: GetPartiesRequest): Future[GetPartiesResponse] =
    authorizer.rpc(service.getParties)(
      getPartiesClaims(request)*
    )(request)

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] =
    authorizer.rpc(service.listKnownParties)(
      RequiredClaim.ApiArea(ClaimApiArea.partiesRead.area),
      RequiredClaim.MatchIdentityProviderId(
        Lens.unit[ListKnownPartiesRequest].identityProviderId
      ),
    )(request)

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] =
    authorizer.rpc(service.allocateParty)(
      allocatePartyClaims*
    )(request)

  override def updatePartyDetails(
      request: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] =
    authorizer.rpc(service.updatePartyDetails)(
      updatePartyDetailsClaims(request)*
    )(request)

  override def updatePartyIdentityProviderId(
      request: UpdatePartyIdentityProviderIdRequest
  ): Future[UpdatePartyIdentityProviderIdResponse] =
    authorizer.rpc(service.updatePartyIdentityProviderId)(
      RequiredClaim.ApiArea(ClaimApiArea.idpsWrite.area),
      RequiredClaim.ApiArea(ClaimApiArea.partiesWrite.area),
    )(request)

  override def generateExternalPartyTopology(
      request: GenerateExternalPartyTopologyRequest
  ): Future[GenerateExternalPartyTopologyResponse] =
    // Note that we can't match on the identity provider ID here, since the request doesn't carry one.
    // That is okay since it doesn't allow reading any data from other IDPs.
    authorizer.rpc(service.generateExternalPartyTopology)(
      RequiredClaim.PartiesReadOrSelfAllocate[GenerateExternalPartyTopologyRequest]()
    )(request)

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()

  override def allocateExternalParty(
      request: AllocateExternalPartyRequest
  ): Future[AllocateExternalPartyResponse] =
    authorizer.rpc(service.allocateExternalParty)(
      allocateExternalPartyClaims*
    )(request)
}

object PartyManagementServiceAuthorization {
  def updatePartyDetailsClaims(
      request: UpdatePartyDetailsRequest
  ): List[RequiredClaim[UpdatePartyDetailsRequest]] =
    (request.partyDetails match {
      case Some(_) =>
        List[RequiredClaim[UpdatePartyDetailsRequest]](
          RequiredClaim.ApiArea(ClaimApiArea.partiesWrite.area),
          RequiredClaim.MatchIdentityProviderId(
            Lens.unit[UpdatePartyDetailsRequest].partyDetails.identityProviderId
          ),
        )
      case None =>
        List[RequiredClaim[UpdatePartyDetailsRequest]](
          RequiredClaim.ApiArea(ClaimApiArea.partiesWrite.area)
        )
    })

  def getPartiesClaims(
      request: GetPartiesRequest
  ): List[RequiredClaim[GetPartiesRequest]] =
    RequiredClaims(
      RequiredClaim.PartiesReadOrSelfRead[GetPartiesRequest](request.parties),
      RequiredClaim.MatchIdentityProviderId(Lens.unit[GetPartiesRequest].identityProviderId),
    )

  def allocatePartyClaims: List[RequiredClaim[AllocatePartyRequest]] =
    RequiredClaims(
      RequiredClaim.PartiesWriteOrSelfAllocate[AllocatePartyRequest](
        Lens.unit[AllocatePartyRequest].userId
      ),
      RequiredClaim.MatchIdentityProviderId(Lens.unit[AllocatePartyRequest].identityProviderId),
    )

  def allocateExternalPartyClaims: List[RequiredClaim[AllocateExternalPartyRequest]] =
    RequiredClaims(
      RequiredClaim.PartiesWriteOrSelfAllocate[AllocateExternalPartyRequest](
        Lens.unit[AllocateExternalPartyRequest].userId
      ),
      RequiredClaim.MatchIdentityProviderId(
        Lens.unit[AllocateExternalPartyRequest].identityProviderId
      ),
    )
}
