// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.state_service.*
import com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateService
import com.digitalasset.canton.auth.{Authorizer, ClaimApiArea, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import com.digitalasset.canton.ledger.api.auth.services.StateServiceAuthorization.{
  getActiveContractsClaims,
  getActiveContractsPageClaims,
  getConnectedSynchronizersClaims,
  getHostedPartiesClaims,
  getHostedPartiesPageClaims,
}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

final class StateServiceAuthorization(
    protected val service: StateService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends StateService
    with ProxyCloseable
    with GrpcApiService {

  override def getActiveContracts(
      request: GetActiveContractsRequest,
      responseObserver: StreamObserver[GetActiveContractsResponse],
  ): Unit =
    authorizer.stream(service.getActiveContracts)(
      getActiveContractsClaims(request)*
    )(request, responseObserver)

  override def getActiveContractsPage(
      request: GetActiveContractsPageRequest
  ): Future[GetActiveContractsPageResponse] =
    authorizer.rpc(service.getActiveContractsPage)(
      getActiveContractsPageClaims(request)*
    )(request)

  override def getConnectedSynchronizers(
      request: GetConnectedSynchronizersRequest
  ): Future[GetConnectedSynchronizersResponse] =
    authorizer.rpc(service.getConnectedSynchronizers)(
      getConnectedSynchronizersClaims(request)*
    )(request)

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] =
    authorizer.rpc(service.getLedgerEnd)(
      RequiredClaim.ApiArea(ClaimApiArea.baseRead.area)
    )(request)

  override def getLatestPrunedOffsets(
      request: GetLatestPrunedOffsetsRequest
  ): Future[GetLatestPrunedOffsetsResponse] =
    authorizer.rpc(service.getLatestPrunedOffsets)(
      RequiredClaim.ApiArea(ClaimApiArea.baseRead.area)
    )(request)

  override def bindService(): ServerServiceDefinition =
    StateServiceGrpc.bindService(this, executionContext)

  /** Translate syncrhonizer record time to an offset
    */
  override def convertRecordTimeToOffset(
      request: ConvertRecordTimeToOffsetRequest
  ): Future[ConvertRecordTimeToOffsetResponse] =
    authorizer.rpc(service.convertRecordTimeToOffset)(
      RequiredClaim.ApiArea(ClaimApiArea.baseRead.area)
    )(request)

  override def getHostedParties(
      request: GetHostedPartiesRequest,
      responseObserver: StreamObserver[GetHostedPartiesResponse],
  ): Unit =
    authorizer.stream(service.getHostedParties)(
      getHostedPartiesClaims(request)*
    )(request, responseObserver)

  override def getHostedPartiesPage(
      request: GetHostedPartiesPageRequest
  ): Future[GetHostedPartiesPageResponse] =
    authorizer.rpc(service.getHostedPartiesPage)(
      getHostedPartiesPageClaims(request)*
    )(request)
}

object StateServiceAuthorization {

  private val partyFilterL: Lens[HostedPartiesFilter, Set[String]] =
    Lens[HostedPartiesFilter, Set[String]](_.partyFilter.toSet)((filter, parties) =>
      filter.copy(partyFilter = parties.toSeq.sorted)
    )

  val partiesForGetHostedPartiesL: Lens[GetHostedPartiesRequest, Set[String]] =
    Lens.unit[GetHostedPartiesRequest].filter.compose(partyFilterL)

  val partiesForGetHostedPartiesPageL: Lens[GetHostedPartiesPageRequest, Set[String]] =
    Lens.unit[GetHostedPartiesPageRequest].filter.compose(partyFilterL)

  // TODO(#38113): Do not fail with PERMISSION_DENIED for callers without readable parties
  def getHostedPartiesClaims(
      request: GetHostedPartiesRequest
  ): List[RequiredClaim[GetHostedPartiesRequest]] =
    RequiredClaim.ApiArea[GetHostedPartiesRequest](ClaimApiArea.ledgerRead.area) ::
      Option
        .when(request.filter.isEmpty)(RequiredClaim.MatchReadAsParties(partiesForGetHostedPartiesL))
        .toList

  // TODO(#38113): Do not fail with PERMISSION_DENIED for callers without readable parties
  def getHostedPartiesPageClaims(
      request: GetHostedPartiesPageRequest
  ): List[RequiredClaim[GetHostedPartiesPageRequest]] =
    RequiredClaim.ApiArea[GetHostedPartiesPageRequest](ClaimApiArea.ledgerRead.area) ::
      Option
        .when(request.filter.isEmpty)(
          RequiredClaim.MatchReadAsParties(partiesForGetHostedPartiesPageL)
        )
        .toList

  def getActiveContractsClaims(
      request: GetActiveContractsRequest
  ): List[RequiredClaim[GetActiveContractsRequest]] =
    RequiredClaim.ApiArea[GetActiveContractsRequest](ClaimApiArea.ledgerRead.area) ::
      request.eventFormat.toList.flatMap(
        RequiredClaims.eventFormatClaims[GetActiveContractsRequest]
      )

  def getActiveContractsPageClaims(
      request: GetActiveContractsPageRequest
  ): List[RequiredClaim[GetActiveContractsPageRequest]] =
    RequiredClaim.ApiArea[GetActiveContractsPageRequest](ClaimApiArea.ledgerRead.area) ::
      request.eventFormat.toList.flatMap(
        RequiredClaims.eventFormatClaims[GetActiveContractsPageRequest]
      )

  val identityProviderIdForGetConnectedSynchronizersL
      : Lens[GetConnectedSynchronizersRequest, String] =
    Lens.unit[GetConnectedSynchronizersRequest].identityProviderId

  def getConnectedSynchronizersClaims(
      request: GetConnectedSynchronizersRequest
  ): List[RequiredClaim[GetConnectedSynchronizersRequest]] =
    if (request.party.isEmpty)
      List(
        RequiredClaim.ApiArea[GetConnectedSynchronizersRequest](ClaimApiArea.baseRead.area)
      )
    else
      List[RequiredClaim[GetConnectedSynchronizersRequest]](
        RequiredClaim.PartiesReadOrSelfRead(Seq(request.party)),
        RequiredClaim.MatchIdentityProviderId(identityProviderIdForGetConnectedSynchronizersL),
      )
}
