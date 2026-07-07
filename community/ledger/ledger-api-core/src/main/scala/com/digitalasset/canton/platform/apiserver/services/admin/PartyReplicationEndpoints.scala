// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.v2.admin.party_management_alpha_service.{
  AddPartyWithAcsRequest,
  AddPartyWithAcsResponse,
  ExportPartyAcsRequest,
  ExportPartyAcsResponse,
  GetAddPartyStatusRequest,
  GetAddPartyStatusResponse,
}
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

/** Interface to enable wiring up Ledger API-exposed party replication endpoints with
  * canton-internal implementation.
  */
trait PartyReplicationEndpoints {

  /** Export party ACS */
  def exportPartyAcs(
      request: ExportPartyAcsRequest,
      responseObserver: StreamObserver[ExportPartyAcsResponse],
  ): Unit

  /** Import party ACS in "online" fashion (OnPR) */
  def addPartyWithAcs(
      responseObserver: StreamObserver[AddPartyWithAcsResponse]
  ): StreamObserver[AddPartyWithAcsRequest]

  /** Obtain online party replication status of an earlier [[addPartyWithAcs]] call */
  def getAddPartyStatus(request: GetAddPartyStatusRequest): Future[GetAddPartyStatusResponse]
}
