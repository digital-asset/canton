// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.v2.admin.party_management_alpha_service.{
  AuthorizePartyUpdateRequest,
  GeneratePartyTopologyUpdateRequest,
  GeneratePartyTopologyUpdateResponse,
  GetAddPartyStatusRequest,
  GetAddPartyStatusResponse,
}
import com.digitalasset.canton.topology.Party

import scala.concurrent.Future

/** Interface to enable wiring up Ledger API-exposed party replication endpoints with
  * canton-internal implementation.
  */
trait PartyReplicationEndpoints {

  /** Obtain online party replication status of an earlier [[addPartyWithAcs]] call */
  def getAddPartyStatus(request: GetAddPartyStatusRequest): Future[GetAddPartyStatusResponse]

  /** Generates a PartyToParticipant topology update transaction.
    *
    * Constructs a new PartyToParticipant mapping that adds the target participant with the
    * onboarding flag set to true, increments the serial, and returns the raw transaction bytes
    * alongside the hash that requires authorization signatures.
    */
  def generatePartyTopologyUpdate(
      request: GeneratePartyTopologyUpdateRequest
  ): Future[GeneratePartyTopologyUpdateResponse]

  /** Authorizes or proposes a PartyToParticipant topology update.
    *
    * Delegates the transaction to the topology manager, which will append the local node's
    * signature. The transaction is submitted as a proposal, allowing the topology processor to
    * automatically evaluate whether the accumulated signatures satisfy the authorization
    * requirements to fully authorize the transaction.
    *
    * @return
    *   A tuple of the `Party` and a boolean flag indicating if the executing node is one of the
    *   onboarding target participants. This flag instructs the API layer whether to execute local
    *   IAM provisioning.
    */
  def authorizePartyUpdate(
      request: AuthorizePartyUpdateRequest
  ): Future[(Party, Boolean)]

}
