// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{Informee, ViewType}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{ConfirmationPolicy, RequestId, RootHash, ViewHash}
import com.digitalasset.canton.topology.MediatorId

import java.util.UUID

trait MediatorRequest extends ProtocolMessage {
  def requestUuid: UUID

  def mediatorId: MediatorId

  def informeesAndThresholdByView: Map[ViewHash, (Set[Informee], NonNegativeInt)]

  def allInformees: Set[LfPartyId] =
    informeesAndThresholdByView
      .flatMap { case (_, (informees, _)) =>
        informees
      }
      .map(_.party)
      .toSet

  def createMediatorResult(
      requestId: RequestId,
      verdict: Verdict,
      recipientParties: Set[LfPartyId],
  ): MediatorResult with SignedProtocolMessageContent

  def confirmationPolicy: ConfirmationPolicy

  /** Returns the hash that all [[com.digitalasset.canton.protocol.messages.RootHashMessage]]s of the request batch should contain.
    * [[scala.None$]] indicates that no [[com.digitalasset.canton.protocol.messages.RootHashMessage]] should be in the batch.
    */
  def rootHash: Option[RootHash]

  def viewType: ViewType
}

object MediatorRequest {
  implicit val mediatorRequestProtocolMessageContentCast
      : ProtocolMessageContentCast[MediatorRequest] = {
    case m: MediatorRequest => Some(m)
    case _ => None
  }
}
