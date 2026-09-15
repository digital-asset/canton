// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.error.MediatorError.{
  DuplicateConfirmationRequest,
  InvalidMessage,
  MalformedMessage,
  Timeout,
}
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.messages.{NonPositiveLocalVerdict, Verdict}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty
import pprint.Tree

sealed trait MediatorVerdict extends Product with Serializable with PrettyPrintingFromCompanion {
  def toVerdict(protocolVersion: ProtocolVersion): Verdict
}

object MediatorVerdict {
  case object MediatorApprove extends MediatorVerdict {
    override def toVerdict(protocolVersion: ProtocolVersion): Verdict =
      Verdict.Approve(protocolVersion)

    override def prettyCompanion: PrettyPrintingCompanion[MediatorApprove.this.type] =
      MediatorApprovePrettyPrintingCompanion
  }
  type MediatorApprove = MediatorApprove.type

  private object MediatorApprovePrettyPrintingCompanion
      extends PrettyPrintingCompanion[MediatorApprove] {
    override protected val pretty: Pretty[MediatorApprove] = prettyOfObject[MediatorApprove]
  }

  final case class ParticipantReject(
      reasons: NonEmpty[List[(Set[LfPartyId], ParticipantId, NonPositiveLocalVerdict)]]
  ) extends MediatorVerdict {
    override def toVerdict(protocolVersion: ProtocolVersion): Verdict =
      Verdict.ParticipantReject(reasons, protocolVersion)

    override def prettyCompanion: PrettyPrintingCompanion[ParticipantReject] = ParticipantReject
  }

  object ParticipantReject extends PrettyPrintingCompanion[ParticipantReject] {
    override protected val pretty: Pretty[ParticipantReject] = {
      import Pretty.PrettyOps

      prettyOfClass(
        unnamedParam(
          _.reasons.map { case (parties, participantId, reason) =>
            Tree.Infix(reason.toTree, s"- reported by $participantId for:", parties.toTree)
          }
        )
      )
    }
  }

  final case class MediatorReject(reason: MediatorError) extends MediatorVerdict {
    override def toVerdict(protocolVersion: ProtocolVersion): Verdict.MediatorReject = {
      val error = reason match {
        case timeout: Timeout.Reject => timeout
        case invalid: InvalidMessage.Reject => invalid
        case malformed: MalformedMessage.Reject => malformed
        case duplicate: DuplicateConfirmationRequest.Reject => duplicate
      }

      Verdict.MediatorReject.tryCreate(
        error.rpcStatusWithoutLoggingContext(),
        reason.isMalformed,
        protocolVersion,
      )
    }

    override def prettyCompanion: PrettyPrintingCompanion[MediatorReject] = MediatorReject
  }

  object MediatorReject extends PrettyPrintingCompanion[MediatorReject] {
    override protected val pretty: Pretty[MediatorReject] = prettyOfClass(
      param("reason", _.reason)
    )
  }
}
