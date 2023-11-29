// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.daml.error.ErrorCode
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.error.MediatorError.{InvalidMessage, MalformedMessage, Timeout}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{LocalReject, Verdict}
import com.digitalasset.canton.version.ProtocolVersion
import pprint.Tree

sealed trait MediatorVerdict extends Product with Serializable with PrettyPrinting {
  def toVerdict(protocolVersion: ProtocolVersion): Verdict
}

object MediatorVerdict {
  case object MediatorApprove extends MediatorVerdict {
    override def toVerdict(protocolVersion: ProtocolVersion): Verdict =
      Verdict.Approve(protocolVersion)

    override def pretty: Pretty[MediatorApprove] = prettyOfObject[MediatorApprove]
  }
  type MediatorApprove = MediatorApprove.type

  final case class ParticipantReject(reasons: NonEmpty[List[(Set[LfPartyId], LocalReject)]])
      extends MediatorVerdict {
    override def toVerdict(protocolVersion: ProtocolVersion): Verdict =
      Verdict.ParticipantReject(reasons, protocolVersion)

    override def pretty: Pretty[ParticipantReject] = {
      import Pretty.PrettyOps

      prettyOfClass(
        unnamedParam(
          _.reasons.map { case (parties, reason) =>
            Tree.Infix(reason.toTree, "- reported by:", parties.toTree)
          }
        )
      )
    }
  }

  final case class MediatorReject(reason: MediatorError) extends MediatorVerdict {
    override def toVerdict(protocolVersion: ProtocolVersion): Verdict.MediatorReject = {
      if (protocolVersion >= Verdict.MediatorRejectV2.firstApplicableProtocolVersion) {
        val error = reason match {
          case timeout: Timeout.Reject => timeout
          case invalid: InvalidMessage.Reject => invalid
          case malformed: MalformedMessage.Reject => malformed
        }
        Verdict.MediatorRejectV2.tryCreate(error.rpcStatusWithoutLoggingContext(), protocolVersion)
      } else {
        def from(cause: String, code: ErrorCode): Verdict.MediatorRejectV1 =
          Verdict.MediatorRejectV1.tryCreate(cause, code.id, code.category.asInt, protocolVersion)

        reason match {
          case timeout @ Timeout.Reject(cause, _unresponsiveParties) => from(cause, timeout.code)
          case invalid @ InvalidMessage.Reject(cause, _codeP) => from(cause, invalid.code)
          case malformed @ MalformedMessage.Reject(cause, _codeP) => from(cause, malformed.code)
        }
      }
    }

    override def pretty: Pretty[MediatorReject] = prettyOfClass(
      param("reason", _.reason)
    )
  }
}
