// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  NotImplementedYet,
  ValueDeserializationError,
}
import com.digitalasset.canton.error._
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version._
import com.google.protobuf.empty
import pprint.Tree

sealed trait Verdict
    extends Product
    with Serializable
    with PrettyPrinting
    with HasVersionedWrapper[VersionedMessage[Verdict]] {
  override def toProtoVersioned(version: ProtocolVersion): VersionedMessage[Verdict] = {
    if (version < ProtocolVersion.unstable_development) // TODO(i10131): replace by stable version
      VersionedMessage(toProtoV0.toByteString, 0)
    else
      VersionedMessage(toProtoV1.toByteString, 1)
  }

  def toProtoV0: v0.Verdict

  def toProtoV1: v1.Verdict
}

object Verdict extends HasVersionedMessageCompanion[Verdict] {
  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.Verdict)(fromProtoV0),
    1 -> supportedProtoVersion(v1.Verdict)(fromProtoV1),
  )

  case object Approve extends Verdict {
    override def toProtoV0: v0.Verdict =
      v0.Verdict(someVerdict = v0.Verdict.SomeVerdict.Approve(empty.Empty()))

    override def toProtoV1: v1.Verdict =
      v1.Verdict(someVerdict = v1.Verdict.SomeVerdict.Approve(empty.Empty()))

    override def pretty: Pretty[Approve.type] = prettyOfObject[Approve.type]
  }

  trait MediatorReject extends Verdict with TransactionError {

    /** The code from the v0 proto message format.
      * Implementations should try to choose the most appropriate code (in doubt: TIMEOUT).
      * DO NOT choose MISSING_CODE, because that will crash during deserialization.
      */
    // Name starts with _ to exclude from MDC.
    def _v0CodeP: v0.MediatorRejection.Code

    override def toProtoV0: v0.Verdict =
      v0.Verdict(someVerdict = v0.Verdict.SomeVerdict.MediatorReject(toProtoMediatorRejectionV0))

    def toProtoMediatorRejectionV0: v0.MediatorRejection =
      v0.MediatorRejection(code = _v0CodeP, reason = cause)

    override def toProtoV1: v1.Verdict =
      v1.Verdict(someVerdict = v1.Verdict.SomeVerdict.MediatorReject(toProtoMediatorRejectV1))

    def toProtoMediatorRejectV1: v1.MediatorReject = {
      v1.MediatorReject(cause = cause, errorCode = code.id)
    }

    override def pretty: Pretty[MediatorReject] =
      prettyOfClass(
        param("id", _.code.id.unquoted),
        param("cause", _.cause.unquoted),
        paramIfDefined("throwable", _.throwableO),
        param("v0CodeP", _._v0CodeP.name.unquoted),
      )
  }

  object MediatorReject {

    def fromProtoV0(
        value: v0.MediatorRejection
    ): ParsingResult[MediatorReject] = {
      val v0.MediatorRejection(codeP, reason) = value

      import v0.MediatorRejection.Code
      codeP match {
        case Code.MissingCode => Left(FieldNotSet("MediatorReject.code"))
        case Code.InformeesNotHostedOnActiveParticipant | Code.InvalidRootHashMessage =>
          Right(MediatorError.InvalidMessage.Reject(reason, codeP))
        case Code.NotEnoughConfirmingParties | Code.ViewThresholdBelowMinimumThreshold |
            Code.WrongDeclaredMediator | Code.NonUniqueRequestUuid =>
          Right(MediatorError.MalformedMessage.Reject(reason, codeP))
        case Code.Timeout => Right(MediatorError.Timeout.Reject(reason))
        case Code.Unrecognized(code) =>
          Left(
            ValueDeserializationError(
              "reject",
              s"Unknown mediator rejection error code ${code} with ${reason}",
            )
          )
      }
    }

    def fromProtoV1(mediatorRejectP: v1.MediatorReject): ParsingResult[MediatorReject] = {
      val v1.MediatorReject(cause, errorCode) = mediatorRejectP
      errorCode match {
        case MediatorError.Timeout.id => Right(MediatorError.Timeout.Reject(cause))
        case MediatorError.InvalidMessage.id => Right(MediatorError.InvalidMessage.Reject(cause))
        case MediatorError.MalformedMessage.id =>
          Right(MediatorError.MalformedMessage.Reject(cause))
        case _ => Right(MediatorError.GenericError(cause, errorCode))
      }
    }
  }

  /** @param reasons Mapping from the parties of a [[com.digitalasset.canton.protocol.messages.MediatorResponse]]
    *                to the rejection reason from the [[com.digitalasset.canton.protocol.messages.MediatorResponse]]
    */
  case class ParticipantReject(reasons: List[(Set[LfPartyId], LocalReject)]) extends Verdict {

    override def toProtoV0: v0.Verdict = {
      val reasonsP = v0.RejectionReasons(reasons.map { case (parties, message) =>
        v0.RejectionReason(parties.toSeq, Some(message.toLocalRejectProtoV0))
      })
      v0.Verdict(someVerdict = v0.Verdict.SomeVerdict.ValidatorReject(reasonsP))
    }

    override def toProtoV1: v1.Verdict = {
      val reasonsP = v1.ParticipantReject(reasons.map { case (parties, message) =>
        v0.RejectionReason(parties.toSeq, Some(message.toLocalRejectProtoV0))
      })
      v1.Verdict(someVerdict = v1.Verdict.SomeVerdict.ParticipantReject(reasonsP))
    }

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

    /** Returns the rejection reason with the highest [[com.daml.error.ErrorCategory]] */
    def keyEvent(implicit loggingContext: ErrorLoggingContext): LocalReject = {
      if (reasons.lengthCompare(1) > 0) {
        val message = show"Request was rejected with multiple reasons. $reasons"
        loggingContext.logger.info(message)(loggingContext.traceContext)
      }
      reasons
        .map(_._2)
        .maxByOption(_.code.category)
        .getOrElse(LocalReject.MalformedRejects.EmptyRejection.Reject())
    }
  }

  object ParticipantReject {
    def fromProtoV0(rejectionReasonsP: v0.RejectionReasons): ParsingResult[ParticipantReject] = {
      val v0.RejectionReasons(reasonsP) = rejectionReasonsP
      for {
        reasons <- reasonsP.traverse(fromProtoReason)
      } yield ParticipantReject(reasons.toList)
    }

    def fromProtoV1(
        participantRejectP: v1.ParticipantReject
    ): ParsingResult[ParticipantReject] = {
      val v1.ParticipantReject(reasonsP) = participantRejectP
      for {
        reasons <- reasonsP.traverse(fromProtoReason)
      } yield ParticipantReject(reasons.toList)
    }
  }

  override protected def name: String = "verdict"

  def fromProtoV0(verdictP: v0.Verdict): ParsingResult[Verdict] = {
    val v0.Verdict(someVerdictP) = verdictP
    import v0.Verdict.{SomeVerdict => V}
    someVerdictP match {
      case V.Approve(empty.Empty(_)) => Right(Approve)
      case V.Timeout(empty.Empty(_)) => Right(MediatorError.Timeout.Reject())
      case V.MediatorReject(mediatorRejectionP) => MediatorReject.fromProtoV0(mediatorRejectionP)
      case V.ValidatorReject(rejectionReasonsP) => ParticipantReject.fromProtoV0(rejectionReasonsP)
      case V.Empty => Left(NotImplementedYet("empty verdict type"))
    }
  }

  def fromProtoV1(verdictP: v1.Verdict): ParsingResult[Verdict] = {
    val v1.Verdict(someVerdictP) = verdictP
    import v1.Verdict.{SomeVerdict => V}
    someVerdictP match {
      case V.Approve(empty.Empty(_)) => Right(Approve)
      case V.MediatorReject(mediatorRejectP) => MediatorReject.fromProtoV1(mediatorRejectP)
      case V.ParticipantReject(participantRejectP) =>
        ParticipantReject.fromProtoV1(participantRejectP)
      case V.Empty => Left(NotImplementedYet("empty verdict type"))
    }
  }

  private def fromProtoReason(
      protoReason: v0.RejectionReason
  ): ParsingResult[(Set[LfPartyId], LocalReject)] = {
    val v0.RejectionReason(partiesP, messageP) = protoReason
    for {
      parties <- partiesP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      message <- ProtoConverter.parseRequired(LocalReject.fromProtoV0, "reject", messageP)
    } yield (parties, message)
  }
}
