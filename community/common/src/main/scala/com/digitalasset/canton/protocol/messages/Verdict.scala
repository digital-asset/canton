// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse._
import com.daml.error.ErrorCategory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  InvariantViolation,
  NotImplementedYet,
  ValueDeserializationError,
}
import com.digitalasset.canton.error._
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.protocol.v0.RejectionReason
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version._
import com.google.protobuf.empty
import pprint.Tree

sealed trait Verdict
    extends Product
    with Serializable
    with PrettyPrinting
    with HasProtocolVersionedWrapper[Verdict] {

  override protected def companionObj: HasProtocolVersionedWrapperCompanion[Verdict] = Verdict

  def toProtoV0: v0.Verdict

  def toProtoV1: v1.Verdict
}

object Verdict
    extends HasProtocolVersionedCompanion[Verdict]
    with ProtocolVersionedCompanionDbHelpers[Verdict] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersion(v0.Verdict)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtobufVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.v4,
      supportedProtoVersion(v1.Verdict)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  case class Approve(representativeProtocolVersion: RepresentativeProtocolVersion[Verdict])
      extends Verdict {
    override def toProtoV0: v0.Verdict =
      v0.Verdict(someVerdict = v0.Verdict.SomeVerdict.Approve(empty.Empty()))

    override def toProtoV1: v1.Verdict =
      v1.Verdict(someVerdict = v1.Verdict.SomeVerdict.Approve(empty.Empty()))

    override def pretty: Pretty[Verdict] = prettyOfString(_ => "Approve")
  }

  object Approve {
    def apply(protocolVersion: ProtocolVersion): Approve = Approve(
      Verdict.protocolVersionRepresentativeFor(protocolVersion)
    )
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
      v1.MediatorReject(cause = cause, errorCode = code.id, errorCategory = code.category.asInt)
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

      val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtobufVersion(0))

      import v0.MediatorRejection.Code
      codeP match {
        case Code.MissingCode => Left(FieldNotSet("MediatorReject.code"))
        case Code.InformeesNotHostedOnActiveParticipant | Code.InvalidRootHashMessage =>
          Right(MediatorError.InvalidMessage.Reject(reason, codeP)(representativeProtocolVersion))
        case Code.NotEnoughConfirmingParties | Code.ViewThresholdBelowMinimumThreshold |
            Code.WrongDeclaredMediator | Code.NonUniqueRequestUuid =>
          Right(MediatorError.MalformedMessage.Reject(reason, codeP)(representativeProtocolVersion))
        case Code.Timeout =>
          Right(MediatorError.Timeout.Reject(reason)(representativeProtocolVersion))
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
      val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtobufVersion(1))

      val v1.MediatorReject(cause, errorCodeP, errorCategoryP) = mediatorRejectP
      errorCodeP match {
        case MediatorError.Timeout.id =>
          Right(MediatorError.Timeout.Reject(cause)(representativeProtocolVersion))
        case MediatorError.InvalidMessage.id =>
          Right(MediatorError.InvalidMessage.Reject(cause)(representativeProtocolVersion))
        case MediatorError.MalformedMessage.id =>
          Right(MediatorError.MalformedMessage.Reject(cause)(representativeProtocolVersion))
        case id =>
          val category = ErrorCategory
            .fromInt(errorCategoryP)
            .getOrElse(ErrorCategory.SystemInternalAssumptionViolated)
          Right(MediatorError.GenericError(cause, id, category)(representativeProtocolVersion))
      }
    }
  }

  /** @param reasons Mapping from the parties of a [[com.digitalasset.canton.protocol.messages.MediatorResponse]]
    *                to the rejection reason from the [[com.digitalasset.canton.protocol.messages.MediatorResponse]]
    */
  case class ParticipantReject(reasons: NonEmpty[List[(Set[LfPartyId], LocalReject)]])(
      val representativeProtocolVersion: RepresentativeProtocolVersion[Verdict]
  ) extends Verdict {

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
        .maxBy1(_.code.category)
    }
  }

  object ParticipantReject {
    def apply(
        reasons: NonEmpty[List[(Set[LfPartyId], LocalReject)]],
        protocolVersion: ProtocolVersion,
    ): ParticipantReject =
      ParticipantReject(reasons)(Verdict.protocolVersionRepresentativeFor(protocolVersion))

    def fromProtoV0(rejectionReasonsP: v0.RejectionReasons): ParsingResult[ParticipantReject] = {
      val v0.RejectionReasons(reasonsP) = rejectionReasonsP
      fromProtoRejectionReasonsV0(reasonsP)
    }

    private def fromProtoRejectionReasonsV0(
        reasonsP: Seq[RejectionReason]
    ): ParsingResult[ParticipantReject] =
      for {
        reasons <- reasonsP.traverse(fromProtoReason)
        reasonsNE <- NonEmpty
          .from(reasons.toList)
          .toRight(InvariantViolation("Field reasons must not be empty!"))
      } yield ParticipantReject(reasonsNE)(
        Verdict.protocolVersionRepresentativeFor(ProtobufVersion(0))
      )

    def fromProtoV1(
        participantRejectP: v1.ParticipantReject
    ): ParsingResult[ParticipantReject] = {
      val v1.ParticipantReject(reasonsP) = participantRejectP
      fromProtoRejectionReasonsV0(reasonsP)
    }
  }

  override protected def name: String = "verdict"

  def fromProtoV0(verdictP: v0.Verdict): ParsingResult[Verdict] = {
    val v0.Verdict(someVerdictP) = verdictP
    import v0.Verdict.{SomeVerdict => V}

    val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtobufVersion(0))

    someVerdictP match {
      case V.Approve(empty.Empty(_)) => Right(Approve(representativeProtocolVersion))
      case V.Timeout(empty.Empty(_)) =>
        Right(MediatorError.Timeout.Reject()(representativeProtocolVersion))
      case V.MediatorReject(mediatorRejectionP) => MediatorReject.fromProtoV0(mediatorRejectionP)
      case V.ValidatorReject(rejectionReasonsP) => ParticipantReject.fromProtoV0(rejectionReasonsP)
      case V.Empty => Left(NotImplementedYet("empty verdict type"))
    }
  }

  def fromProtoV1(verdictP: v1.Verdict): ParsingResult[Verdict] = {
    val v1.Verdict(someVerdictP) = verdictP
    import v1.Verdict.{SomeVerdict => V}

    val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtobufVersion(1))

    someVerdictP match {
      case V.Approve(empty.Empty(_)) => Right(Approve(representativeProtocolVersion))
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
