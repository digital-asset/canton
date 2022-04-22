// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse._
import com.daml.error.{ErrorCategory, ErrorGroup, Explanation, Resolution}
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  NotImplementedYet,
  ValueDeserializationError,
}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.MediatorRejectionGroup
import com.digitalasset.canton.error._
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.protocol.v0.MediatorRejection.Code
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasProtoV0,
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.google.protobuf.empty
import org.slf4j.event.Level
import pprint.Tree

sealed trait Verdict
    extends Product
    with Serializable
    with PrettyPrinting
    with HasVersionedWrapper[VersionedMessage[Verdict]]
    with HasProtoV0[v0.Verdict] {
  override def toProtoVersioned(version: ProtocolVersion): VersionedMessage[Verdict] =
    VersionedMessage(toProtoV0.toByteString, 0)

  override def toProtoV0: v0.Verdict
}

object Verdict extends HasVersionedMessageCompanion[Verdict] {
  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.Verdict)(fromProtoV0)
  )

  case object Approve extends Verdict {
    override def toProtoV0: v0.Verdict =
      v0.Verdict(someVerdict = v0.Verdict.SomeVerdict.Approve(empty.Empty()))

    override def pretty: Pretty[Approve.type] = prettyOfObject[Approve.type]
  }

  sealed trait MediatorReject
      extends Verdict
      with TransactionErrorWithEnum[v0.MediatorRejection.Code]
      with HasProtoV0[v0.Verdict] {

    def reason: String

    def toProtoMediatorRejectV0: v0.MediatorRejection = v0.MediatorRejection(
      code.protoCode,
      reason,
    )

    override def toProtoV0: v0.Verdict =
      v0.Verdict(someVerdict = v0.Verdict.SomeVerdict.MediatorReject(toProtoMediatorRejectV0))

    override def pretty: Pretty[this.type] =
      prettyOfClass(param("id", _.code.id.unquoted), param("reason", _.reason.unquoted))
  }

  type MediatorRejectErrorCode = ErrorCodeWithEnum[v0.MediatorRejection.Code]
  abstract class MediatorRejectError(
      override val cause: String,
      override val throwableO: Option[Throwable] = None,
  )(implicit override val code: MediatorRejectErrorCode)
      extends TransactionErrorWithEnumImpl[v0.MediatorRejection.Code](
        cause,
        throwableO,
        // All mediator rejections are reported asynchronously and are therefore covered by the submission rank guarantee
        definiteAnswer = true,
      )

  object MediatorReject extends MediatorRejectionGroup {

    def fromProtoV0(
        value: v0.MediatorRejection
    ): ParsingResult[MediatorReject] =
      (value.code match {
        case Code.MissingCode => Left(FieldNotSet("MediatorReject.code"))
        case Code.InformeesNotHostedOnActiveParticipant =>
          Right(Topology.InformeesNotHostedOnActiveParticipants.Reject)
        case Code.NotEnoughConfirmingParties =>
          Right(MaliciousSubmitter.NotEnoughConfirmingParties.Reject)
        case Code.ViewThresholdBelowMinimumThreshold =>
          Right(MaliciousSubmitter.ViewThresholdBelowMinimumThreshold.Reject)
        case Code.InvalidRootHashMessage => Right(Topology.InvalidRootHashMessages.Reject)
        case Code.Timeout => Right(Timeout.Reject)
        case Code.WrongDeclaredMediator => Right(MaliciousSubmitter.WrongDeclaredMediator.Reject)
        case Code.Unrecognized(code) =>
          Left(
            ValueDeserializationError(
              "reject",
              s"Unknown mediator rejection error code ${code} with ${value.reason}",
            )
          )
      }).map(rej => rej(value.reason))

    object Topology extends ErrorGroup() {

      @Explanation(
        """The transaction is referring to informees that are not hosted on any active participant on this domain."""
      )
      @Resolution(
        "This error can happen either if the transaction is racing with a topology state change, or due to malicious or faulty behaviour."
      )
      object InformeesNotHostedOnActiveParticipants
          extends MediatorRejectErrorCode(
            id = "MEDIATOR_SAYS_INFORMEES_NOT_HOSTED_ON_ACTIVE_PARTICIPANTS",
            ErrorCategory.InvalidGivenCurrentSystemStateOther,
            v0.MediatorRejection.Code.InformeesNotHostedOnActiveParticipant,
          ) {

        override def logLevel: Level = Level.WARN

        case class Reject(override val reason: String)
            extends MediatorRejectError(
              cause =
                "Rejected transaction due to informees not being hosted on an active participant"
            )
            with MediatorReject
      }

      @Explanation(
        """This rejection indicates that a submitter has sent a view with invalid root hash messages."""
      )
      @Resolution(
        "This error can happen either if the transaction is racing with a topology state change, or due to malicious or faulty behaviour."
      )
      object InvalidRootHashMessages
          extends MediatorRejectErrorCode(
            id = "MEDIATOR_SAYS_INVALID_ROOT_HASH_MESSAGES",
            ErrorCategory.InvalidGivenCurrentSystemStateOther,
            v0.MediatorRejection.Code.InvalidRootHashMessage,
          ) {

        override def logLevel: Level = Level.WARN

        case class Reject(override val reason: String)
            extends MediatorRejectError(
              cause = "Rejected transaction due to invalid root hash messages"
            )
            with MediatorReject

      }
    }

    @Explanation(
      """This rejection indicates that the transaction has been rejected by the mediator as it didn't receive enough confirmations within the confirmation timeout window."""
    )
    @Resolution(
      "Check that all involved participants are available and not overloaded."
    )
    object Timeout
        extends MediatorRejectErrorCode(
          id = "MEDIATOR_SAYS_TX_TIMED_OUT",
          ErrorCategory.ContentionOnSharedResources,
          v0.MediatorRejection.Code.Timeout,
        ) {

      case class Reject(reason: String = "")
          extends MediatorRejectError(
            cause =
              "Rejected transaction as the mediator did not receive sufficient confirmations within the expected timeframe"
          )
          with MediatorReject
    }

    object MaliciousSubmitter extends ErrorGroup {

      @Explanation(
        """This rejection indicates that a submitter has sent a manipulated view."""
      )
      @Resolution("Investigate whether the submitter is faulty or malicious.")
      object ViewThresholdBelowMinimumThreshold
          extends MediatorRejectErrorCode(
            id = "MEDIATOR_SAYS_VIEW_THRESHOLD_BELOW_MINIMUM_THRESHOLD",
            ErrorCategory.MaliciousOrFaultyBehaviour,
            v0.MediatorRejection.Code.ViewThresholdBelowMinimumThreshold,
          ) {

        case class Reject(override val reason: String)
            extends MediatorRejectError(
              cause =
                "Rejected transaction as a view has threshold below the confirmation policy's minimum threshold"
            )
            with MediatorReject

      }

      @Explanation(
        """This rejection indicates that a submitter has sent a manipulated view."""
      )
      @Resolution("Investigate whether the submitter is faulty or malicious.")
      object NotEnoughConfirmingParties
          extends MediatorRejectErrorCode(
            id = "MEDIATOR_SAYS_NOT_ENOUGH_CONFIRMING_PARTIES",
            ErrorCategory.MaliciousOrFaultyBehaviour,
            v0.MediatorRejection.Code.NotEnoughConfirmingParties,
          ) {

        case class Reject(override val reason: String)
            extends MediatorRejectError(
              cause = "Rejected transaction as a view has not enough confirming parties"
            )
            with MediatorReject

      }

      @Explanation(
        """This rejection indicates that the submitter sent the request to the wrong mediator"""
      )
      @Resolution("Investigate whether the submitter is faulty or malicious.")
      object WrongDeclaredMediator
          extends MediatorRejectErrorCode(
            id = "MEDIATOR_SAYS_DECLARED_MEDIATOR_IS_WRONG",
            ErrorCategory.MaliciousOrFaultyBehaviour,
            v0.MediatorRejection.Code.WrongDeclaredMediator,
          ) {

        case class Reject(override val reason: String)
            extends MediatorRejectError(
              cause =
                "The declared mediator in the MediatorRequest is not the mediator that received the request"
            )
            with MediatorReject

      }
    }

  }

  /** @param reasons Mapping from the parties of a [[com.digitalasset.canton.protocol.messages.MediatorResponse]]
    *                to the rejection reason from the [[com.digitalasset.canton.protocol.messages.MediatorResponse]]
    */
  case class RejectReasons(reasons: List[(Set[LfPartyId], LocalReject)]) extends Verdict {

    override def toProtoV0: v0.Verdict = {
      val reasonsP = v0.RejectionReasons(reasons.map { case (parties, message) =>
        v0.RejectionReason(parties.toSeq, Some(message.toLocalRejectProtoV0))
      })
      v0.Verdict(someVerdict = v0.Verdict.SomeVerdict.ValidatorReject(reasonsP))
    }

    override def pretty: Pretty[RejectReasons] = {
      implicit val pr: Pretty[List[(Set[LfPartyId], LocalReject)]] = prettyReasons
      prettyOfClass(unnamedParam(_.reasons))
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

  /** Make this an implicit, if you need to pretty print rejection reasons.
    */
  val prettyReasons: Pretty[List[(Set[LfPartyId], LocalReject)]] = reasons => {
    import Pretty._
    reasons.map { case (parties, reason) =>
      Tree.Infix(reason.toTree, "- reported by:", parties.toTree)
    }.toTree
  }

  case object Timeout extends Verdict {
    override def toProtoV0: v0.Verdict =
      v0.Verdict(someVerdict = v0.Verdict.SomeVerdict.Timeout(empty.Empty()))

    override def pretty: Pretty[Timeout.type] = prettyOfObject[Timeout.type]
  }

  override protected def name: String = "verdict"

  def fromProtoV0(protoVerdict: v0.Verdict): ParsingResult[Verdict] = {
    import v0.Verdict.{SomeVerdict => V}
    protoVerdict match {
      case v0.Verdict(V.Approve(empty.Empty(_))) => Right(Approve)
      case v0.Verdict(V.Timeout(empty.Empty(_))) => Right(Timeout)
      case v0.Verdict(V.MediatorReject(reason)) => MediatorReject.fromProtoV0(reason)
      case v0.Verdict(V.ValidatorReject(protoReasons)) =>
        for {
          reasons <- protoReasons.reasons.toList.traverse(fromProtoReason)
        } yield RejectReasons(reasons)
      case unknownError => Left(NotImplementedYet(unknownError.getClass.getName))
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
