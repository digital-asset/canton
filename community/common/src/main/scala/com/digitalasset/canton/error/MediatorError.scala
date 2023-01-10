// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.*
import com.digitalasset.canton.error.CantonErrorGroups.MediatorErrorGroup
import com.digitalasset.canton.protocol.messages.Verdict
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.version.{ProtocolVersion, RepresentativeProtocolVersion}
import org.slf4j.event.Level

object MediatorError extends MediatorErrorGroup {

  @Explanation(
    """This rejection indicates that the transaction has been rejected by the mediator as it didn't receive enough confirmations within the participant response timeout."""
  )
  @Resolution(
    "Check that all involved participants are available and not overloaded."
  )
  object Timeout
      extends ErrorCode(
        id = "MEDIATOR_SAYS_TX_TIMED_OUT",
        ErrorCategory.ContentionOnSharedResources,
      ) {

    case class Reject(
        override val cause: String =
          "Rejected transaction as the mediator did not receive sufficient confirmations within the expected timeframe."
    )(
        val representativeProtocolVersion: RepresentativeProtocolVersion[Verdict]
    ) extends BaseCantonError.Impl(cause)
        with MediatorReject {

      override def _v0CodeP: v0.MediatorRejection.Code = v0.MediatorRejection.Code.Timeout
    }

    object Reject {
      def create(protocolVersion: ProtocolVersion): Reject =
        Reject()(Verdict.protocolVersionRepresentativeFor(protocolVersion))
    }
  }

  @Explanation(
    """The mediator has received an invalid message (request or response).
      |The message will be discarded. As a consequence, the underlying request may be rejected.
      |No corruption of the ledger is to be expected."""
  )
  @Resolution("Address the cause of the error. Let the submitter retry the command.")
  object InvalidMessage
      extends ErrorCode(
        "MEDIATOR_INVALID_MESSAGE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    override def logLevel: Level = Level.WARN

    case class Reject(
        override val cause: String,
        override val _v0CodeP: v0.MediatorRejection.Code = v0.MediatorRejection.Code.Timeout,
    )(val representativeProtocolVersion: RepresentativeProtocolVersion[Verdict])
        extends BaseCantonError.Impl(cause)
        with MediatorReject

    object Reject {
      def create(
          cause: String,
          v0CodeP: v0.MediatorRejection.Code = v0.MediatorRejection.Code.Timeout,
          protocolVersion: ProtocolVersion,
      ): Reject =
        Reject(cause, v0CodeP)(Verdict.protocolVersionRepresentativeFor(protocolVersion))
    }
  }

  /** Used as a fallback to represent mediator errors coming from a mediator running a higher version.
    * @param id the id of the error code at the mediator. Only pass documented error code ids here, to avoid confusion.
    */
  case class GenericError(
      override val cause: String,
      id: String,
      category: ErrorCategory,
      override val _v0CodeP: v0.MediatorRejection.Code = v0.MediatorRejection.Code.Timeout,
  )(val representativeProtocolVersion: RepresentativeProtocolVersion[Verdict])
      extends BaseCantonError
      with MediatorReject {
    override def code: ErrorCode =
      new ErrorCode(id, category) {}
  }

  @Explanation(
    """The mediator has received a malformed message. This may occur due to a bug at the sender of the message.
      |The message will be discarded. As a consequence, the underlying request may be rejected.
      |No corruption of the ledger is to be expected."""
  )
  @Resolution("Contact support.")
  object MalformedMessage extends AlarmErrorCode("MEDIATOR_RECEIVED_MALFORMED_MESSAGE") {

    case class Reject(
        override val cause: String,
        override val _v0CodeP: v0.MediatorRejection.Code = v0.MediatorRejection.Code.Timeout,
    )(val representativeProtocolVersion: RepresentativeProtocolVersion[Verdict])
        extends Alarm(cause)
        with MediatorReject

    object Reject {
      def apply(
          cause: String,
          v0CodeP: v0.MediatorRejection.Code,
          protocolVersion: ProtocolVersion,
      ): Reject =
        Reject(cause, v0CodeP)(Verdict.protocolVersionRepresentativeFor(protocolVersion))

      def apply(
          cause: String,
          protocolVersion: ProtocolVersion,
      ): Reject = Reject(cause)(Verdict.protocolVersionRepresentativeFor(protocolVersion))
    }
  }

  @Explanation(
    "Request processing failed due to a violation of internal invariants. It indicates a bug at the mediator."
  )
  @Resolution("Contact support.")
  object InternalError
      extends ErrorCode(
        "MEDIATOR_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    /** @param throwableO optional throwable that will not be serialized and is therefore not delivered to clients.
      */
    case class Reject(
        override val cause: String,
        override val throwableO: Option[Throwable] = None,
    ) extends BaseCantonError.Impl(cause) {}
  }
}
