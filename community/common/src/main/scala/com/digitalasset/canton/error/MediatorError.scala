// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error._
import com.digitalasset.canton.error.CantonErrorGroups.MediatorErrorGroup
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.v0
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
    ) extends BaseCantonError.Impl(cause)
        with MediatorReject {

      override def _v0CodeP: v0.MediatorRejection.Code = v0.MediatorRejection.Code.Timeout
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
    ) extends BaseCantonError.Impl(cause)
        with MediatorReject
  }

  /** Used as a fallback to represent mediator errors coming from a mediator running a higher version.
    * @param id the id of the error code at the mediator. Only pass documented error code ids here, to avoid confusion.
    */
  case class GenericError(
      override val cause: String,
      id: String,
      override val _v0CodeP: v0.MediatorRejection.Code = v0.MediatorRejection.Code.Timeout,
  ) extends BaseCantonError
      with MediatorReject {
    override def code: ErrorCode = Timeout.code
    // TODO(i8744): replace by this line once the docs generator can handle it
    // new ErrorCode(id, ErrorCategory.InvalidGivenCurrentSystemStateOther) {}
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
    ) extends Alarm(cause)
        with MediatorReject
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
