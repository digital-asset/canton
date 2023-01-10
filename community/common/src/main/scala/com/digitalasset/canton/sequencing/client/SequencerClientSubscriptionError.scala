// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

sealed trait SequencerClientSubscriptionError extends Product with Serializable {
  def mbException: Option[Throwable] = None
}

object SequencerClientSubscriptionError {
  case class EventValidationError(error: SequencedEventValidationError)
      extends SequencerClientSubscriptionError
  trait SendTrackerUpdateError extends SequencerClientSubscriptionError

  sealed trait ApplicationHandlerFailure
      extends SequencerClientSubscriptionError
      with PrettyPrinting

  /** The application handler returned that it is being shutdown. */
  case object ApplicationHandlerShutdown extends ApplicationHandlerFailure {
    override def pretty: Pretty[ApplicationHandlerShutdown.type] =
      prettyOfObject[ApplicationHandlerShutdown.type]
  }

  /** The application handler returned that it is being passive. */
  case class ApplicationHandlerPassive(reason: String) extends ApplicationHandlerFailure {
    override def pretty: Pretty[ApplicationHandlerPassive] =
      prettyOfClass(param("reason", _.reason.unquoted))
  }

  /** The application handler threw an exception while processing the event (synchronously or asynchronously) */
  case class ApplicationHandlerException(
      exception: Throwable,
      firstSequencerCounter: SequencerCounter,
      lastSequencerCounter: SequencerCounter,
  ) extends ApplicationHandlerFailure {
    override def mbException: Option[Throwable] = Some(exception)

    override def pretty: Pretty[ApplicationHandlerException] = prettyOfClass(
      param("first sequencer counter", _.firstSequencerCounter),
      param("last sequencer counter", _.lastSequencerCounter),
      unnamedParam(_.exception),
    )
  }

}
