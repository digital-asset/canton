// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import com.digitalasset.canton.sequencing.protocol.MessageId

import scala.concurrent.Future

package object client {

  /** Signature for handlers that are notified that the sequencer clock has progressed beyond
    * the max sequencing time of a send previously done by the client and receipt has not been
    * witnessed.
    * It's currently unclear what we'll do when handling these timeouts so the error type is left
    * intentionally undefined.
    */
  type SendTimeoutHandler =
    MessageId => EitherT[Future, SendTrackerUpdateError.TimeoutHandlerError, Unit]

  /** Signature for callbacks provided to the send operation to take advantage of the SendTracker to provide
    * tracking of the eventual send result. Callback is ephemeral and will be lost if the SequencerClient is recreated
    * or the process exits.
    * @see [[SequencerClient.sendAsync]]
    */
  type SendCallback = SendResult => Unit
}
