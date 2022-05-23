// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import cats.instances.either._
import cats.syntax.either._
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.protocol.messages.{DefaultOpenEnvelope, ProtocolMessage}
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpener.EventDeserializationError
import com.digitalasset.canton.sequencing.{ApplicationHandler, EnvelopeBox}
import com.digitalasset.canton.sequencing.protocol.ClosedEnvelope
import com.digitalasset.canton.version.ProtocolVersion

/** Opener for envelopes inside an arbitrary [[EnvelopeBox]] */
class EnvelopeOpener[Box[+_]](protocolVersion: ProtocolVersion, hashOps: HashOps)(implicit
    Box: EnvelopeBox[Box]
) {
  def open(closed: Box[ClosedEnvelope]): Box[DefaultOpenEnvelope] = {
    val openedEventE = Box.traverse(closed) { closedEnvelope =>
      closedEnvelope.openEnvelope(
        ProtocolMessage.fromEnvelopeContentByteString(protocolVersion, hashOps)
      )
    }

    openedEventE.valueOr { error =>
      // TODO(M40) We shouldn't open the envelopes in the sequencer client because the mediator may want to react to
      //  a garbage informee message by sending a rejection to all recipients of the root hash messages
      throw EventDeserializationError(error)
    }
  }
}

object EnvelopeOpener {

  /** Opens the envelopes inside the [[EnvelopeBox]] before handing them to the given application handler. */
  def apply[Box[+_]](protocolVersion: ProtocolVersion, hashOps: HashOps)(
      handler: ApplicationHandler[Box, DefaultOpenEnvelope]
  )(implicit Box: EnvelopeBox[Box]): ApplicationHandler[Box, ClosedEnvelope] = handler.replace {
    val opener = new EnvelopeOpener[Box](protocolVersion, hashOps)

    closedEvent => handler(opener.open(closedEvent))
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  case class EventDeserializationError(error: ProtoDeserializationError, cause: Throwable = null)
      extends RuntimeException(s"Failed to deserialize event: $error", cause)
}
