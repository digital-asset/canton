// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member

/** An [[Envelope]] wraps an envelope content such as a
  * [[com.digitalasset.canton.protocol.messages.ProtocolMessage]] together with the recipients.
  *
  * @tparam M
  *   The type of the envelope content
  */
trait Envelope[+M] extends PrettyPrinting {

  def recipients: Recipients

  def forRecipient(
      member: Member,
      groupAddresses: Set[GroupRecipient],
  ): Option[Envelope[M]]

  def toClosedCompressedEnvelope: ClosedCompressedEnvelope

  def toClosedUncompressedEnvelopeResult: ParsingResult[ClosedUncompressedEnvelope]

  /** Should only be used in tests and where you are a 100% sure it won't throw an exception
    */
  def toClosedUncompressedEnvelopeUnsafe: ClosedUncompressedEnvelope =
    toClosedUncompressedEnvelopeResult.valueOr(error =>
      throw new IllegalArgumentException(error.message)
    )
}
