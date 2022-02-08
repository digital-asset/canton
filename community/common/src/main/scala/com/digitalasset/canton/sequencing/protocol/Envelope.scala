// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.util.HasProtoV0WithVersion
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

/** An [[Envelope]] wraps an envelope content such as a [[com.digitalasset.canton.protocol.messages.ProtocolMessage]]
  * together with the recipients.
  *
  * @tparam M The type of the envelope content
  */
trait Envelope[+M] extends HasProtoV0WithVersion[v0.Envelope] with PrettyPrinting {

  def recipients: Recipients

  def forRecipient(member: Member): Option[Envelope[M]]

  /** Returns the contents of the envelope */
  protected def content: M

  /** Returns the serialized contents of the envelope */
  protected def contentAsByteString(version: ProtocolVersion): ByteString

  override def toProtoV0(version: ProtocolVersion): v0.Envelope = v0.Envelope(
    content = contentAsByteString(version),
    recipients = Some(recipients.toProtoV0),
  )

  /** Closes the envelope by serializing the contents */
  def closeEnvelope: ClosedEnvelope =
    ClosedEnvelope(contentAsByteString(ProtocolVersion.default), recipients)
}
