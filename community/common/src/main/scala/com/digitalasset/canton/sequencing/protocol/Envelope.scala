// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.HasProtoV0
import com.google.protobuf.ByteString

/** An [[Envelope]] wraps an envelope content such as a [[com.digitalasset.canton.protocol.messages.ProtocolMessage]]
  * together with the recipients.
  *
  * @tparam M The type of the envelope content
  */
trait Envelope[+M] extends HasProtoV0[v0.Envelope] with PrettyPrinting {

  def recipients: Recipients

  def forRecipient(member: Member): Option[Envelope[M]]

  /** Returns the contents of the envelope */
  protected def content: M

  /** Returns the serialized contents of the envelope */
  protected def contentAsByteString: ByteString

  override def toProtoV0: v0.Envelope = v0.Envelope(
    content = contentAsByteString,
    recipients = Some(recipients.toProtoV0),
  )
}
