// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence

trait SignedProtocolMessageContent
    extends ProtocolVersionedMemoizedEvidence
    with HasDomainId
    with PrettyPrinting
    with Product
    with Serializable {

  /** Converts this object into a [[com.google.protobuf.ByteString]] using [[com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence.getCryptographicEvidence]]
    * and wraps the result in the appropriate [[com.digitalasset.canton.protocol.v0.SignedProtocolMessage.SomeSignedProtocolMessage]] constructor.
    */
  protected[messages] def toProtoSomeSignedProtocolMessage
      : v0.SignedProtocolMessage.SomeSignedProtocolMessage

  /** Hash purpose that uniquely identifies the type of message content to be signed. */
  def hashPurpose: HashPurpose

  /** The timestamp of the [[com.digitalasset.canton.crypto.SyncCryptoApi]] used for signing this message.
    */
  def signingTimestamp: CantonTimestamp

  override def pretty: Pretty[this.type] = prettyOfObject[SignedProtocolMessageContent]
}

object SignedProtocolMessageContent {
  trait SignedMessageContentCast[A] {
    def toKind(content: SignedProtocolMessageContent): Option[A]
  }
}
