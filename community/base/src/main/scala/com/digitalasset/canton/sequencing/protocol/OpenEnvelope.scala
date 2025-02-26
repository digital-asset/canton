// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Functor
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.{DefaultOpenEnvelope, ProtocolMessage}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.ProtocolVersion

/** An [[OpenEnvelope]] contains a not serialized protocol message
  *
  * @tparam M
  *   The type of the protocol message
  */
final case class OpenEnvelope[+M <: ProtocolMessage](
    protocolMessage: M,
    override val recipients: Recipients,
)(protocolVersion: ProtocolVersion)
    extends Envelope[M] {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def traverse[F[_], MM <: ProtocolMessage](
      f: M => F[MM]
  )(implicit F: Functor[F]): F[OpenEnvelope[MM]] =
    F.map(f(protocolMessage)) { newProtocolMessage =>
      if (newProtocolMessage eq protocolMessage) this.asInstanceOf[OpenEnvelope[MM]]
      else this.copy(protocolMessage = newProtocolMessage)
    }

  override protected def pretty: Pretty[DefaultOpenEnvelope] =
    prettyOfClass(unnamedParam(_.protocolMessage), param("recipients", _.recipients))

  override def forRecipient(
      member: Member,
      groupAddresses: Set[GroupRecipient],
  ): Option[OpenEnvelope[M]] = {
    val subtrees = recipients.forMember(member, groupAddresses)
    subtrees.map(s => this.copy(recipients = s))
  }

  /** Closes the envelope by serializing the contents */
  override def closeEnvelope: ClosedEnvelope =
    ClosedEnvelope.fromProtocolMessage(protocolMessage, recipients, protocolVersion)

  /** Copy method without the second argument so that it can be used with autogenerated lenses */
  def copy[MM <: ProtocolMessage](
      protocolMessage: MM = protocolMessage,
      recipients: Recipients = recipients,
  ): OpenEnvelope[MM] = OpenEnvelope(protocolMessage, recipients)(protocolVersion)
}
