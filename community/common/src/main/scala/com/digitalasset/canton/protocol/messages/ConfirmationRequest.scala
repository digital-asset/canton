// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.topology.MediatorId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope, Recipients}

/** Represents the confirmation request as sent from a submitting node to the sequencer.
  */
case class ConfirmationRequest(
    informeeMessage: InformeeMessage,
    viewEnvelopes: Seq[OpenEnvelope[TransactionViewMessage]],
) extends PrettyPrinting {

  def mediator: MediatorId = informeeMessage.mediatorId

  def asBatch: Batch[DefaultOpenEnvelope] = {
    val mediatorEnvelope: DefaultOpenEnvelope =
      OpenEnvelope(informeeMessage, Recipients.cc(mediator))

    val rootHashMessage = RootHashMessage(
      rootHash = informeeMessage.fullInformeeTree.transactionId.toRootHash,
      domainId = informeeMessage.domainId,
      viewType = ViewType.TransactionViewType,
      payload = EmptyRootHashMessagePayload,
      protocolVersion = informeeMessage.representativeProtocolVersion.unwrap,
    )
    val participants = viewEnvelopes.flatMap { envelope =>
      envelope.protocolMessage.participants
        .getOrElse {
          // NOTE: We do not serialize the original informee participants as part of a serialized encrypted view message.
          // Due to sharing of a key a fingerprint may map to multiple participants.
          // However we only use the informee participants before serialization, so this information is not required afterwards.
          throw new IllegalStateException(
            s"Obtaining informee participants on deserialized encrypted view message"
          )
        }
    }.distinct

    val rootHashMessages = NonEmpty
      .from(participants)
      .map { participantsNE =>
        OpenEnvelope(
          rootHashMessage,
          Recipients.groups(participantsNE.map(NonEmpty.mk(Set, _, mediator))),
        )
      }
      .toList

    val envelopes: List[DefaultOpenEnvelope] =
      rootHashMessages ++ (viewEnvelopes: Seq[DefaultOpenEnvelope])
    Batch(mediatorEnvelope +: envelopes)
  }

  override def pretty: Pretty[ConfirmationRequest] = prettyOfClass(
    param("informee message", _.informeeMessage),
    param("view envelopes", _.viewEnvelopes),
  )
}
