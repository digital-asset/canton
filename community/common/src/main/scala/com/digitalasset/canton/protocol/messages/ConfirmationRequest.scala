// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.{NonEmptyList, NonEmptySet}
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
    )
    val participants = viewEnvelopes.flatMap(_.protocolMessage.randomSeed.keySet).distinct

    val rootHashMessages = NonEmptyList
      .fromList(participants.toList)
      .map { participantsNel =>
        OpenEnvelope(
          rootHashMessage,
          Recipients.groups(participantsNel.map(NonEmptySet.of(_, mediator))),
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
