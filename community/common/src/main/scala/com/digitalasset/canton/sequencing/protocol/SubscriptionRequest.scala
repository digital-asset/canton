// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member

/** A request to receive events from a given counter from a sequencer.
  *
  * @param member the member subscribing to the sequencer
  * @param counter the counter of the first event to receive.
  */
case class SubscriptionRequest(member: Member, counter: SequencerCounter) {

  // despite being serialized in the HttpSequencerClient, we don't introduce a `VersionedSubscriptionRequest` because
  // we assume that the subscription endpoint will also be bumped if a V1 SubscriptionRequest is ever introduced
  def toProtoV0: v0.SubscriptionRequest =
    v0.SubscriptionRequest(member.toProtoPrimitive, counter.v)
}

object SubscriptionRequest {
  def fromProtoV0(
      subscriptionRequestP: v0.SubscriptionRequest
  ): ParsingResult[SubscriptionRequest] = {
    val v0.SubscriptionRequest(memberP, counter) = subscriptionRequestP
    for {
      member <- Member.fromProtoPrimitive(memberP, "member")
    } yield SubscriptionRequest(member, SequencerCounter(counter))
  }

}
