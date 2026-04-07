// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.SequencerAggregatorPekko.HasSequencerSubscriptionFactoryPekko
import com.digitalasset.canton.sequencing.client.pool.SequencerConnectionWithPekkoSubscribe
import com.digitalasset.canton.sequencing.protocol.SubscriptionRequest
import com.digitalasset.canton.topology.{Member, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

trait SequencerSubscriptionFactoryPekko[E] extends HasSequencerSubscriptionFactoryPekko[E] {

  /** The ID of the sequencer this factory creates subscriptions to */
  def sequencerId: SequencerId

  def create(
      startingTimestamp: Option[CantonTimestamp]
  )(implicit traceContext: TraceContext): SequencerSubscriptionPekko[E]

  def retryPolicy: SubscriptionErrorRetryPolicyPekko[E]

  override def subscriptionFactory: this.type = this
}

object SequencerSubscriptionFactoryPekko {
  def fromConnection[E](
      sequencerID: SequencerId,
      connection: SequencerConnectionWithPekkoSubscribe.Aux[E],
      member: Member,
      protocolVersion: ProtocolVersion,
  ): SequencerSubscriptionFactoryPekko[E] =
    new SequencerSubscriptionFactoryPekko[E] {
      override def sequencerId: SequencerId = sequencerID

      override def create(startingTimestamp: Option[CantonTimestamp])(implicit
          traceContext: TraceContext
      ): SequencerSubscriptionPekko[E] = {
        val request = SubscriptionRequest(member, startingTimestamp, protocolVersion)
        connection.subscribe(request)
      }

      override val retryPolicy: SubscriptionErrorRetryPolicyPekko[E] =
        connection.subscriptionRetryPolicyPekko
    }
}
