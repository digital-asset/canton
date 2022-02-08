// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.crypto.{CryptoPureApi, Hash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.store.SequencedEventStore.{
  IgnoredSequencedEvent,
  OrdinarySequencedEvent,
  PossiblyIgnoredSequencedEvent,
}

/** Metadata of a [[protocol.SequencedEvent]]. Used by the sequencer client to persist details of the last event it has seen
  * but where we don't need details of its content.
  * @param counter The counter value assigned to the event
  * @param timestamp The timestamp assigned to the event
  * @param hash The hash of the event contents that is also used for signing the event
  *             None for ignored sequenced events.
  */
case class SequencedEventMetadata(
    counter: SequencerCounter,
    timestamp: CantonTimestamp,
    hash: Option[Hash],
)

object SequencedEventMetadata {
  def apply(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      hash: Hash,
  ): SequencedEventMetadata =
    SequencedEventMetadata(counter, timestamp, Some(hash))

  def fromPossiblyIgnoredSequencedEvent(
      cryptoApi: CryptoPureApi,
      event: PossiblyIgnoredSequencedEvent[_],
  ): SequencedEventMetadata =
    event match {
      case IgnoredSequencedEvent(timestamp, counter, _) =>
        SequencedEventMetadata(counter, timestamp, None)
      case OrdinarySequencedEvent(signedEvent) =>
        SequencedEventMetadata(
          counter = event.counter,
          timestamp = event.timestamp,
          hash = Some(SignedContent.hashContent(cryptoApi, signedEvent.content)),
        )
    }
}
