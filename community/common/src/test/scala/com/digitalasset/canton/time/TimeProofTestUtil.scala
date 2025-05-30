// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.syntax.option.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, SignedContent, TimeProof}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.{DefaultTestIdentities, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, SequencerCounter}

object TimeProofTestUtil {
  def mkTimeProof(
      timestamp: CantonTimestamp,
      previousEventTimestamp: Option[CantonTimestamp] = None,
      counter: Long = 0L,
      targetSynchronizer: Target[PhysicalSynchronizerId] = Target(
        DefaultTestIdentities.physicalSynchronizerId
      ),
      protocolVersion: ProtocolVersion = BaseTest.testedProtocolVersion,
  ): TimeProof = {
    val deliver = Deliver.create(
      previousEventTimestamp,
      timestamp,
      targetSynchronizer.unwrap,
      TimeProof.mkTimeProofRequestMessageId.some,
      Batch.empty(protocolVersion),
      None,
      protocolVersion,
      Option.empty[TrafficReceipt],
    )
    val signedContent =
      SignedContent(deliver, SymbolicCrypto.emptySignature, None, protocolVersion)
    val event = OrdinarySequencedEvent(SequencerCounter(counter), signedContent)(TraceContext.empty)
    TimeProof
      .fromEvent(event)
      .fold(err => sys.error(s"Failed to create time proof: $err"), identity)
  }
}
