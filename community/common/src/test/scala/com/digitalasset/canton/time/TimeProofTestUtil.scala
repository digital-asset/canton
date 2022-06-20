// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.syntax.option._
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, SignedContent}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.{DefaultTestIdentities, DomainId}
import com.digitalasset.canton.tracing.TraceContext

object TimeProofTestUtil {
  def mkTimeProof(
      timestamp: CantonTimestamp,
      counter: Long = 0L,
      domainId: DomainId = DefaultTestIdentities.domainId,
  ): TimeProof = {
    val deliver = Deliver.create(
      counter,
      timestamp,
      domainId,
      TimeProof.mkTimeProofRequestMessageId.some,
      Batch.empty(TestDomainParameters.defaultStatic.protocolVersion),
      TestDomainParameters.defaultStatic.protocolVersion,
    )
    val signedContent = SignedContent(deliver, SymbolicCrypto.emptySignature, None)
    val event = OrdinarySequencedEvent(signedContent)(TraceContext.empty)
    TimeProof
      .fromEvent(event)
      .fold(err => sys.error(s"Failed to create time proof: $err"), identity)
  }
}
