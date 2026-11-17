// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.admin.grpc

import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.synchronizer.sequencer.SequencerSnapshot
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

final case class InitializeSequencerRequest(
    synchronizerId: SynchronizerId,
    topologySnapshot: Source[GenericStoredTopologyTransaction, NotUsed],
    synchronizerParameters: StaticSynchronizerParameters,
    sequencerSnapshot: Option[SequencerSnapshot] = None,
    ignoreLsuPsidCheck: Boolean = false,
)
