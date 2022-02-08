// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.topology.{DefaultTestIdentities, TopologyManagerTest}

import scala.concurrent.Future

class ParticipantTopologyManagerTest extends TopologyManagerTest {

  "participant topology state manager" should {
    behave like topologyManager((clock, store, crypto, factory) =>
      Future.successful {
        val mgr = new ParticipantTopologyManager(
          clock,
          store,
          crypto,
          DefaultProcessingTimeouts.testing,
          factory,
        )
        mgr.setParticipantId(DefaultTestIdentities.participant1)
        mgr
      }
    )

  }

}
