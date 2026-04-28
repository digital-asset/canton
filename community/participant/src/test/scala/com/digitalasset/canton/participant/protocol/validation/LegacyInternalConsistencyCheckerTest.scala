// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.topology.ParticipantId

class LegacyInternalConsistencyCheckerTest extends InternalConsistencyCheckerTest {

  "Internal consistency checker" when {

    val participantId: ParticipantId = ParticipantId("test")
    val sut = new NextGenInternalConsistencyChecker(participantId, loggerFactory)

    "rollback scope order" should checkRollbackScopeOrder()

    "standard happy cases" should checkStandardHappyCases(sut)

  }
}
