// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.BaseTest.eventually
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.matchers.should.Matchers.*

object AutomaticAssignment {

  /** Sets the assignment exclusivity timeout to zero if the testedProtocolVersion is at most
    * ProtocolVersion.v36, which disables automatic assignments. From ProtocolVersion.v37, the
    * automatic assignments are disabled by
    * [[com.digitalasset.canton.integration.ConfigTransforms.disableAutomaticAssignment]].
    *
    * @param owners
    *   all the owners of the synchronizer, so that the change is fully authorized
    */
  def disable(
      owners: Iterable[InstanceReference],
      synchronizerId: SynchronizerId,
  ): Unit =
    if (BaseTest.testedProtocolVersion <= ProtocolVersion.v36) {
      owners.foreach(owner =>
        owner.topology.synchronizer_parameters.propose_update(
          synchronizerId,
          _.update(assignmentExclusivityTimeout = NonNegativeFiniteDuration.Zero),
        )
      )

      owners.foreach(owner =>
        eventually() {
          owner.topology.synchronizer_parameters
            .get_dynamic_synchronizer_parameters(synchronizerId)
            .assignmentExclusivityTimeout shouldBe NonNegativeFiniteDuration.Zero
        }
      )
    } else ()
}
