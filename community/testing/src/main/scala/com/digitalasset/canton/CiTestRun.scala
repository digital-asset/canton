// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

object CiTestRun {
  val topologySnapshotConsistencyChecks: Option[Boolean] =
    Option.when(sys.env.contains("CI"))(
      sys.env.get("CANTON_NIGHTLY_TEST_RUN").contains("true")
    )
}
