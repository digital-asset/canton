// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.store.db.{DbTest, H2Test, MigrationMode, PostgresTest}
import com.digitalasset.canton.topology.store.TopologyStoreXTest

trait DbTopologyStoreXTest extends TopologyStoreXTest with DbTopologyStoreXHelper {
  this: DbTest =>

  "DbTopologyStore" should {
    behave like topologyStore(() => createTopologyStore())
  }
}

class TopologyStoreXTestPostgres extends DbTopologyStoreXTest with PostgresTest {

  // TODO(#12373) remove this from unstable/dev when releasing BFT
  override val migrationMode: MigrationMode = MigrationMode.DevVersion
}

class TopologyStoreXTestH2 extends DbTopologyStoreXTest with H2Test {

  // TODO(#12373) remove this from unstable/dev when releasing BFT
  override val migrationMode: MigrationMode = MigrationMode.DevVersion
}
