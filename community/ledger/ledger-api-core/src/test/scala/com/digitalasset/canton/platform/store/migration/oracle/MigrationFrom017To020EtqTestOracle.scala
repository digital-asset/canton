// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.migration.oracle

import com.digitalasset.canton.platform.store.migration.tests.MigrationEtqTests

class MigrationFrom017To020EtqTestOracle
    extends MigrationEtqTests
    with OracleAroundEachForMigrations {
  override def srcMigration: String = "17"
  override def dstMigration: String = "20"
}
