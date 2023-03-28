// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.migration.postgres

import com.digitalasset.canton.platform.store.migration.tests.MigrationEtqTests

class MigrationFrom127To130EtqTestPostgres
    extends MigrationEtqTests
    with PostgresAroundEachForMigrations {
  override def srcMigration: String = "127"
  override def dstMigration: String = "130"
}
