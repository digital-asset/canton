// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.store.db.{H2Test, PostgresTest}
import com.digitalasset.canton.topology.store.DownloadTopologyStateForInitializationServiceTest

final class DownloadTopologyStateForInitializationServiceTestPostgres
    extends DownloadTopologyStateForInitializationServiceTest
    with DbTopologyStoreHelper
    with PostgresTest

final class DownloadTopologyStateForInitializationServiceTestH2
    extends DownloadTopologyStateForInitializationServiceTest
    with DbTopologyStoreHelper
    with H2Test
