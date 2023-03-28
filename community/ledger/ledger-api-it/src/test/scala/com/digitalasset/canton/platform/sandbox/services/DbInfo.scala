// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.sandbox.services

import com.digitalasset.canton.platform.store.DbType

final case class DbInfo(
    jdbcUrl: String,
    dbType: DbType,
)
