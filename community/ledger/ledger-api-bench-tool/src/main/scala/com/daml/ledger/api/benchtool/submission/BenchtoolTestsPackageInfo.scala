// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.infrastructure.TestDars

final case class BenchtoolTestsPackageInfo(
    packageId: String
)

object BenchtoolTestsPackageInfo {
  val BenchtoolTestsPackageName = "benchtool-tests"

  // The packageId obtained from the compiled Scala bindings
  val StaticDefault: BenchtoolTestsPackageInfo =
    BenchtoolTestsPackageInfo(packageId = TestDars.benchtoolDarPackageId)

}
