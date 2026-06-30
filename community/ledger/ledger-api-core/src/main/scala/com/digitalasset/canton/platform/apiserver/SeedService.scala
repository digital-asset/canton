// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

object SeedService {

  /** This type is deprecated and no longer used (see `ledgerApiServerParametersConfigReader` in
    * CantonConfig.scala). It is kept in the code solely for backwards compatibility.
    */
  // TODO(i33818): Remove
  sealed abstract class Seeding(val name: String) extends Product with Serializable {
    override def toString: String = name
  }

  object Seeding {

    case object Strong extends Seeding("strong")

    case object Weak extends Seeding("testing-weak")

    case object Static extends Seeding("testing-static")

  }
}
