// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain

import scala.concurrent.duration.{FiniteDuration, *}

package object sequencing {

  /** If a participant hasn't been active within this duration they are considered stale.
    * This allows deleting them from the Sequencer without any overrides.
    */
  val DefaultStalenessDuration: FiniteDuration = 15.days

}
