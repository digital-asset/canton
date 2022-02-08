// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.services.time

import java.time.Instant

sealed trait TimeProviderType extends Product with Serializable
object TimeProviderType {
  case class Static(time: Instant) extends TimeProviderType
  case object WallClock extends TimeProviderType
}
