// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.data.Offset

/** Ordering helpers for streams.
  */
object OrderingUtils {

  /** Orders [[scala.Long]] keys (e.g. event-sequential ids), honoring the stream direction. */
  def orderingBasedOnDescending(descendingOrder: Boolean): Ordering[Long] =
    if (descendingOrder)
      Ordering.Long.reverse
    else
      Ordering.Long

  def offsetOrdering[T](descendingOrder: Boolean): Ordering[(Offset, T)] =
    Ordering.by[(Offset, T), Long](_._1.unwrap)(orderingBasedOnDescending(descendingOrder))
}
