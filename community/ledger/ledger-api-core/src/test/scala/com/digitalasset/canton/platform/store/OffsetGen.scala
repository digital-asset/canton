// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.digitalasset.canton.data.Offset

private[platform] object OffsetGen {

  /** Index-based offset used by buffer/cache/index specs. The 1e9 base keeps these synthetic
    * offsets clear of the small offsets used elsewhere.
    */
  def offset(idx: Long): Offset = Offset.tryFromLong(1000000000L + idx)
}
