// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package query

import com.daml.lf.data.Ref
import com.daml.lf.typesig
import com.daml.lf.value.Value as V

object ValuePredicate {
  type TypeLookup = Ref.Identifier => Option[typesig.DefDataType.FWT]
  type LfV = V
}
