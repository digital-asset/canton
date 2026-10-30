// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.validation.ValidationParam
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}

private[admin] object AdminApiResponseValidation {

  /** The validation level to read the responses of a node's own admin API with. They come from the
    * operator's node, which validated the content when it entered the node, so the client reads
    * them as trusted and applies no limits.
    */
  val Trusted: ProtocolVersionValidation = ProtocolVersionValidation.NoValidation

  /** The length bound to pass along with [[Trusted]], which never reads it. MUST NOT be used with
    * any other validation level, as it bounds nothing.
    */
  val NoLengthLimit: ValidationParam[PositiveInt] =
    ValidationParam(ProtocolVersion.minimum -> PositiveInt.MaxValue)
}
