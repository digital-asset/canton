// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersionValidation

/** Entry point for validating a field read at the Protobuf deserialization boundary. */
object ProtoValidation {

  /** Validate a field via its [[ProtoValidator]], protocol-version-gated (enforced starting from
    * `ProtocolVersion.v36`).
    *
    * `ProtocolVersionValidation.NoValidation` skips the check entirely and must be used only for
    * content from a trusted source (e.g. read back from the database, or a trusted admin request),
    * never for input received from an untrusted source.
    */
  def validate[A](value: A, field: Option[String], pvv: ProtocolVersionValidation)(implicit
      validator: ProtoValidator[A]
  ): ParsingResult[A] =
    pvv match {
      case ProtocolVersionValidation.PV(pv) => validator.validate(value, pv, field)
      case ProtocolVersionValidation.NoValidation => Right(value)
    }
}
