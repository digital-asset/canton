// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.data.ExternalCallPayloadDescription.hexPayloadSize
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.daml.lf.transaction.ExternalCallResult

/** Deterministic external-call identity. Config and input are engine-emitted canonical hex strings.
  * The pretty-printed form deliberately shows only payload sizes, never the payloads.
  */
final case class ExternalCallKey(
    extensionId: String,
    functionId: String,
    config: String,
    input: String,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[ExternalCallKey] = ExternalCallKey
}

object ExternalCallKey extends PrettyPrintingCompanion[ExternalCallKey] {

  override protected val pretty: Pretty[ExternalCallKey] = prettyOfClass(
    param("extension id", _.extensionId.doubleQuoted),
    param("function id", _.functionId.doubleQuoted),
    param("config bytes", key => hexPayloadSize(key.config).doubleQuoted),
    param("input bytes", key => hexPayloadSize(key.input).doubleQuoted),
  )

  /** Orders by the semantic identity fields, lexicographically. */
  implicit val externalCallKeyOrdering: Ordering[ExternalCallKey] =
    Ordering.by(key => (key.extensionId, key.functionId, key.config, key.input))

  def fromResult(result: ExternalCallResult): ExternalCallKey =
    ExternalCallKey(
      result.extensionId,
      result.functionId,
      result.config.toHexString,
      result.input.toHexString,
    )
}
