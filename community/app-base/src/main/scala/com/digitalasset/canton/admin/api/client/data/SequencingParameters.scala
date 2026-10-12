// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

final case class SequencingParameters(payload: Option[ByteString])
    extends PrettyPrintingFromCompanion {

  private[canton] def toInternal(
      protocolVersion: ProtocolVersion
  ): protocol.SequencingParameters = {
    val rpv = protocol.SequencingParameters.protocolVersionRepresentativeFor(protocolVersion)
    protocol.SequencingParameters(payload)(rpv)
  }

  override def prettyCompanion: PrettyPrintingCompanion[SequencingParameters] =
    SequencingParameters
}

object SequencingParameters extends PrettyPrintingCompanion[SequencingParameters] {
  override protected val pretty: Pretty[SequencingParameters] =
    prettyOfClass(
      param("payload", _.payload)
    )
}
