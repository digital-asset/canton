// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Synchronizer

final case class PendingOperationMetadata(
    operationName: String,
    operationKey: String,
    synchronizer: Synchronizer,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[PendingOperationMetadata] =
    PendingOperationMetadata
}

object PendingOperationMetadata extends PrettyPrintingCompanion[PendingOperationMetadata] {
  override protected val pretty: Pretty[PendingOperationMetadata] = prettyOfClass(
    param("operationName", _.operationName.unquoted),
    param("operationKey", _.operationKey.unquoted),
    param("synchronizer", _.synchronizer),
  )

  def fromProtoV30(proto: v30.PendingOperationMetadata): ParsingResult[PendingOperationMetadata] = {
    val res =
      ProtoConverter.parseRequired(Synchronizer.fromProtoV30, "synchronizer", proto.synchronizer)
    res.map(synchronizer =>
      PendingOperationMetadata(
        operationName = proto.operationName,
        operationKey = proto.operationKey,
        synchronizer = synchronizer,
      )
    )
  }
}
