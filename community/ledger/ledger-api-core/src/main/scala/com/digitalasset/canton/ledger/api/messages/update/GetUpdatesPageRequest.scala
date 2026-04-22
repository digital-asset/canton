// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.update

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.UpdateFormat
import com.google.protobuf.ByteString

final case class GetUpdatesPageRequest(
    startExclusive: Option[Option[Offset]], // Outer None == dynamic bound, inner None ==
    endInclusive: Option[Offset],
    continueStreamFromIncl: Option[Offset],
    maxPageSize: Int,
    updateFormat: UpdateFormat,
    descendingOrder: Boolean,
    requestChecksum: ByteString,
    participantChecksum: ByteString,
)
