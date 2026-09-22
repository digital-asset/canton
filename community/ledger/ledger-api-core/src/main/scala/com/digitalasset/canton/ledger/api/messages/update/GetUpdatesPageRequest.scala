// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.update

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.UpdateFormat
import com.google.protobuf.ByteString

final case class GetUpdatesPageRequest(
    // Outer None == dynamic bound, inner None == before ledger offset 1 (begin).
    startExclusive: Option[
      Option[Offset]
    ],
    endInclusive: Option[Offset],
    continueStreamFromIncl: Option[Offset],
    maxPageSize: Int,
    updateFormat: UpdateFormat,
    descendingOrder: Boolean,
    requestChecksum: ByteString,
    participantChecksum: ByteString,
)
