// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.state

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.index.IndexerHostedPartyFilter

final case class GetHostedPartiesPageRequest(
    filter: IndexerHostedPartyFilter,
    requestedHostedAt: Option[Option[Offset]],
    continuation: Option[HostedPartiesContinuation],
    pageSize: Int,
)
