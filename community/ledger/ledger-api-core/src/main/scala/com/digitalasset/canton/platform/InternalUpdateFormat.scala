// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.canton.ledger.api.{TopologyFormat, TransactionShape}
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.topology.SynchronizerId

final case class InternalUpdateFormat(
    includeTransactions: Option[InternalTransactionFormat],
    includeReassignments: Option[InternalEventFormat],
    includeTopologyEvents: Option[TopologyFormat],
    includeAcsCommitments: Option[SynchronizerId],
)

final case class InternalTransactionFormat(
    internalEventFormat: InternalEventFormat,
    transactionShape: TransactionShape,
)

final case class InternalEventFormat(
    templatePartiesFilter: TemplatePartiesFilter,
    eventProjectionProperties: EventProjectionProperties,
)
