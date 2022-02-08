// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.util.HasProtoV0

/** Encapsulated resource limits for a participant.
  *
  * @param maxDirtyRequests the maximum number of requests that are currently being validated.
  *                         This also covers requests submitted by other participants.
  * @param maxRate the maximum rate at which commands may be submitted through the ledger api.
  */
case class ResourceLimits(maxDirtyRequests: Option[NonNegativeInt], maxRate: Option[NonNegativeInt])
    extends HasProtoV0[v0.ResourceLimits] {

  override def toProtoV0: v0.ResourceLimits =
    v0.ResourceLimits(
      maxDirtyRequests = maxDirtyRequests.fold(-1)(_.unwrap),
      maxRate = maxRate.fold(-1)(_.unwrap),
    )
}

object ResourceLimits {
  def fromProtoV0(resourceLimitsP: v0.ResourceLimits): ResourceLimits = {
    val v0.ResourceLimits(maxDirtyRequestsP, maxRateP) = resourceLimitsP

    val maxDirtyRequests =
      if (maxDirtyRequestsP >= 0) Some(NonNegativeInt.tryCreate(maxDirtyRequestsP)) else None
    val maxRate = if (maxRateP >= 0) Some(NonNegativeInt.tryCreate(maxRateP)) else None

    ResourceLimits(maxDirtyRequests, maxRate)
  }

  def noLimit: ResourceLimits = ResourceLimits(None, None)

  def community: ResourceLimits = ResourceLimits(Some(NonNegativeInt.tryCreate(100)), None)
}
