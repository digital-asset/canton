// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.config.RequireTypes.PositiveLong
import com.digitalasset.canton.topology.Member

import scala.concurrent.Future

final case class MemberTrafficControlState(totalExtraTrafficLimit: PositiveLong)

/** The subset of the topology client providing traffic state information */
trait DomainTrafficControlStateClient {
  this: BaseTopologySnapshotClient =>

  def trafficControlStatus(): Future[Map[Member, MemberTrafficControlState]]
}
