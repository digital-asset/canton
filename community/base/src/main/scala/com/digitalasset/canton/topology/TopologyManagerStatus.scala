// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

trait TopologyManagerStatus {
  def queueSize: Int
}

object TopologyManagerStatus {

  /** @param statusProviders
    *   a collection of [[TopologyManagerStatus]] instances that are combined into an aggregated
    *   status.
    */
  def combined(
      statusProviders: TopologyManagerStatus*
  ): TopologyManagerStatus =
    new TopologyManagerStatus {
      override def queueSize: Int = statusProviders.map(_.queueSize).sum
    }
}
