// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.transaction.TransactionVersion._

import scala.collection.immutable.SortedMap
import scala.math.Ordered.orderingToOrdered

object DamlLfVersionToProtocolVersions {

  /** This Map links the Daml Lf-version to the minimum protocol version that supports it. */
  private val damlLfVersionToMinimumProtocolVersions
      : SortedMap[TransactionVersion, ProtocolVersion] =
    SortedMap(
      TransactionVersion.V14 -> ProtocolVersion.v2_0_0,
      TransactionVersion.V15 ->
        ProtocolVersion.unstable_development,
      TransactionVersion.VDev ->
        ProtocolVersion.unstable_development,
    )

  def getMinimumSupportedProtocolVersion(
      transactionVersion: TransactionVersion
  ): ProtocolVersion = {
    assert(
      transactionVersion >= TransactionVersion.V14,
      s"Canton only supports transaction versions more recent or equal to ${TransactionVersion.V14}",
    )
    damlLfVersionToMinimumProtocolVersions(transactionVersion)
  }

}
