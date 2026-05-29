// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.protocol.LfSerializationVersion

import scala.collection.immutable.SortedMap
import scala.math.Ordered.orderingToOrdered

object LfSerializationVersionToProtocolVersions {

  /** This Map links the Daml Lf-version to the minimum protocol version that supports it. Sorted
    * high-to-low to allow the maximum supported version for PV to be found without iterating over
    * the whole map.
    */
  private val lfSerializationVersionToMinimumProtocolVersions
      : SortedMap[LfSerializationVersion, ProtocolVersion] =
    SortedMap(
      LfSerializationVersion.V1 -> ProtocolVersion.v34,
      LfSerializationVersion.V2 -> ProtocolVersion.v35,
      LfSerializationVersion.VDev -> ProtocolVersion.dev,
    )(LfSerializationVersion.`SerializationVersion Ordering`.reverse)

  def getMinimumSupportedProtocolVersion(
      serializationVersion: LfSerializationVersion
  ): ProtocolVersion = {
    assert(
      serializationVersion >= LfSerializationVersion.V1,
      s"Canton only supports LF serialization versions more recent or equal to ${LfSerializationVersion.V1}",
    )
    lfSerializationVersionToMinimumProtocolVersions(serializationVersion)
  }

  def maxSerializationVersionForProtocolVersion(
      protocolVersion: ProtocolVersion
  ): LfSerializationVersion =
    lfSerializationVersionToMinimumProtocolVersions
      .collectFirst {
        case (sv, pv) if protocolVersion >= pv => sv
      }
      .getOrElse(
        throw new IllegalStateException(
          s"Protocol version $protocolVersion does not supported any LF serialization version"
        )
      )

  require(
    lfSerializationVersionToMinimumProtocolVersions.values.exists(_ == ProtocolVersion.minimum),
    "The minimum protocol version must support at least one LF serialization version",
  )

}
