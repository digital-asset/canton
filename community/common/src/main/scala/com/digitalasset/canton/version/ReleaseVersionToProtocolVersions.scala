// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}

object ReleaseVersionToProtocolVersions {

  import ProtocolVersion.*
  // For each (major, minor) the list of supported protocol versions
  // Don't make this variable private because it's used in `console-reference.canton`
  val majorMinorToProtocolVersions: Map[(Int, Int), NonEmpty[List[ProtocolVersion]]] = Map(
    ReleaseVersions.v2_0_0 -> List(v2),
    ReleaseVersions.v2_1_0 -> List(v2),
    ReleaseVersions.v2_2_0 -> List(v2),
    ReleaseVersions.v2_3_0 -> List(v2, v3),
    ReleaseVersions.v2_4_0 -> List(v2, v3),
    ReleaseVersions.v2_5_0 -> List(v2, v3, v4),
    ReleaseVersions.v2_6_0_snapshot -> List(v2, v3, v4),
  ).map { case (release, pvs) => (release.majorMinor, NonEmptyUtil.fromUnsafe(pvs)) }

  def get(releaseVersion: ReleaseVersion): Option[NonEmpty[List[ProtocolVersion]]] =
    majorMinorToProtocolVersions.get(releaseVersion.majorMinor)

  def getOrElse(
      releaseVersion: ReleaseVersion,
      default: => NonEmpty[List[ProtocolVersion]],
  ): NonEmpty[List[ProtocolVersion]] =
    majorMinorToProtocolVersions.getOrElse(releaseVersion.majorMinor, default)
}
