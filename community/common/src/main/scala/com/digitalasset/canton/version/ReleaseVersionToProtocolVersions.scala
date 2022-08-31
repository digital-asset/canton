// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}

object ReleaseVersionToProtocolVersions {

  import ProtocolVersion._
  // At some point after Daml 3.0, this Map may diverge for domain and participant because we have
  // different compatibility guarantees for participants and domains and we will need to add separate maps for each
  // don't make `releaseVersionToProtocolVersions` private - it's used in `console-reference.canton`
  val releaseVersionToProtocolVersions: Map[ReleaseVersion, NonEmpty[List[ProtocolVersion]]] = Map(
    ReleaseVersions.v2_0_0_snapshot -> List(v2_0_0),
    ReleaseVersions.v2_0_0 -> List(v2_0_0),
    ReleaseVersions.v2_0_1 -> List(v2_0_0),
    ReleaseVersions.v2_1_0_snapshot -> List(v2_0_0),
    ReleaseVersions.v2_1_0 -> List(v2_0_0),
    ReleaseVersions.v2_1_0_rc1 -> List(v2_0_0),
    ReleaseVersions.v2_1_1_snapshot -> List(v2_0_0),
    ReleaseVersions.v2_1_1 -> List(v2_0_0),
    ReleaseVersions.v2_2_0_snapshot -> List(v2_0_0),
    ReleaseVersions.v2_2_0 -> List(v2_0_0),
    ReleaseVersions.v2_2_0_rc1 -> List(v2_0_0),
    ReleaseVersions.v2_2_1 -> List(v2_0_0),
    ReleaseVersions.v2_2_0 -> List(v2_0_0),
    ReleaseVersions.v2_3_0_snapshot -> List(v2_0_0, v3_0_0),
    ReleaseVersions.v2_3_0_rc1 -> List(v2_0_0, v3_0_0),
    ReleaseVersions.v2_3_0 -> List(v2_0_0, v3_0_0),
    ReleaseVersions.v2_3_1 -> List(v2_0_0, v3_0_0),
    ReleaseVersions.v2_3_2 -> List(v2_0_0, v3_0_0),
    ReleaseVersions.v2_3_3 -> List(v2_0_0, v3_0_0),
    ReleaseVersions.v2_4_0_snapshot -> List(v2_0_0, v3_0_0),
  ).map { case (release, pvs) => (release, NonEmptyUtil.fromUnsafe(pvs)) }
}
