// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.language.LanguageVersion.Major.*
import com.digitalasset.daml.lf.language.LanguageVersion.Minor.*

import scala.annotation.nowarn
import scala.collection.MapView

trait LanguageVersionGenerated {
  val allStableLegacyLfVersions: List[LanguageVersion] =
    List(6, 7, 8, 11, 12, 13, 14, 15, 17).map(i => LanguageVersion(V1, Stable(i)))
  val List(v1_6, v1_7, v1_8, v1_11, v1_12, v1_13, v1_14, v1_15, v1_17) =
    allStableLegacyLfVersions: @nowarn(
      "msg=match may not be exhaustive"
    )
  val v1_dev: LanguageVersion = LanguageVersion(V1, Dev)
  val allLegacyLfVersions: List[LanguageVersion] = allStableLegacyLfVersions.appended(v1_dev)

  lazy val explicitVersions: MapView[String, LanguageVersion] =
    BuildInfo.explicitVersionsDTO.view.mapValues(LanguageVersion.assertFromDTOJson)

  lazy val v2_1: LanguageVersion = explicitVersions("v2_1")
  lazy val v2_2: LanguageVersion = explicitVersions("v2_2")
  // 2.3 staging changelog
  // 02/26 introduced rev 1
  // 02/26 added SECP256K1_VALIDATE_KEY to rev 1 (no breaking change, no new rev)
  // 03/26 added QueryNByKey to rev 1 (no breaking change, no new rev)
  // 03/25 moved ExerciseByKey and FetchByKey to rev 1 (no breaking change, no new rev)
  lazy val v2_3_1: LanguageVersion = explicitVersions("v2_3_1")
  lazy val v2_3: LanguageVersion = explicitVersions("v2_3")
  lazy val v2_dev: LanguageVersion = explicitVersions("v2_dev")

  lazy val namedVersions: MapView[String, LanguageVersion] =
    BuildInfo.namedVersionsDTO.view.mapValues(LanguageVersion.assertFromDTOJson)

  lazy val defaultLfVersion = namedVersions("defaultLfVersion")
  lazy val devLfVersion = namedVersions("devLfVersion")
  lazy val latestStableLfVersion = namedVersions("latestStableLfVersion")

  lazy val versionLists: MapView[String, Seq[LanguageVersion]] =
    BuildInfo.versionListsDTO.view.mapValues(_.map(LanguageVersion.assertFromDTOJson))

  lazy val allLfVersions = versionLists("allLfVersions")
  lazy val stableLfVersions = versionLists("stableLfVersions")
  // DEPRECATED langauge lists
  lazy val compilerLfVersions = versionLists("compilerLfVersions")

  // ranges hardcoded (for now)
  lazy val allLfVersionsRange: VersionRange.Inclusive[LanguageVersion] = VersionRange(v2_1, v2_dev)
  lazy val stableLfVersionsRange: VersionRange.Inclusive[LanguageVersion] = VersionRange(v2_1, v2_2)
  lazy val earlyAccessLfVersionsRange: VersionRange.Inclusive[LanguageVersion] =
    VersionRange(v2_1, v2_2)
}
