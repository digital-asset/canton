// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//
// THIS FILE WAS GENERATED FROM //canton/community/daml-lf/language/daml-lf.bzl
// (via generate_features.py)
//
// IT IS CHECKED IN AS A TEMPORARY MEASURE FOR THE MIGRATION OF LF/ENGINE TO THE
// CANTON REPO IN THE FUTURE, IT WILL BE AUTO-GENERATED ONCE MORE
//

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.language.LanguageVersion.{Major, Minor}

import scala.annotation.nowarn

trait LegacyLanguageVersions {
  val allStableLegacyLfVersions: List[LanguageVersion] =
    List(6, 7, 8, 11, 12, 13, 14, 15, 17).map(i => LanguageVersion(Major.V1, Minor.Stable(i)))
  val List(v1_6, v1_7, v1_8, v1_11, v1_12, v1_13, v1_14, v1_15, v1_17) =
    allStableLegacyLfVersions: @nowarn("msg=match may not be exhaustive")
  val v1_dev: LanguageVersion = LanguageVersion(Major.V1, Minor.Dev)
  val allLegacyLfVersions: List[LanguageVersion] = allStableLegacyLfVersions.appended(v1_dev)
}

trait LegacyLanguageFeatures extends LegacyLanguageVersions {
  object LegacyFeatures {
    val default = v1_6
    val internedPackageId = v1_6
    val internedStrings = v1_7
    val internedDottedNames = v1_7
    val numeric = v1_7
    val anyType = v1_7
    val typeRep = v1_7
    val typeSynonyms = v1_8
    val packageMetadata = v1_8
    val genComparison = v1_11
    val genMap = v1_11
    val scenarioMustFailAtMsg = v1_11
    val contractIdTextConversions = v1_11
    val exerciseByKey = v1_11
    val internedTypes = v1_11
    val choiceObservers = v1_11
    val bigNumeric = v1_13
    val exceptions = v1_14
    val basicInterfaces = v1_15
    val choiceFuncs = v1_dev
    val choiceAuthority = v1_dev
    val natTypeErasure = v1_dev
    val packageUpgrades = v1_17
    val sharedKeys = v1_17
    val templateTypeRepToText = v1_dev
    val extendedInterfaces = v1_dev
    val unstable = v1_dev
  }
}
