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

import com.digitalasset.daml.lf.language.LanguageVersion.{Feature, Major, Minor}

import scala.annotation.nowarn

trait LanguageFeaturesGenerated extends LanguageVersionGenerated {
  val allStableLegacyLfVersions: List[LanguageVersion] =
    List(6, 7, 8, 11, 12, 13, 14, 15, 17).map(i => LanguageVersion(Major.V1, Minor.Stable(i)))
  val List(v1_6, v1_7, v1_8, v1_11, v1_12, v1_13, v1_14, v1_15, v1_17) =
    allStableLegacyLfVersions: @nowarn("msg=match may not be exhaustive")
  val v1_dev: LanguageVersion = LanguageVersion(Major.V1, Minor.Dev)
  val allLegacyLfVersions: List[LanguageVersion] = allStableLegacyLfVersions.appended(v1_dev)

  // TODO (FEATURE): Remove this hardcoded object once V1 features are also generated
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

  // --- Hardcoded V2 Features ---

  val featureUnstable: Feature = Feature(
    name = "Unstable, experimental features",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureTextMap: Feature = Feature(
    name = "TextMap type",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureBigNumeric: Feature = Feature(
    name = "BigNumeric type",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureExceptions: Feature = Feature(
    name = "Daml Exceptions",
    versionRange = VersionRange.From(v2_1),
  )

  val featureExtendedInterfaces: Feature = Feature(
    name = "Guards in interfaces",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureChoiceFuncs: Feature = Feature(
    name = "choiceController and choiceObserver functions",
    versionRange = VersionRange.Empty(),
  )

  val featureTemplateTypeRepToText: Feature = Feature(
    name = "templateTypeRepToText function",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureUCKBuiltins: Feature = Feature(
    name =
      "Old style (UCK) key builtins (fetchByKey, exerciseByKey (UCK semantics), lookupByKey (UCK semantics), ...)",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureNUCK: Feature = Feature(
    name = "Non-unique contract keys",
    versionRange = VersionRange.From(v2_3_1), // dev whilst developing, then 2.3(-rcn)
  )

  val featureFetchBykey: Feature = Feature(
    name = "Fetch by key",
    versionRange = featureNUCK.versionRange,
  )

  val featureExerciseBykey: Feature = Feature(
    name = "Exercise by key",
    versionRange = featureNUCK.versionRange,
  )

  val featureLookupBykey: Feature = Feature(
    name = "Lookup by key",
    versionRange = featureUCKBuiltins.versionRange,
  )

  val featureContractKeys: Feature = Feature(
    name = "Contract Keys",
    versionRange = VersionRange.From(v2_3_1),
  )

  val featureFlatArchive: Feature = Feature(
    name = "Flat Archive",
    versionRange = VersionRange.From(v2_2),
  )

  val featurePackageImports: Feature = Feature(
    name = "Explicit package imports",
    versionRange = VersionRange.From(v2_2),
  )

  val featureComplexAnyType: Feature = Feature(
    name = "Complex Any type",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureExperimental: Feature = Feature(
    name = "Daml Experimental",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featurePackageUpgrades: Feature = Feature(
    name = "Package upgrades",
    versionRange = VersionRange.From(v2_1),
  )

  val featureChoiceAuthority: Feature = Feature(
    name = "Choice Authorizers",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureExternalCall: Feature = Feature(
    name = "External Call",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureUnsafeFromInterface: Feature = Feature(
    name = "UnsafeFromInterface builtin",
    versionRange = VersionRange.Until(v2_1),
  )

  val featureExtendedCryptoPrimitives: Feature = Feature(
    name = "Extended crypto primitives",
    versionRange = VersionRange.From(v2_3_1),
  )

  val allFeatures: List[Feature] = List(
    featureUnstable,
    featureTextMap,
    featureBigNumeric,
    featureExceptions,
    featureExtendedInterfaces,
    featureChoiceFuncs,
    featureTemplateTypeRepToText,
    featureUCKBuiltins,
    featureFetchBykey,
    featureExerciseBykey,
    featureLookupBykey,
    featureContractKeys,
    featureFlatArchive,
    featurePackageImports,
    featureComplexAnyType,
    featureExperimental,
    featurePackageUpgrades,
    featureChoiceAuthority,
    featureExternalCall,
    featureUnsafeFromInterface,
  )

}
