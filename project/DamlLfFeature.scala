// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import sbt.*
import sbt.Keys.*
import sbt.nio.Keys as _
import DamlLfVersion.*

final case class DamlLfFeature(
    name: String,
    cppFlag: String,
    versionRange: DamlLfFeature.VersionRange,
) {
  def toScala: String = s"""Feature(name = "$name", versionRange = ${versionRange.toScala})"""
}

object DamlLfFeature {
  sealed trait VersionRange extends Product with Serializable {
    def toScala: String
  }

  object VersionRange {
    final case class Inclusive(lowerBound: DamlLfVersion, upperBound: DamlLfVersion)
        extends VersionRange {
      override def toScala: String =
        s"VersionRange.Inclusive(${lowerBound.toScala}, ${upperBound.toScala})"
    }

    final case class Until(upperBound: DamlLfVersion) extends VersionRange {
      override def toScala: String = s"VersionRange.Until(${upperBound.toScala})"
    }

    final case class From(lowerBound: DamlLfVersion) extends VersionRange {
      override def toScala: String = s"VersionRange.From(${lowerBound.toScala})"
    }

    final case class Empty() extends VersionRange {
      override def toScala: String = "VersionRange.Empty()"
    }

    implicit val encodeVersionRange: Encoder[VersionRange] = Encoder.instance {
      case v: Inclusive => v.asJson
      case v: Until => v.asJson
      case v: From => v.asJson
      case _: Empty => Json.obj("type" -> Json.fromString("Empty"))
    }
  }

  private val v2_1 = DamlLfVersion(2, Stable(1))
  private val v2_2 = DamlLfVersion(2, Stable(2))
  private val v2_3_1 = DamlLfVersion(2, Staging(3, 1))
  private val v2_dev = DamlLfVersion(2, Dev)

  val featureUnstable = DamlLfFeature(
    name = "Unstable, experimental features",
    cppFlag = "DAML_UNSTABLE",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureBigNumeric = DamlLfFeature(
    name = "BigNumeric type",
    cppFlag = "DAML_BIGNUMERIC",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureExceptions = DamlLfFeature(
    name = "Daml Exceptions",
    cppFlag = "DAML_EXCEPTIONS",
    versionRange = VersionRange.From(v2_1),
  )

  val featureExtendedInterfaces = DamlLfFeature(
    name = "Guards in interfaces",
    cppFlag = "DAML_INTERFACE_EXTENDED",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureChoiceFuncs = DamlLfFeature(
    name = "choiceController and choiceObserver functions",
    cppFlag = "DAML_CHOICE_FUNCS",
    versionRange = VersionRange.Empty(),
  )

  val featureTemplateTypeRepToText = DamlLfFeature(
    name = "templateTypeRepToText function",
    cppFlag = "DAML_TEMPLATE_TYPEREP_TO_TEXT",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureLegacyLookupByKey = DamlLfFeature(
    name = "Lookup by key (legacy, UCK variant)",
    cppFlag = "DAML_LEGACY_LOOKUP_BY_KEY",
    versionRange = VersionRange.Empty(),
  )

  val featureNUCK = DamlLfFeature(
    name = "Non-unique contract keys",
    cppFlag = "DAML_NUCK",
    versionRange = VersionRange.From(v2_3_1),
  )

  val featureContractKeys = DamlLfFeature(
    name = "Contract Keys",
    cppFlag = "DAML_CONTRACT_KEYS",
    versionRange = featureNUCK.versionRange,
  )

  val featureFlatArchive = DamlLfFeature(
    name = "Flat Archive",
    cppFlag = "DAML_FLATARCHIVE",
    versionRange = VersionRange.From(v2_2),
  )

  val featurePackageImports = DamlLfFeature(
    name = "Explicit package imports",
    cppFlag = "DAML_PackageImports",
    versionRange = VersionRange.From(v2_2),
  )

  val featureComplexAnyType = DamlLfFeature(
    name = "Complex Any type",
    cppFlag = "DAML_COMPLEX_ANY_TYPE",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureExperimental = DamlLfFeature(
    name = "Daml Experimental",
    cppFlag = "DAML_EXPERIMENTAL",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureChoiceAuthority = DamlLfFeature(
    name = "Choice Authorizers",
    cppFlag = "DAML_ChoiceAuthority",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureExternalCall = DamlLfFeature(
    name = "External Call",
    cppFlag = "DAML_ExternalCall",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureUnsafeFromInterface = DamlLfFeature(
    name = "UnsafeFromInterface builtin",
    cppFlag = "DAML_UnsafeFromInterface",
    versionRange = VersionRange.Until(v2_1),
  )

  val featureExtendedCryptoPrimitives = DamlLfFeature(
    name = "Extended crypto primitives",
    cppFlag = "DAML_ExtendedCryptoPrimitives",
    versionRange = VersionRange.From(v2_3_1),
  )

  val allFeatures: Map[String, DamlLfFeature] = Map(
    "featureUnstable" -> featureUnstable,
    "featureBigNumeric" -> featureBigNumeric,
    "featureExceptions" -> featureExceptions,
    "featureExtendedInterfaces" -> featureExtendedInterfaces,
    "featureChoiceFuncs" -> featureChoiceFuncs,
    "featureTemplateTypeRepToText" -> featureTemplateTypeRepToText,
    "featureNUCK" -> featureNUCK,
    "featureLegacyLookupByKey" -> featureLegacyLookupByKey,
    "featureContractKeys" -> featureContractKeys,
    "featureFlatArchive" -> featureFlatArchive,
    "featurePackageImports" -> featurePackageImports,
    "featureComplexAnyType" -> featureComplexAnyType,
    "featureExperimental" -> featureExperimental,
    "featureChoiceAuthority" -> featureChoiceAuthority,
    "featureExternalCall" -> featureExternalCall,
    "featureUnsafeFromInterface" -> featureUnsafeFromInterface,
    "featureExtendedCryptoPrimitives" -> featureExtendedCryptoPrimitives,
  )

  def generateFeaturesScala = Def.task {
    val sb = new StringBuilder
    sb.append(
      """|package com.digitalasset.daml.lf.language
         |
         |import com.digitalasset.daml.lf.language.LanguageVersion.{Feature, Major, Minor}
         |import com.digitalasset.daml.lf.VersionRange
         |
         |// AUTO-GENERATED from DamlLfFeature.scala, DO NOT EDIT
         |trait LanguageFeaturesGenerated {
         |""".stripMargin
    )

    for ((name, feature) <- allFeatures) {
      sb.append(s"  val $name: Feature = ${feature.toScala}\n")
    }

    val featureNames = allFeatures.keys.mkString(", ")
    sb.append(s"\n  val allFeatures: List[Feature] = List($featureNames)\n")

    sb.append("}\n")

    val outputFile =
      (Compile / sourceManaged).value / "com/digitalasset/daml/lf/language" / "LanguageFeaturesGenerated.scala"
    IO.write(outputFile, sb.toString)
    Seq(outputFile)
  }

  def generateFeaturesJson = Def.task {
    // 1. Generate JSON
    val jsonString = allFeatures.asJson.spaces2

    // 2. Write file (to target/scala-2.12/resource_managed/main/...)
    val outputFile = (Compile / resourceManaged).value / "daml-lf-features.json"
    IO.write(outputFile, jsonString)

    Seq(outputFile)
  }
}
