// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import io.circe.*
import io.circe.syntax.*
import io.circe.generic.auto.*
import sbt.*
import sbt.Keys.*
import sbt.nio.{Keys as _, *}
import sbt.util.HashFileInfo

final case class DamlLfVersion(major: Int, minor: DamlLfVersion.Minor) {
  def dotted: String = s"$major.${minor.dotted}"
  def toJson: String = this.asJson.noSpaces
  def toScala: String = s"LanguageVersion(Major.V$major, ${minor.toScala})"
}

object DamlLfVersion {
  sealed trait Minor extends Product with Serializable {
    // TODO(#31057): remove dotted when damlc does not depend on it anymore
    def dotted: String
    def toScala: String
  }
  final case class Stable(version: Int) extends Minor {
    // TODO(#31057): remove when damlc does not depend on it anymore
    override def dotted: String = version.toString
    override def toScala: String = s"Minor.Stable($version)"
  }

  final case class Staging(version: Int, revision: Int) extends Minor {
    // TODO(#31057): remove when damlc does not depend on it anymore
    override def dotted: String = s"${version.toString}-rc$revision"
    override def toScala: String = s"Minor.Staging($version, $revision)"
  }

  case object Dev extends Minor {
    // TODO(#31057): remove when damlc does not depend on it anymore
    override def dotted: String = "dev"
    override def toScala: String = "Minor.Dev"
  }

  private val v2_1 = DamlLfVersion(2, Stable(1))
  private val v2_2 = DamlLfVersion(2, Stable(2))
  private val v2_3_1 = DamlLfVersion(2, Staging(3, 1))
  // keep v2_3 pointed to latest revision
  private val v2_3 = v2_3_1
  private val v2_dev = DamlLfVersion(2, Dev)

  val explicitVersions: Map[String, DamlLfVersion] = Map(
    "v2_1" -> v2_1,
    "v2_2" -> v2_2,
    "v2_3_1" -> v2_3_1,
    "v2_3" -> v2_3,
    "v2_dev" -> v2_dev,
  )
  val namedVersions: Map[String, DamlLfVersion] = Map(
    "defaultLfVersion" -> v2_2,
    "devLfVersion" -> v2_dev,
    "latestStableLfVersion" -> v2_2,
  )

  val allLfVersions = List(v2_1, v2_2, v2_3, v2_dev)
  private val stableLfVersions = List(v2_1, v2_2)
  private val compilerLfVersions = allLfVersions

  val versionLists: Map[String, List[DamlLfVersion]] = Map(
    "allLfVersions" -> allLfVersions,
    "stableLfVersions" -> stableLfVersions,
    "compilerInputLfVersions" -> compilerLfVersions,
    "compilerOutputLfVersions" -> compilerLfVersions,
    "compilerLfVersions" -> compilerLfVersions,
  )

  def generateVersionsScala = Def.task {
    val sb = new StringBuilder
    sb.append(
      """|package com.digitalasset.daml.lf.language
         |
         |import com.digitalasset.daml.lf.language.LanguageVersion
         |import com.digitalasset.daml.lf.language.LanguageVersion.*
         |
         |trait LanguageVersionGenerated {
         |""".stripMargin
    )

    for ((name, version) <- explicitVersions) {
      sb.append(s"  val $name: LanguageVersion = ${version.toScala}\n")
    }

    for ((name, version) <- namedVersions) {
      sb.append(s"  val $name: LanguageVersion = ${version.toScala}\n")
    }

    for ((name, versions) <- versionLists) {
      val elems = versions.map(_.toScala).mkString(", ")
      sb.append(s"  val $name: List[LanguageVersion] = List($elems)\n")
    }

    sb.append("}\n\n")

    val outputFile =
      (Compile / sourceManaged).value / "com/digitalasset/daml/lf/language" / "LanguageVersionGenerated.scala"
    IO.write(outputFile, sb.toString)
    Seq(outputFile)
  }

  def generateVersionsDTOJson = Def.task {
    case class LfVersionReport(
        explicitVersions: List[DamlLfVersion],
        namedVersions: Map[String, DamlLfVersion],
        versionLists: Map[String, List[DamlLfVersion]],
    )

    val report = LfVersionReport(
      explicitVersions = explicitVersions.values.toList,
      namedVersions = namedVersions,
      versionLists = versionLists,
    )

    val jsonString = report.asJson.spaces2

    val outputFile = (Compile / resourceManaged).value / "daml-lf-versions-dto.json"
    IO.write(outputFile, jsonString)

    Seq(outputFile)
  }

  def generateVersionsJson = Def.task {
    // TODO(#31057):remove this object in favour of dto
    case class LfVersionReport(
        explicitVersions: Seq[String],
        namedVersions: Map[String, String],
        versionLists: Map[String, List[String]],
    )

    // 1. Gather data
    val report = LfVersionReport(
      explicitVersions = explicitVersions.values.toList.map(_.dotted),
      namedVersions = namedVersions.mapValues(_.dotted),
      versionLists = versionLists.mapValues(_.map(_.dotted)),
    )

    // 2. Generate JSON
    val jsonString = report.asJson.spaces2

    // 3. Write file (to target/scala-2.12/resource_managed/main/...)
    val outputFile = (Compile / resourceManaged).value / "daml-lf-versions.json"
    IO.write(outputFile, jsonString)

    Seq(outputFile)
  }
}
