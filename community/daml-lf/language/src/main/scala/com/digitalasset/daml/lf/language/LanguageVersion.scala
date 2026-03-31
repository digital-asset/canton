// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import io.circe.{Decoder, Json}

import scala.math.Ordered.orderingToOrdered

final case class LanguageVersion private[lf] (
    major: LanguageVersion.Major,
    minor: LanguageVersion.Minor,
) extends Ordered[LanguageVersion] {

  def pretty = toString

  override def toString: String = s"${major.pretty}.${minor.pretty}"

  def isDevVersion: Boolean = minor.isDevVersion

  override def compare(that: LanguageVersion): Int =
    (this.major, this.minor).compare((that.major, that.minor))
}

object LanguageVersion extends LanguageFeaturesGenerated {

  /** Decode a LanguageVersion from a JSON string produced by the sbt plugin's Circe encoding of
    * LanguageVersionDto. Expected format: {"major":2,"minor":{"Stable":{"version":1}}}
    * {"major":2,"minor":{"Staging":{"version":3,"revision":1}}} {"major":2,"minor":{"Dev":{}}}
    *
    * Unchecked because it does not verify the parsed version is a known/released version.
    */
  def fromDTOJson(jsonStr: String): Either[String, LanguageVersion] =
    io.circe.parser.decode[LanguageVersion](jsonStr)(dtoDecoder).left.map(_.getMessage)

  def assertFromDTOJson(jsonStr: String): LanguageVersion = data.assertRight(fromDTOJson(jsonStr))

  private implicit lazy val minorDecoder: Decoder[Minor] = Decoder.instance { cursor =>
    cursor
      .downField("Stable")
      .as[Json]
      .flatMap(_.hcursor.downField("version").as[Int])
      .map(Minor.Stable(_))
      .orElse(
        cursor
          .downField("Staging")
          .as[Json]
          .flatMap { json =>
            for {
              version <- json.hcursor.downField("version").as[Int]
              revision <- json.hcursor.downField("revision").as[Int]
            } yield Minor.Staging(version, revision)
          }
      )
      .orElse(
        cursor.downField("Dev").as[Json].map(_ => Minor.Dev)
      )
  }

  private lazy val dtoDecoder: Decoder[LanguageVersion] = Decoder.instance { cursor =>
    for {
      majorInt <- cursor.downField("major").as[Int]
      minor <- cursor.downField("minor").as[Minor]
    } yield {
      val major = majorInt match {
        case 2 => Major.V2
        case unsupported =>
          throw new IllegalArgumentException(s"Unsupported major version: $unsupported")
      }
      LanguageVersion(major, minor)
    }
  }

  def assertFromString(s: String): LanguageVersion = data.assertRight(fromString(s))

  def fromString(str: String): Either[String, LanguageVersion] =
    (allLegacyLfVersions ++ allLfVersions)
      .find(_.toString == str)
      .toRight(
        s"Failed to parse $str, it is not supported (supported non-legacy versions are $allLfVersions)"
      )

  // TODO: remove after https://github.com/digital-asset/daml/issues/22403
  def supportsPackageUpgrades(lv: LanguageVersion): Boolean =
    lv.major match {
      case Major.V2 => featurePackageUpgrades.enabledIn(lv)
      case Major.V1 => lv >= LegacyFeatures.packageUpgrades
    }

  // TODO: remove after feature https://github.com/digital-asset/daml/issues/22403
  def allUpToVersion(version: LanguageVersion): VersionRange.Inclusive[LanguageVersion] =
    version.major match {
      case Major.V2 => VersionRange(v2_1, version)
      case _ => throw new IllegalArgumentException(s"${version.major.pretty} not supported")
    }

  sealed abstract class Major(val major: Int)
      extends Product
      with Serializable
      with Ordered[Major] {
    val pretty = major.toString

    override def compare(that: Major): Int = this.pretty.compare(that.pretty)
  }

  sealed abstract class Minor extends Product with Serializable with Ordered[Minor] {
    val toProtoIdentifier: String = pretty

    def pretty: String

    def isDevVersion: Boolean = false

    override def compare(that: Minor): Int = (this, that) match {
      // Dev > Staging > Stable
      case (Minor.Dev, Minor.Dev) => 0
      case (Minor.Dev, _) => 1
      case (_, Minor.Dev) => -1

      case (Minor.Staging(a, rca), Minor.Staging(b, rcb)) => (a, rca).compare((b, rcb))
      case (Minor.Staging(_, _), Minor.Stable(_)) => 1
      case (Minor.Stable(_), Minor.Staging(_, _)) => -1

      case (Minor.Stable(a), Minor.Stable(b)) => a.compare(b)
    }
  }

  final case class Feature(
      name: String,
      versionRange: VersionRange[LanguageVersion],
  ) {
    def enabledIn(lv: LanguageVersion): Boolean = versionRange.contains(lv)
  }

  object Major {
    private val allMajors = List(V1, V2)

    def fromString(str: String): Either[String, Major] =
      allMajors
        .find(_.pretty == str)
        .toRight(s"$str is not supported, supported majors: $allMajors")

    case object V1 extends Major(1)

    case object V2 extends Major(2)

  }

  object Minor {
    def assertFromString(s: String): Minor = data.assertRight(fromString(s))

    def fromString(str: String): Either[String, Minor] =
      (allLfVersions ++ allLegacyLfVersions)
        .map(_.minor)
        .find(_.pretty == str)
        .toRight(
          s"$str is not supported, supported minors: ${(allLfVersions ++ allLegacyLfVersions).map(_.minor)}"
        )

    final case class Stable(version: Int) extends Minor {
      override def pretty: String = version.toString
    }

    final case class Staging(version: Int, revision: Int) extends Minor {
      override def pretty: String = s"$version-rc$revision"
    }

    case object Dev extends Minor {
      override def pretty: String = "dev"

      override def isDevVersion: Boolean = true
    }
  }
}
