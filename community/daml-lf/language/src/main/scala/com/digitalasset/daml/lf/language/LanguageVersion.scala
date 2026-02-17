// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import scala.math.Ordered.orderingToOrdered


final case class LanguageVersion private(
                                          major: LanguageVersion.Major,
                                          minor: LanguageVersion.Minor,
                                        ) extends Ordered[LanguageVersion] {

  def pretty = toString

  override def toString: String = s"${major.pretty}.${minor.pretty}"

  def isDevVersion: Boolean = minor.isDevVersion

  override def compare(that: LanguageVersion): Int = {
    (this.major, this.minor).compare((that.major, that.minor))
  }
}

object LanguageVersion extends LanguageFeaturesGenerated {
  private[this] lazy val V2NumericRegex = """2\.(\d+)(?:-rc(\d+))?""".r

  def assertFromStringUnchecked(str: String): LanguageVersion = data.assertRight(fromStringUnchecked(str))

  /**
   * Unchecked because it does not check if the parsed version "exists" (if you pass it 2.n it will succeed, even if
   * 2.n is not released yet
   */
  def fromStringUnchecked(str: String): Either[String, LanguageVersion] = {
    str match {
      case "2.dev" =>
        Right(LanguageVersion(Major.V2, Minor.Dev))

      case V2NumericRegex(nStr, mStr) =>
        try {
          val n = nStr.toInt
          val minor = if (mStr == null) {
            Minor.Stable(n)
          } else {
            Minor.Staging(n, mStr.toInt)
          }
          Right(LanguageVersion(Major.V2, minor))
        } catch {
          case _: NumberFormatException =>
            Left(s"Version component in '$str' is too large for an Int")
        }
      case _ =>
        Left(s"Unsupported LF version: '$str'. Expected '2.dev', '2.n', or '2.n-rcm'")
    }
  }

  def assertFromString(s: String): LanguageVersion = data.assertRight(fromString(s))

  def fromString(str: String): Either[String, LanguageVersion] =
    (allLegacyLfVersions ++ allLfVersions)
      .find(_.toString == str)
      .toRight(s"Failed to parse ${str}, it is not supported (supported non-legacy versions are ${allLfVersions})")

  // TODO: remove after https://github.com/digital-asset/daml/issues/22403
  def supportsPackageUpgrades(lv: LanguageVersion): Boolean =
    lv.major match {
      case Major.V2 => featurePackageUpgrades.enabledIn(lv)
      case Major.V1 => lv >= LegacyFeatures.packageUpgrades
    }

  // TODO: remove after feature https://github.com/digital-asset/daml/issues/22403
  def allUpToVersion(version: LanguageVersion): VersionRange.Inclusive[LanguageVersion] = {
    version.major match {
      case Major.V2 => VersionRange(v2_1, version)
      case _ => throw new IllegalArgumentException(s"${version.major.pretty} not supported")
    }
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
        .toRight(s"${str} is not supported, supported majors: ${allMajors}")

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
          s"${str} is not supported, supported minors: ${(allLfVersions ++ allLegacyLfVersions).map(_.minor)}"
        )

    final case class Stable(version: Int) extends Minor {
      override def pretty: String = version.toString
    }

    final case class Staging(version: Int, revision: Int) extends Minor {
      override def pretty: String = s"${version}-rc${revision}"
    }

    case object Dev extends Minor {
      override def pretty: String = "dev"

      override def isDevVersion: Boolean = true
    }
  }
}
