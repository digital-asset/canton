// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.traverse._
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

import scala.annotation.tailrec
import scala.util.Try

/** Trait that represents how a version in Canton is modelled. */
sealed trait CantonVersion extends Ordered[CantonVersion] with PrettyPrinting {

  def major: Int
  def minor: Int
  def patch: Int
  def optSuffix: Option[String]
  def isSnapshot: Boolean = optSuffix.contains("SNAPSHOT")
  def isStable: Boolean = optSuffix.isEmpty
  def fullVersion: String = s"$major.$minor.$patch${optSuffix.map("-" + _).getOrElse("")}"

  override def pretty: Pretty[CantonVersion] = prettyOfString(_ => fullVersion)
  def toProtoPrimitive: String = fullVersion

  def raw: (Int, Int, Int, Option[String]) = (major, minor, patch, optSuffix)

  /* Compared according to the SemVer specification: https://semver.org/#spec-item-11
  (implementation was only tested when specifying a pre-release suffix, see `CantonVersionTest`, but not e.g. with a metadata suffix). */
  override def compare(that: CantonVersion): Int =
    if (this.major != that.major) this.major compare that.major
    else if (this.minor != that.minor) this.minor compare that.minor
    else if (this.patch != that.patch) this.patch compare that.patch
    else suffixComparison(this.optSuffix, that.optSuffix)

  def suffixComparison(maybeSuffix1: Option[String], maybeSuffix2: Option[String]): Int = {
    (maybeSuffix1, maybeSuffix2) match {
      case (None, None) => 0
      case (None, Some(_)) => 1
      case (Some(_), None) => -1
      case (Some(suffix1), Some(suffix2)) =>
        suffixComparisonInternal(suffix1.split("\\.").toSeq, suffix2.split("\\.").toSeq)
    }
  }

  private def suffixComparisonInternal(suffixes1: Seq[String], suffixes2: Seq[String]): Int = {
    // partially adapted (and generalised) from gist.github.com/huntc/35f6cec0a47ce7ef62c0 (Apache 2 license)
    type PreRelease = Either[String, Int]
    def toPreRelease(s: String): PreRelease = Try(Right(s.toInt)).getOrElse(Left(s))

    def comparePreReleases(preRelease: PreRelease, thatPreRelease: PreRelease): Int =
      (preRelease, thatPreRelease) match {
        case (Left(str1), Left(str2)) =>
          str1 compare str2
        case (Left(_), Right(_)) =>
          1
        case (Right(number1), Right(number2)) =>
          number1 compare number2
        case (Right(_), Left(_)) =>
          -1
      }

    @tailrec
    def go(
        suffix1: Option[String],
        suffix2: Option[String],
        tail1: Seq[String],
        tail2: Seq[String],
    ): Int = {
      (suffix1, suffix2) match {
        case (None, None) => 0
        // if we have a suffix (else we would have terminated earlier), then more suffixes are better
        case (None, Some(_)) => -1
        case (Some(_), None) => 1
        case (Some(suffix1), Some(suffix2)) =>
          val res = comparePreReleases(toPreRelease(suffix1), toPreRelease(suffix2))
          if (res != 0) res
          else go(tail1.headOption, tail2.headOption, tail1.drop(1), tail2.drop(1))
      }
    }

    go(suffixes1.headOption, suffixes2.headOption, suffixes1.drop(1), suffixes2.drop(1))
  }
}

sealed trait CompanionTrait {
  protected def createInternal(
      rawVersion: String,
      baseName: String,
  ): Either[String, (Int, Int, Int, Option[String])] = {
    // `?:` removes the capturing group, so we get a cleaner pattern-match statement
    val regex = raw"([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,4})(?:-(.*))?".r

    rawVersion match {
      case regex(rawMajor, rawMinor, rawPatch, suffix) =>
        val parsedDigits = List(rawMajor, rawMinor, rawPatch).traverse(raw =>
          raw.toIntOption.toRight(s"Couldn't parse number `$raw`")
        )
        parsedDigits.flatMap {
          case List(major, minor, patch) =>
            // `suffix` is `null` if no suffix is given
            Right((major, minor, patch, Option(suffix)))
          case _ => Left(s"Unexpected error while parsing version `$rawVersion`")
        }

      case _ =>
        Left(
          s"Unable to convert string `$rawVersion` to a valid ${baseName}. A ${baseName} is similar to a semantic version. For example, '1.2.3' or '1.2.3-SNAPSHOT' are valid ${baseName}s."
        )
    }
  }

}

/** This class represent a release version.
  * Please refer to the [[https://docs.daml.com/canton/usermanual/versioning.html versioning documentation]]
  * in the user manual for details.
  */
final case class ReleaseVersion(
    major: Int,
    minor: Int,
    patch: Int,
    optSuffix: Option[String] = None,
) extends CantonVersion {
  def majorMinor: (Int, Int) = (major, minor)
}

object ReleaseVersion extends CompanionTrait {

  def create(rawVersion: String): Either[String, ReleaseVersion] =
    createInternal(rawVersion, ReleaseVersion.getClass.getSimpleName).map {
      case (major, minor, patch, optSuffix) =>
        new ReleaseVersion(major, minor, patch, optSuffix)
    }
  def tryCreate(rawVersion: String): ReleaseVersion = create(rawVersion).fold(sys.error, identity)

  /** The release this process belongs to. */
  val current: ReleaseVersion = ReleaseVersion.tryCreate(BuildInfo.version)
}

/** This class represents a revision of the Sequencer.sol contract. */
final case class EthereumContractVersion(
    major: Int,
    minor: Int,
    patch: Int,
    optSuffix: Option[String] = None,
) extends CantonVersion

object EthereumContractVersion extends CompanionTrait {

  def create(rawVersion: String): Either[String, EthereumContractVersion] =
    createInternal(rawVersion, EthereumContractVersion.getClass.getSimpleName).map {
      case (major, minor, patch, optSuffix) =>
        new EthereumContractVersion(major, minor, patch, optSuffix)
    }
  def tryCreate(rawVersion: String): EthereumContractVersion =
    create(rawVersion).fold(sys.error, identity)

  /** Which revisions of the Sequencer.sol contract are supported and can be deployed by a certain release? */
  def tryReleaseVersionToEthereumContractVersions(
      v: ReleaseVersion
  ): NonEmpty[List[EthereumContractVersion]] = {
    assert(ReleaseVersionToProtocolVersions.get(v).isDefined)
    if (v < ReleaseVersions.v2_3_0)
      NonEmpty(List, v1_0_0)
    else
      NonEmpty(List, v1_0_0, v1_0_1)
  }

  lazy val v1_0_0: EthereumContractVersion = EthereumContractVersion(1, 0, 0)
  lazy val v1_0_1: EthereumContractVersion = EthereumContractVersion(1, 0, 1)

  lazy val allKnownVersions = List(v1_0_0, v1_0_1)

  lazy val latest: EthereumContractVersion = tryReleaseVersionToEthereumContractVersions(
    ReleaseVersion.current
  ).max1
  lazy val versionInTests: EthereumContractVersion = latest

}
