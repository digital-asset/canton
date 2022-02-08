// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either._
import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.StringConversionError
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion.{
  InvalidProtocolVersion,
  UnsupportedVersion,
  supportedProtocolsDomain,
  supportedProtocolsParticipant,
}
import pureconfig.error.FailureReason
import pureconfig.{ConfigReader, ConfigWriter}

import scala.annotation.tailrec
import scala.util.Try

/** Trait that represents how a version in Canton is modelled. */
trait CantonVersion extends Ordered[CantonVersion] with PrettyPrinting {

  def major: Int
  def minor: Int
  def patch: Int
  def isSnapshot: Boolean
  def optSuffix: Option[String]
  def fullVersion: String = s"$major.$minor.$patch${optSuffix.map("-" + _).getOrElse("")}"

  override def pretty: Pretty[CantonVersion] = prettyOfString(_ => fullVersion)
  def toProtoPrimitive: String = fullVersion

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

  def suffixComparisonInternal(suffixes1: Seq[String], suffixes2: Seq[String]): Int = {
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

object CantonVersion {

  private def pv(rawVersion: String): ProtocolVersion = ProtocolVersion.tryCreate(rawVersion)
  private def rv(rawVersion: String): ReleaseVersion = ReleaseVersion.tryCreate(rawVersion)

  private[version] def getSupportedProtocolsParticipantForRelease(
      release: ReleaseVersion
  ): List[ProtocolVersion] = {

    /* This map, as well as the analogue domain node map, will only become relevant once we have Canton GA and
     * at least version 1.0.1 of the Canton protocol. Until then, each release will only support one Canton protocol version
     * that corresponds to the release version. */
    val supportedProtocolsParticipant: Map[ReleaseVersion, List[ProtocolVersion]] = Map(
      rv(BuildInfo.version) -> List(pv(BuildInfo.version))
    )
    supportedProtocolsParticipant.getOrElse(
      release,
      sys.error(
        s"Please add the supported protocol versions of a participant of release version $release to `supportedProtocolsParticipant` in `CantonVersion.scala`."
      ),
    )
  }

  private[version] def getSupportedProtocolsDomainForRelease(
      release: ReleaseVersion
  ): List[ProtocolVersion] = {
    val supportedProtocolsDomain: Map[ReleaseVersion, List[ProtocolVersion]] = Map(
      rv(BuildInfo.version) -> List(pv(BuildInfo.version))
    )
    supportedProtocolsDomain.getOrElse(
      release,
      sys.error(
        s"Please add the supported protocol versions of domain nodes of release version $release to `supportedProtocolsDomain` in `CantonVersion.scala`."
      ),
    )
  }

}

trait CompanionTrait {
  protected def createInternal(
      rawVersion: String
  ): Either[String, (Int, Int, Int, Boolean, Option[String])] = {
    // `?:` removes the capturing group, so we get a cleaner pattern-match statement
    val regex = raw"([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,4})(?:-(.*))?".r
    rawVersion match {
      case regex(rawMajor, rawMinor, rawPatch, suffix) =>
        val parsedDigits = List(rawMajor, rawMinor, rawPatch).traverse(raw =>
          Try(raw.toInt).toOption.toRight(s"Couldn't parse number $raw")
        )
        val isSnapshot = Option(suffix).exists(_.toUpperCase.contains("SNAPSHOT"))
        parsedDigits.flatMap {
          case List(major, minor, patch) =>
            // `suffix` is `null` if no suffix is given
            Right((major, minor, patch, isSnapshot, Option(suffix)))
          case _ => Left(s"Unexpected error while parsing version $rawVersion")
        }
      case _ =>
        Left(
          s"Unable to convert string `$rawVersion` to a valid CantonVersion. A valid CantonVersion would for example be '1.2.3' with optionally the suffix '-SNAPSHOT''. "
        )
    }
  }

}

/** This class represent a release version.
  * Please refer to the [[https://www.canton.io/docs/stable/user-manual/usermanual/versioning.html versioning documentation]]
  * in the user manual for details.
  */
final case class ReleaseVersion(
    major: Int,
    minor: Int,
    patch: Int,
    override val isSnapshot: Boolean = false,
    optSuffix: Option[String] = None,
) extends CantonVersion

object ReleaseVersion extends CompanionTrait {
  private[this] def apply(n: Int): ReleaseVersion = throw new UnsupportedOperationException(
    "Use create method"
  )

  def create(rawVersion: String): Either[String, ReleaseVersion] =
    createInternal(rawVersion).map { case (major, minor, patch, isSnapshot, optSuffix) =>
      new ReleaseVersion(major, minor, patch, isSnapshot, optSuffix)
    }
  def tryCreate(rawVersion: String): ReleaseVersion = create(rawVersion).fold(sys.error, identity)

  /** The release this process belongs to. */
  val current: ReleaseVersion = ReleaseVersion.tryCreate(BuildInfo.version)
}

/** A Canton protocol version is a snapshot of how the Canton protocols, that nodes use to communicate, function at a certain point in time
  * (e.g., this ‘snapshot’ contains the information how exactly a `SubmissionRequest` to the sequencer looks like and how exactly a Sequencer handles a call of the `SendAsync` RPC).
  * It is supposed to capture everything that is involved in two different Canton nodes interacting with each other.
  *
  * The protocol version is important for ensuring we meet our compatibility guarantees such that we can
  *  - update systems running older Canton versions
  *  - migrate data from older versions in the database
  *  - communicate with Canton nodes of different releases
  *
  * We identify protocol versions through a major, minor and patch digit. Technically, each functional change to a
  * protocol version is a breaking change in the sense of semantic versioning and thus should lead to a major version according to SemVer.
  * To communicate in a more practical, still semantic-versioning-adjacent manner, which protocol versions a certain Canton node supports,
  * each Canton node documents the highest protocol it supports and also supports all previous protocol versions (of the same major version).
  * For example, if the latest protocol version is 1.2.0, and the previous protocol versions are 1.0.0, 1.1.0, and 1.1.1, then
  * a Canton component that exposes protocol version 1.2.0 as highest protocol version also supports 1.0.0, 1.1.0, and 1.1.1.
  *
  * We say that two protocol versions are compatible if they share the same major version (e.g. 1.3 and 1.7 are compatible, 1.3 and 2.2 are not).
  * If two Canton nodes have a protocol version which is compatible, they can transact and interact with each-other (using one of the protocol versions they share).
  * Two Canton nodes coming from the same release are always guaranteed to be compatible in such a way.
  *
  * For more details, please refer to the [[https://www.canton.io/docs/stable/user-manual/usermanual/versioning.html versioning documentation]]
  * in the user manual.
  */
// Internal only: for the full background, please refer to the following [design doc](https://docs.google.com/document/d/1kDiN-373bZOWploDrtOJ69m_0nKFu_23RNzmEXQOFc8/edit?usp=sharing).
final case class ProtocolVersion(
    major: Int,
    minor: Int,
    patch: Int,
    override val isSnapshot: Boolean = false,
    optSuffix: Option[String] = None,
) extends CantonVersion

object ProtocolVersion extends CompanionTrait {
  private[this] def apply(n: Int): ProtocolVersion = throw new UnsupportedOperationException(
    "Use create method"
  )
  val current: ProtocolVersion = ProtocolVersion.tryCreate(BuildInfo.protocolVersion)
  val latest: ProtocolVersion = current
  // Might not be `latest` at some point in the future
  val default: ProtocolVersion = latest

  def create(rawVersion: String): Either[String, ProtocolVersion] =
    createInternal(rawVersion).map { case (major, minor, patch, isSnapshot, optSuffix) =>
      new ProtocolVersion(major, minor, patch, isSnapshot, optSuffix)
    }

  def tryCreate(rawVersion: String): ProtocolVersion = create(rawVersion).fold(sys.error, identity)

  def fromProtoPrimitive(rawVersion: String): ParsingResult[ProtocolVersion] =
    ProtocolVersion.create(rawVersion).leftMap(StringConversionError)

  /** Returns the protocol versions supported by the participant of the current release.
    */
  def supportedProtocolsParticipant: Seq[ProtocolVersion] =
    CantonVersion.getSupportedProtocolsParticipantForRelease(ReleaseVersion.current)

  /** Returns the protocol versions supported by domain nodes of the current release.
    */
  def supportedProtocolsDomain: Seq[ProtocolVersion] =
    CantonVersion.getSupportedProtocolsDomainForRelease(ReleaseVersion.current)

  final case class InvalidProtocolVersion(override val description: String) extends FailureReason
  final case class UnsupportedVersion(version: ProtocolVersion, supported: Seq[ProtocolVersion])
      extends FailureReason {
    override def description: String =
      s"CantonVersion $version is not supported! The supported versions are ${supported.map(_.toString).mkString(", ")}. Please configure one of these protocol versions in the DomainParameters. "
  }

  /** Returns successfully if the client and server should be compatible.
    * Otherwise returns an error message.
    *
    * We do this by compatibility check by going through a list of all versions instead of, for example, just checking
    * whether the supported version by the client is larger than the required version by the server,
    * to ensure that we don't run into issues with patches for old minor versions.
    * Otherwise, the situation may occur where, for example, the latest protocol version is 1.3.0 but we release patch
    * release version 1.1.1 after the release of version 1.3.0. If we would just check the major versions, version 1.3.0
    * would then mistakenly think that it is compatible with a node running version 1.1.1.
    *
    * This sort of error can occur because Canton is operated in a distributed environment where not every node is on the
    * same version.
    */
  def canClientConnectToServer(
      clientSupportedVersions: Seq[ProtocolVersion],
      server: ProtocolVersion,
      clientMinimumProtocolVersion: Option[ProtocolVersion],
  ): Either[HandshakeError, Unit] = {
    val clientSupportsRequiredVersion = clientSupportedVersions.contains(server)
    val clientMinVersionLargerThanReqVersion = clientMinimumProtocolVersion.exists(_ > server)
    if (clientMinVersionLargerThanReqVersion)
      Left(MinProtocolError(server, clientMinimumProtocolVersion, clientSupportsRequiredVersion))
    else if (!clientSupportsRequiredVersion)
      Left(VersionNotSupportedError(server, clientSupportedVersions))
    else Right(())
  }
}

sealed trait HandshakeError {
  def description: String
}
case class MinProtocolError(
    server: ProtocolVersion,
    clientMinimumProtocolVersion: Option[ProtocolVersion],
    clientSupportsRequiredVersion: Boolean,
) extends HandshakeError {
  override def description: String =
    s"The version required by the domain (${server.fullVersion}) is lower than the minimum version configured by the participant (${clientMinimumProtocolVersion
      .map(_.fullVersion)
      .getOrElse("")}). " +
      s"${if (clientSupportsRequiredVersion) "The participant supports the version required by the domain and would be able to connect to the domain if the minimum required version is configured to be lower."} "
}

case class VersionNotSupportedError(
    server: ProtocolVersion,
    clientSupportedVersions: Seq[ProtocolVersion],
) extends HandshakeError {
  override def description: String =
    s"The protocol version required by the server (${server.fullVersion}) is not among the supported protocol versions by the client $clientSupportedVersions. "
}

/** Wrapper around a [[ProtocolVersion]] so we can verify during configuration loading that domain operators only
  * configure a [[ProtocolVersion]] which is supported by the corresponding sequencer release.
  */
final case class DomainProtocolVersion(version: ProtocolVersion) {
  def unwrap: ProtocolVersion = version
}
object DomainProtocolVersion {
  implicit val domainProtocolVersionWriter: ConfigWriter[DomainProtocolVersion] =
    ConfigWriter.toString(_.version.fullVersion)
  lazy implicit val domainProtocolVersionReader: ConfigReader[DomainProtocolVersion] = {
    ConfigReader.fromString[DomainProtocolVersion] { str =>
      for {
        version <- ProtocolVersion.create(str).leftMap[FailureReason](InvalidProtocolVersion)
        _ <- Either.cond(
          supportedProtocolsDomain.contains(version),
          (),
          UnsupportedVersion(version, supportedProtocolsDomain),
        )
      } yield DomainProtocolVersion(version)
    }
  }
}

/** Wrapper around a [[ProtocolVersion]] so we can verify during configuration loading that participant operators only
  * configure a minimum [[ProtocolVersion]] in [[com.digitalasset.canton.participant.config.LocalParticipantConfig]]
  * which is supported by the corresponding participant release.
  */
final case class ParticipantProtocolVersion(version: ProtocolVersion) {
  def unwrap: ProtocolVersion = version
}
object ParticipantProtocolVersion {
  implicit val participantProtocolVersionWriter: ConfigWriter[ParticipantProtocolVersion] =
    ConfigWriter.toString(_.version.fullVersion)
  lazy implicit val participantProtocolVersionReader: ConfigReader[ParticipantProtocolVersion] = {
    ConfigReader.fromString[ParticipantProtocolVersion] { str =>
      for {
        version <- ProtocolVersion.create(str).leftMap[FailureReason](InvalidProtocolVersion)
        _ <- Either.cond(
          supportedProtocolsParticipant.contains(version),
          (),
          UnsupportedVersion(version, supportedProtocolsParticipant),
        )
      } yield ParticipantProtocolVersion(version)
    }
  }

}
