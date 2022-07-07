// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.error.ErrorCategory.MaliciousOrFaultyBehaviour
import com.daml.error.{ErrorCode, Explanation, Resolution}
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.ProtoDeserializationError.StringConversionError
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config.RequireTypes.InstanceName
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.HandshakeErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.CantonVersion.releaseVersionToProtocolVersions
import com.digitalasset.canton.version.ProtocolVersion.{
  InvalidProtocolVersion,
  UnsupportedVersion,
  deprecated,
}
import com.google.common.annotations.VisibleForTesting
import pureconfig.error.FailureReason
import pureconfig.{ConfigReader, ConfigWriter}

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

object CantonVersion {

  import ProtocolVersion._
  // At some point after Daml 3.0, this Map may diverge for domain and participant because we have
  // different compatibility guarantees for participants and domains and we will need to add separate maps for each
  // don't make `releaseVersionToProtocolVersions` private - it's used in `console-reference.canton`
  val releaseVersionToProtocolVersions: Map[ReleaseVersion, NonEmpty[List[ProtocolVersion]]] = Map(
    ReleaseVersion.v2_0_0_snapshot -> List(v2_0_0),
    ReleaseVersion.v2_0_0 -> List(v2_0_0),
    ReleaseVersion.v2_0_1 -> List(v2_0_0),
    ReleaseVersion.v2_1_0_snapshot -> List(v2_0_0),
    ReleaseVersion.v2_1_0 -> List(v2_0_0),
    ReleaseVersion.v2_1_0_rc1 -> List(v2_0_0),
    ReleaseVersion.v2_1_1_snapshot -> List(v2_0_0),
    ReleaseVersion.v2_1_1 -> List(v2_0_0),
    ReleaseVersion.v2_2_0_snapshot -> List(v2_0_0),
    ReleaseVersion.v2_2_0 -> List(v2_0_0),
    ReleaseVersion.v2_2_0_rc1 -> List(v2_0_0),
    ReleaseVersion.v2_2_0 -> List(v2_0_0),
    ReleaseVersion.v2_3_0_snapshot -> List(v2_0_0, v3_0_0),
    ReleaseVersion.v2_3_0_rc1 -> List(v2_0_0, v3_0_0),
    ReleaseVersion.v2_3_0 -> List(v2_0_0, v3_0_0),
    ReleaseVersion.v2_3_1_snapshot -> List(v2_0_0, v3_0_0),
  ).map { case (release, pvs) => (release, NonEmptyUtil.fromUnsafe(pvs)) }

}

sealed trait CompanionTrait {
  protected def createInternal(
      rawVersion: String
  ): Either[String, (Int, Int, Int, Option[String])] = {
    val dev = ProtocolVersion.unstable_development.fullVersion

    // `?:` removes the capturing group, so we get a cleaner pattern-match statement
    val regex = raw"([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,4})(?:-(.*))?".r
    rawVersion match {
      case regex(rawMajor, rawMinor, rawPatch, suffix) =>
        val parsedDigits = List(rawMajor, rawMinor, rawPatch).traverse(raw =>
          raw.toIntOption.toRight(s"Couldn't parse number $raw")
        )
        parsedDigits.flatMap {
          case List(major, minor, patch) =>
            // `suffix` is `null` if no suffix is given
            Right((major, minor, patch, Option(suffix)))
          case _ => Left(s"Unexpected error while parsing version $rawVersion")
        }

      // Since dev uses Int.MaxValue, it does not satisfy the regex above
      case `dev` => Right(ProtocolVersion.unstable_development.raw)

      case _ =>
        Left(
          s"Unable to convert string $rawVersion to a valid CantonVersion. A CantonVersion is similar to a semantic version. For example, '1.2.3' or '1.2.3-SNAPSHOT' are valid CantonVersions."
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
    optSuffix: Option[String] = None,
) extends CantonVersion

object ReleaseVersion extends CompanionTrait {

  def create(rawVersion: String): Either[String, ReleaseVersion] =
    createInternal(rawVersion).map { case (major, minor, patch, optSuffix) =>
      new ReleaseVersion(major, minor, patch, optSuffix)
    }
  def tryCreate(rawVersion: String): ReleaseVersion = create(rawVersion).fold(sys.error, identity)

  /** The release this process belongs to. */
  val current: ReleaseVersion = ReleaseVersion.tryCreate(BuildInfo.version)
  lazy val v2_0_0_snapshot: ReleaseVersion = ReleaseVersion(2, 0, 0, Some("SNAPSHOT"))
  lazy val v2_0_0: ReleaseVersion = ReleaseVersion(2, 0, 0)
  lazy val v2_0_1: ReleaseVersion = ReleaseVersion(2, 0, 1)
  lazy val v2_1_0_snapshot: ReleaseVersion = ReleaseVersion(2, 1, 0, Some("SNAPSHOT"))
  lazy val v2_1_0: ReleaseVersion = ReleaseVersion(2, 1, 0)
  lazy val v2_1_0_rc1: ReleaseVersion = ReleaseVersion(2, 1, 0, Some("rc1"))
  lazy val v2_1_1_snapshot: ReleaseVersion = ReleaseVersion(2, 1, 1, Some("SNAPSHOT"))
  lazy val v2_1_1: ReleaseVersion = ReleaseVersion(2, 1, 1)
  lazy val v2_2_0_snapshot: ReleaseVersion = ReleaseVersion(2, 2, 0, Some("SNAPSHOT"))
  lazy val v2_2_0_rc1: ReleaseVersion = ReleaseVersion(2, 2, 0, Some("rc1"))
  lazy val v2_2_0: ReleaseVersion = ReleaseVersion(2, 2, 0)
  lazy val v2_3_0_snapshot: ReleaseVersion = ReleaseVersion(2, 3, 0, Some("SNAPSHOT"))
  lazy val v2_3_0_rc1: ReleaseVersion = ReleaseVersion(2, 3, 0, Some("rc1"))
  lazy val v2_3_0: ReleaseVersion = ReleaseVersion(2, 3, 0)
  lazy val v2_3_1_snapshot: ReleaseVersion = ReleaseVersion(2, 3, 1, Some("SNAPSHOT"))

}

/** A Canton protocol version is a snapshot of how the Canton protocols, that nodes use to communicate, function at a certain point in time
  * (e.g., this ‘snapshot’ contains the information what exactly a `SubmissionRequest` to the sequencer looks like and how exactly a Sequencer handles a call of the `SendAsync` RPC).
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
// or [code walkthrough](https://drive.google.com/file/d/199wHq-P5pVPkitu_AYLR4V3i0fJtYRPg/view?usp=sharing)
final case class ProtocolVersion(
    major: Int,
    minor: Int,
    patch: Int,
    optSuffix: Option[String] = None,
) extends CantonVersion {
  def isDeprecated: Boolean = deprecated.contains(this)
}

/** When dealing with transfer, allow to be more precise with respect to the domain */
case class SourceProtocolVersion(v: ProtocolVersion) extends AnyVal
case class TargetProtocolVersion(v: ProtocolVersion) extends AnyVal

object ProtocolVersion extends CompanionTrait {
  private val allProtocolVersions: List[ProtocolVersion] =
    BuildInfo.protocolVersions.map(ProtocolVersion.tryCreate).toList

  val deprecated: Seq[ProtocolVersion] = Seq(ProtocolVersion.v2_0_0)

  val latest: ProtocolVersion =
    allProtocolVersions.maxOption.getOrElse(
      sys.error("Release needs to support at least one protocol version")
    )

  /** Should be used when hardcoding a protocol version for a test to signify that a hardcoded protocol version is safe
    * in this instance.
    */
  @VisibleForTesting
  val latestForTest: ProtocolVersion = latest

  def create(rawVersion: String): Either[String, ProtocolVersion] =
    createInternal(rawVersion).map { case (major, minor, patch, optSuffix) =>
      new ProtocolVersion(major, minor, patch, optSuffix)
    }

  /** Parse a ProtocolVersion
    * @param rawVersion
    * @return Parsed protocol version
    * @throws java.lang.RuntimeException if the given parameter cannot be parsed to a protocol version
    */
  def tryCreate(rawVersion: String): ProtocolVersion = create(rawVersion).fold(sys.error, identity)

  def fromProtoPrimitive(rawVersion: String): ParsingResult[ProtocolVersion] =
    ProtocolVersion.create(rawVersion).leftMap(StringConversionError)

  private def getDevelopmentVersions(
      includeDevelopmentVersions: Boolean
  ): List[ProtocolVersion] =
    if (includeDevelopmentVersions)
      List(ProtocolVersion.unstable_development)
    else List.empty

  /** Returns the protocol versions supported by the participant of the current release.
    */
  def supportedProtocolsParticipant(
      release: ReleaseVersion = ReleaseVersion.current,
      includeDevelopmentVersions: Boolean,
  ): NonEmpty[List[ProtocolVersion]] = {
    releaseVersionToProtocolVersions.getOrElse(
      release,
      sys.error(
        s"Please add the supported protocol versions of a participant of release version $release to `releaseVersionToProtocolVersions` in `CantonVersion.scala`."
      ),
    ) ++ getDevelopmentVersions(includeDevelopmentVersions)
  }

  /** Returns the protocol versions supported by the domain of the current release.
    */
  def supportedProtocolsDomain(
      release: ReleaseVersion = ReleaseVersion.current,
      includeDevelopmentVersions: Boolean,
  ): NonEmpty[List[ProtocolVersion]] = {
    releaseVersionToProtocolVersions.getOrElse(
      release,
      sys.error(
        s"Please add the supported protocol versions of domain nodes of release version $release to `releaseVersionToProtocolVersions` in `CantonVersion.scala`."
      ),
    ) ++ getDevelopmentVersions(includeDevelopmentVersions)
  }

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def getLatestSupportedProtocolDomain(release: ReleaseVersion): ProtocolVersion = {
    supportedProtocolsDomain(release, includeDevelopmentVersions = false).max
  }

  final case class InvalidProtocolVersion(override val description: String) extends FailureReason
  final case class UnsupportedVersion(version: ProtocolVersion, supported: Seq[ProtocolVersion])
      extends FailureReason {
    override def description: String =
      s"CantonVersion $version is not supported! The supported versions are ${supported.map(_.toString).mkString(", ")}. Please configure one of these protocol versions in the DomainParameters. "
  }

  /** Returns successfully if the client and server should be compatible.
    * Otherwise returns an error message.
    *
    * The client and server are compatible if the protocol version required by the server is not lower than
    * the clientMinimumVersion and the protocol version required by the server is among the protocol versions supported
    * by the client (exact string match).
    *
    * Note that this compatibility check cannot be implemented by simply verifying whether the supported
    * version by the client is larger than the required version by the server as this may lead to issues with
    * patches for old minor versions.
    * For example, if the latest release version is 1.3.0 but we release patch release version 1.1.1 after
    * the release of version 1.3.0, a node on version 1.3.0 which only checks whether
    * are versions are smaller, would mistakenly indicate that it is compatible with a node running version 1.1.1.
    * This issue is avoided if the client sends all protocol versions it supports and an exact string match is required.
    * Generally, this sort of error can occur because Canton is operated in a distributed environment where not every
    * node is on the same version.
    */
  def canClientConnectToServer(
      clientSupportedVersions: Seq[ProtocolVersion],
      server: ProtocolVersion,
      clientMinimumProtocolVersion: Option[ProtocolVersion],
  ): Either[HandshakeError, Unit] = {
    val clientSupportsRequiredVersion = clientSupportedVersions.contains(server)
    val clientMinVersionLargerThanReqVersion = clientMinimumProtocolVersion.exists(_ > server)
    // if dev-version support is on for participant and domain, ignore the min protocol version
    if (clientSupportsRequiredVersion && server == ProtocolVersion.unstable_development)
      Right(())
    else if (clientMinVersionLargerThanReqVersion)
      Left(MinProtocolError(server, clientMinimumProtocolVersion, clientSupportsRequiredVersion))
    else if (!clientSupportsRequiredVersion)
      Left(VersionNotSupportedError(server, clientSupportedVersions))
    else Right(())
  }

  lazy val unstable_development: ProtocolVersion = ProtocolVersion(Int.MaxValue, 0, 0, Some("DEV"))
  lazy val minimum_protocol_version: ProtocolVersion =
    ProtocolVersion(2, 0, 0) // Minimum stable protocol version introduced
  lazy val v2_0_0: ProtocolVersion = ProtocolVersion(2, 0, 0)
  lazy val v3_0_0: ProtocolVersion = ProtocolVersion(3, 0, 0)
  // TODO(i8793): signifies an instance where the protocol version is currently hardcoded but should likely be
  // passed in via propagating the protocol version set in the domain parameters
  lazy val v2_0_0_Todo_i8793: ProtocolVersion = v2_0_0

  /** @return Parsed protocol version if found in environment variable `CANTON_PROTOCOL_VERSION`
    * @throws java.lang.RuntimeException if the given parameter cannot be parsed to a protocol version
    */
  def tryGetOptFromEnv: Option[ProtocolVersion] = sys.env
    .get("CANTON_PROTOCOL_VERSION")
    .map(raw =>
      if (raw.toUpperCase == "DEV") ProtocolVersion.unstable_development.fullVersion else raw
    )
    .map(ProtocolVersion.tryCreate)
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
    createInternal(rawVersion).map { case (major, minor, patch, optSuffix) =>
      new EthereumContractVersion(major, minor, patch, optSuffix)
    }
  def tryCreate(rawVersion: String): EthereumContractVersion =
    create(rawVersion).fold(sys.error, identity)

  /** Which revisions of the Sequencer.sol contract are supported and can be deployed by a certain release? */
  def tryReleaseVersionToEthereumContractVersions(
      v: ReleaseVersion
  ): NonEmpty[List[EthereumContractVersion]] = {
    assert(CantonVersion.releaseVersionToProtocolVersions.contains(v))
    if (v < ReleaseVersion.v2_3_0_snapshot)
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

/** Trait for errors that are returned to clients when handshake fails. */
sealed trait HandshakeError {
  def description: String
}

final case class MinProtocolError(
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

final case class VersionNotSupportedError(
    server: ProtocolVersion,
    clientSupportedVersions: Seq[ProtocolVersion],
) extends HandshakeError {
  override def description: String =
    s"The protocol version required by the server (${server.fullVersion}) is not among the supported protocol versions by the client $clientSupportedVersions. "
}

object HandshakeErrors extends HandshakeErrorGroup {

  @Explanation(
    """This error is logged or returned if a participant or domain are using deprecated protocol versions.
      |Deprecated protocol versions might not be secure anymore."""
  )
  @Resolution(
    """Migrate to a new domain that uses the most recent protocol version."""
  )
  object DeprecatedProtocolVersion
      extends ErrorCode("DEPRECATED_PROTOCOL_VERSION", MaliciousOrFaultyBehaviour) {
    case class WarnSequencerClient(domainAlias: DomainAlias, version: ProtocolVersion)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"This node is connecting to a sequencer using the deprecated protocol version " +
            s"${version} which should not be used in production. We recommend only connecting to sequencers with a later protocol version (such as ${ProtocolVersion.latest})."
        )
    case class WarnDomain(name: InstanceName, version: ProtocolVersion)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          s"This domain node is configured to use the deprecated protocol version " +
            s"${version} which should not be used in production. We recommend migrating to a later protocol version (such as ${ProtocolVersion.latest})."
        )

    case class WarnParticipant(name: InstanceName, minimumProtocolVersion: Option[ProtocolVersion])(
        implicit val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          s"This participant node's configured minimum protocol version ${minimumProtocolVersion} includes deprecated protocol versions. " +
            s"We recommend using only the most recent protocol versions."
        ) {
      override def logOnCreation: Boolean = false
    }
  }
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
          // we support development versions when parsing, but catch dev versions without
          // the safety flag during config validation
          ProtocolVersion
            .supportedProtocolsDomain(includeDevelopmentVersions = true)
            .contains(version),
          (),
          UnsupportedVersion(
            version,
            ProtocolVersion.supportedProtocolsDomain(includeDevelopmentVersions = false),
          ),
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
          // same as domain: support parsing of dev
          ProtocolVersion
            .supportedProtocolsParticipant(includeDevelopmentVersions = true)
            .contains(version),
          (),
          UnsupportedVersion(
            version,
            ProtocolVersion.supportedProtocolsParticipant(includeDevelopmentVersions = false),
          ),
        )
      } yield ParticipantProtocolVersion(version)
    }
  }

}
