// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.error.ErrorCategory.MaliciousOrFaultyBehaviour
import com.daml.error.{ErrorCode, Explanation, Resolution}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.ProtoDeserializationError.StringConversionError
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config.RequireTypes.InstanceName
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.HandshakeErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion.{
  InvalidProtocolVersion,
  UnsupportedVersion,
  deprecated,
}
import com.digitalasset.canton.version.ReleaseVersionToProtocolVersions.releaseVersionToProtocolVersions
import pureconfig.error.FailureReason
import pureconfig.{ConfigReader, ConfigWriter}
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

/** A Canton protocol version is a snapshot of how the Canton protocols, that nodes use to communicate, function at a certain point in time
  * (e.g., this ‘snapshot’ contains the information what exactly a `SubmissionRequest` to the sequencer looks like and how exactly a Sequencer
  * handles a call of the `SendAsync` RPC).
  * It is supposed to capture everything that is involved in two different Canton nodes interacting with each other.
  *
  * The protocol version is important for ensuring we meet our compatibility guarantees such that we can
  *  - update systems running older Canton versions
  *  - migrate data from older versions in the database
  *  - communicate with Canton nodes of different releases
  *
  * Two Canton nodes can interact if they can speak the same protocol version.
  *
  * For more details, please refer to the [[https://docs.daml.com/canton/usermanual/versioning.html versioning documentation]]
  * in the user manual.
  */
// Internal only: for the full background, please refer to the following [design doc](https://docs.google.com/document/d/1kDiN-373bZOWploDrtOJ69m_0nKFu_23RNzmEXQOFc8/edit?usp=sharing).
// or [code walkthrough](https://drive.google.com/file/d/199wHq-P5pVPkitu_AYLR4V3i0fJtYRPg/view?usp=sharing)
final case class ProtocolVersion(v: Int) extends Ordered[ProtocolVersion] with PrettyPrinting {
  def isDeprecated: Boolean = deprecated.contains(this)

  def isDev: Boolean = v == Int.MaxValue
  def isStable: Boolean = !isDev

  override def pretty: Pretty[ProtocolVersion] =
    prettyOfString(_ => if (isDev) "dev" else v.toString)

  def toProtoPrimitive: Int = v

  // We keep the .0.0 so that old binaries can still decode it
  def toProtoPrimitiveS: String = s"$v.0.0"

  override def compare(that: ProtocolVersion): Int = v.compare(that.v)
}

object ProtocolVersion {

  implicit val protocolVersionWriter: ConfigWriter[ProtocolVersion] =
    ConfigWriter.toString(_.toProtoPrimitiveS)

  lazy implicit val protocolVersionReader: ConfigReader[ProtocolVersion] = {
    ConfigReader.fromString[ProtocolVersion] { str =>
      ProtocolVersion.create(str).leftMap[FailureReason](InvalidProtocolVersion)
    }
  }

  implicit val getResultProtocolVersion: GetResult[ProtocolVersion] =
    GetResult { r => ProtocolVersion(r.nextInt()) }

  implicit val setParameterProtocolVersion: SetParameter[ProtocolVersion] =
    (pv: ProtocolVersion, pp: PositionedParameters) => pp >> pv.v

  val all: List[ProtocolVersion] =
    BuildInfo.protocolVersions.map(ProtocolVersion.tryCreate).toList

  val deprecated: Seq[ProtocolVersion] = Seq(ProtocolVersion.v2)

  val latest: ProtocolVersion =
    all.maxOption.getOrElse(
      sys.error("Release needs to support at least one protocol version")
    )

  def lastStableVersions2: (ProtocolVersion, ProtocolVersion) = {
    val List(beforeLastStableProtocolVersion, lastStableProtocolVersion) =
      ProtocolVersion.all.sorted.takeRight(2): @unchecked

    (beforeLastStableProtocolVersion, lastStableProtocolVersion)
  }

  /** Try to parse a semver version.
    * Return:
    *
    * - None if `rawVersion` does not satisfy the semver regexp
    * - Some(Left(_)) if `rawVersion` satisfies the regex but if an error is found
    *   (e.g., if minor!=0).
    * - Some(Right(ProtocolVersion(_))) in case of success
    */
  private def parseSemver(rawVersion: String): Option[Either[String, ProtocolVersion]] = {
    val regex = raw"([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,4})".r

    rawVersion match {
      case regex(rawMajor, rawMinor, rawPatch) =>
        val parsedDigits = List(rawMajor, rawMinor, rawPatch).traverse(raw =>
          raw.toIntOption.toRight(s"Couldn't parse number $raw")
        )

        parsedDigits match {
          case Left(error) => Some(Left(error))

          case Right(List(major, minor, patch)) =>
            Some(
              Either.cond(
                minor == 0 && patch == 0,
                ProtocolVersion(major),
                s"Protocol version should consist of a single number; but `$rawVersion` found",
              )
            )

          case _ => Some(Left(s"Unexpected error while parsing version $rawVersion"))
        }

      case _ => None
    }
  }

  private def parseDev(rawVersion: String): Option[ProtocolVersion] = {
    // ignore case for dev version ... scala regex doesn't know case insensitivity ...
    val devRegex = "^[dD][eE][vV]$".r
    val devFull = ProtocolVersion.dev.toProtoPrimitiveS

    rawVersion match {
      // Since dev uses Int.MaxValue, it does not satisfy the regex above
      case `devFull` | devRegex() => Some(ProtocolVersion.dev)
      case _ => None
    }
  }

  def create(rawVersion: String): Either[String, ProtocolVersion] =
    rawVersion.toIntOption match {
      case Some(value) => Right(ProtocolVersion(value))

      case None =>
        parseSemver(rawVersion)
          .orElse(parseDev(rawVersion).map(Right(_)))
          .getOrElse(Left(s"Unable to convert string `$rawVersion` to a valid protocol version."))
    }

  /** Parse a ProtocolVersion
    * @param rawVersion
    * @return Parsed protocol version
    * @throws java.lang.RuntimeException if the given parameter cannot be parsed to a protocol version
    */
  def tryCreate(rawVersion: String): ProtocolVersion = create(rawVersion).fold(sys.error, identity)

  def fromProtoPrimitiveS(rawVersion: String): ParsingResult[ProtocolVersion] =
    ProtocolVersion.create(rawVersion).leftMap(StringConversionError)

  private def getDevelopmentVersions(
      includeDevelopmentVersions: Boolean
  ): List[ProtocolVersion] =
    if (includeDevelopmentVersions)
      List(ProtocolVersion.dev)
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
        s"Please add the supported protocol versions of a participant of release version $release to `releaseVersionToProtocolVersions` in `ReleaseVersionToProtocolVersions.scala`."
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
        s"Please add the supported protocol versions of domain nodes of release version $release to `releaseVersionToProtocolVersions` in `ReleaseVersionToProtocolVersions.scala`."
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
    if (clientSupportsRequiredVersion && server == ProtocolVersion.dev)
      Right(())
    else if (clientMinVersionLargerThanReqVersion)
      Left(MinProtocolError(server, clientMinimumProtocolVersion, clientSupportsRequiredVersion))
    else if (!clientSupportsRequiredVersion)
      Left(VersionNotSupportedError(server, clientSupportedVersions))
    else Right(())
  }

  /** Check used by domain nodes (mediator / sequencer) to verify whether they are able to join a domain using a specific protocol */
  def isSupportedByDomainNode(
      haveDevVersionSupport: Boolean,
      protocolVersion: ProtocolVersion,
  ): Either[String, Unit] = {
    val supported =
      ProtocolVersion.supportedProtocolsDomain(includeDevelopmentVersions = haveDevVersionSupport)
    ProtocolVersion
      .canClientConnectToServer(supported, protocolVersion, None)
      .leftMap(_.description)
  }

  lazy val dev: ProtocolVersion = ProtocolVersion(Int.MaxValue)

  // Minimum stable protocol version introduced
  lazy val minimum: ProtocolVersion = ProtocolVersion(2)

  lazy val v2: ProtocolVersion = ProtocolVersion(2)
  lazy val v3: ProtocolVersion = ProtocolVersion(3)
  // TODO(i8793): signifies an instance where the protocol version is currently hardcoded but should likely be
  // passed in via propagating the protocol version set in the domain parameters
  lazy val v2Todo_i8793: ProtocolVersion = v2

  /** @return Parsed protocol version if found in environment variable `CANTON_PROTOCOL_VERSION`
    * @throws java.lang.RuntimeException if the given parameter cannot be parsed to a protocol version
    */
  def tryGetOptFromEnv: Option[ProtocolVersion] = sys.env
    .get("CANTON_PROTOCOL_VERSION")
    .map(ProtocolVersion.tryCreate)
}

object Transfer {

  /** When dealing with transfer, allow to be more precise with respect to the domain */
  case class SourceProtocolVersion(v: ProtocolVersion) extends AnyVal

  object SourceProtocolVersion {
    implicit val getResultSourceProtocolVersion: GetResult[SourceProtocolVersion] =
      GetResult[ProtocolVersion].andThen(SourceProtocolVersion(_))

    implicit val setParameterSourceProtocolVersion: SetParameter[SourceProtocolVersion] =
      (pv: SourceProtocolVersion, pp: PositionedParameters) => pp >> pv.v
  }

  case class TargetProtocolVersion(v: ProtocolVersion) extends AnyVal

  object TargetProtocolVersion {
    implicit val getResultTargetProtocolVersion: GetResult[TargetProtocolVersion] =
      GetResult[ProtocolVersion].andThen(TargetProtocolVersion(_))

    implicit val setParameterTargetProtocolVersion: SetParameter[TargetProtocolVersion] =
      (pv: TargetProtocolVersion, pp: PositionedParameters) => pp >> pv.v
  }
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
    s"The version required by the domain (${server.toString}) is lower than the minimum version configured by the participant (${clientMinimumProtocolVersion
      .map(_.toString)
      .getOrElse("")}). " +
      s"${if (clientSupportsRequiredVersion) "The participant supports the version required by the domain and would be able to connect to the domain if the minimum required version is configured to be lower."} "
}

final case class VersionNotSupportedError(
    server: ProtocolVersion,
    clientSupportedVersions: Seq[ProtocolVersion],
) extends HandshakeError {
  override def description: String =
    s"The protocol version required by the server (${server.toString}) is not among the supported protocol versions by the client $clientSupportedVersions. "
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
    ConfigWriter.toString(_.version.toProtoPrimitiveS)
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
    ConfigWriter.toString(_.version.toProtoPrimitiveS)

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
