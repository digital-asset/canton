// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.StringConversionError
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion.{deprecated, unstable}
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

  def isUnstable: Boolean = unstable.contains(this)
  def isStable: Boolean = !isUnstable

  private def isDev: Boolean = v == Int.MaxValue

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

  final case class InvalidProtocolVersion(override val description: String) extends FailureReason

  // All stable protocol versions supported by this release
  private val supported: NonEmpty[List[ProtocolVersion]] =
    NonEmpty
      .from(BuildInfo.protocolVersions.map(ProtocolVersion.tryCreate).toList)
      .getOrElse(
        sys.error("Release needs to support at least one protocol version")
      )

  private val deprecated: Seq[ProtocolVersion] = Seq(ProtocolVersion.v2)
  val unstable: NonEmpty[List[ProtocolVersion]] =
    NonEmpty.mk(List, ProtocolVersion.v4, ProtocolVersion.dev)

  val latest: ProtocolVersion = supported.max1

  def lastStableVersions2: (ProtocolVersion, ProtocolVersion) = {
    val List(beforeLastStableProtocolVersion, lastStableProtocolVersion) =
      ProtocolVersion.supported.forgetNE.sorted.takeRight(2): @unchecked

    (beforeLastStableProtocolVersion, lastStableProtocolVersion)
  }

  lazy val dev: ProtocolVersion = ProtocolVersion(Int.MaxValue)

  // Minimum stable protocol version introduced
  lazy val minimum: ProtocolVersion = ProtocolVersion(2)

  lazy val v2: ProtocolVersion = ProtocolVersion(2)
  lazy val v3: ProtocolVersion = ProtocolVersion(3)
  lazy val v4: ProtocolVersion = ProtocolVersion(4)

  // TODO(i10354): remove hardcoded protocol version in MerkleTree
  lazy val v2Todo_i10354: ProtocolVersion = v2

  /** @return Parsed protocol version if found in environment variable `CANTON_PROTOCOL_VERSION`
    * @throws java.lang.RuntimeException if the given parameter cannot be parsed to a protocol version
    */
  def tryGetOptFromEnv: Option[ProtocolVersion] = sys.env
    .get("CANTON_PROTOCOL_VERSION")
    .map(ProtocolVersion.tryCreate)
}

/*
 This class wraps a protocol version which is global to the participant.
 The wrapped value usually corresponds to the latest (stable) protocol version supported by the binary.
 */
final case class ReleaseProtocolVersion(v: ProtocolVersion) extends AnyVal

object ReleaseProtocolVersion {
  val latest: ReleaseProtocolVersion = ReleaseProtocolVersion(ProtocolVersion.latest)
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
