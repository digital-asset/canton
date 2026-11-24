// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

import com.daml.crypto.MessageDigestPrototype
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.language.LanguageVersion.{Major, Minor}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf

private[archive] sealed abstract class ArchivePayload {
  def pkgId: PackageId
  def version: LanguageVersion
}

private[archive] object ArchivePayload {

  final case class Lf1(
      pkgId: PackageId,
      proto: DamlLf1.Package,
      minor: Minor,
  ) extends ArchivePayload {
    val version = LanguageVersion(Major.V1, minor)
  }

  final case class Lf2(
      pkgId: PackageId,
      proto: DamlLf2.Package,
      minor: Minor,
      patch: Int,
  ) extends ArchivePayload {
    val version = LanguageVersion(Major.V2, minor)
  }
}

private[archive] object Reader {

  private def validateUnknownFields(m: protobuf.Message, schemaMode: Boolean): Either[Error, Unit] =
    if (schemaMode)
      Right(())
    else
      com.digitalasset.daml.SafeProto.ensureNoUnknownFields(m).left.map(Error.Parsing.apply)

  /* Checks the Archive wrapper without parsing its payload: the hash function is supported,
   * the hash is well formed and matches the SHA-256 of the payload bytes, and, if `schemaMode`
   * is `false`, the wrapper has no unknown fields.
   *
   * Returns the verified hash, which uniquely identifies the payload bytes.
   */
  def validateArchive(
      lf: DamlLf.Archive,
      schemaMode: Boolean,
  ): Either[Error, PackageId] = for {
    theirHash <- lf.getHashFunction match {
      case DamlLf.HashFunction.SHA256 =>
        PackageId
          .fromString(lf.getHash)
          .left
          .map(err => Error.Parsing("Invalid hash: " + err))
      case DamlLf.HashFunction.UNRECOGNIZED =>
        Left(Error.Parsing("Unrecognized hash function"))
    }
    _ <- validateUnknownFields(lf, schemaMode)
    ourHash = MessageDigestPrototype.Sha256.newDigest
      .digest(lf.getPayload.toByteArray)
      .map("%02x" format _)
      .mkString
    _ <- Either.cond(
      theirHash == ourHash,
      (),
      Error.Parsing(s"Mismatching hashes! Expected $ourHash but got $theirHash"),
    )
  } yield theirHash

  /* Parses the payload of an Archive whose hash has already been validated by `validateArchive`
   * and converts it into a scala ArchivePayload.
   *
   * Checks that the version is supported and, if `schemaMode` is `false`, ensures the payload
   * has no unknown fields.
   */
  def readValidatedArchive(
      hash: PackageId,
      lf: DamlLf.Archive,
      schemaMode: Boolean,
  ): Either[Error, ArchivePayload] = for {
    proto <- ArchivePayloadParser.fromByteString(lf.getPayload)
    payload <- readArchivePayload(hash, proto, schemaMode)
  } yield payload

  /* Convert an Archive proto message into a scala ArchivePayload.
   *
   * Checks that the hash of the payload matches the hash field, that the version is
   * supported, and if `schemaMode` is `false`, ensures the message has no unknown fields.
   */
  @VisibleForTesting
  def readArchive(
      lf: DamlLf.Archive,
      schemaMode: Boolean,
  ): Either[Error, ArchivePayload] =
    validateArchive(lf, schemaMode).flatMap(readValidatedArchive(_, lf, schemaMode))

  /* Converts a DamlLf.ArchivePayload Protocol Buffer message to a Scala ArchivePayload.
   *
   * Checks that the payload's version is supported and, if `schemaMode` is `false`,
   * ensures the message has no unknown fields.
   */
  private[archive] def readArchivePayload(
      hash: PackageId,
      lf: DamlLf.ArchivePayload,
      schemaMode: Boolean,
  ): Either[Error, ArchivePayload] = lf.getSumCase match {
    case DamlLf.ArchivePayload.SumCase.DAML_LF_1 =>
      if (lf.getPatch != 0)
        Left(Error.Parsing("Patch version is not supported for LF1"))
      else {
        val minor = Minor.assertParsingSuccessful(Minor.fromString(lf.getMinor))
        for {
          _ <- validateUnknownFields(lf, schemaMode)
          pkg <- lf1PackageParser.fromByteString(lf.getDamlLf1)
        } yield ArchivePayload.Lf1(hash, pkg, minor)
      }
    case DamlLf.ArchivePayload.SumCase.DAML_LF_2 =>
      val minor = Minor.assertParsingSuccessful(Minor.fromString(lf.getMinor))
      for {
        _ <- validateUnknownFields(lf, schemaMode)
        pkg <- lf2PackageParser(minor).fromByteString(lf.getDamlLf2)
        _ <- validateUnknownFields(pkg, schemaMode)
      } yield ArchivePayload.Lf2(
        hash,
        pkg,
        Minor.assertParsingSuccessful(Minor.fromString(lf.getMinor)),
        lf.getPatch,
      )
    case DamlLf.ArchivePayload.SumCase.SUM_NOT_SET =>
      Left(Error.Parsing("Unrecognized or Unsupported LF version"))
  }
}
