// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, Util as AstUtil}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

object Decode {

  /*
   * Decodes an ArchivePayload into a complete AST.
   * Returns an error if the payload is invalid or contains an unknown LF version.
   */
  @VisibleForTesting
  private[archive] def decodeArchivePayload(
      payload: ArchivePayload
  ): Either[Error, (PackageId, Ast.Package)] =
    decodeArchivePayload(payload, schemaMode = false)

  @VisibleForTesting
  @throws[Error]
  private[archive] def assertDecodeArchivePayload(
      payload: ArchivePayload
  ): (PackageId, Ast.Package) =
    assertRight(decodeArchivePayload(payload))

  /*
   * Decodes the serialized bytes of an archive payload (the content of the `payload` field of
   * an archive) into a schema. The package id is trusted, it is not checked against the payload.
   */
  def decodeArchivePayloadBytesSchema(
      packageId: PackageId,
      payload: ByteString,
  ): Either[Error, (PackageId, Ast.PackageSignature)] =
    for {
      proto <- ArchivePayloadParser.fromByteString(payload)
      archivePayload <- Reader.readArchivePayload(packageId, proto, schemaMode = true)
      result <- decodeArchivePayloadSchema(archivePayload)
    } yield result

  @VisibleForTesting
  @throws[Error]
  private[archive] def assertDecodeArchivePayloadSchema(
      payload: ArchivePayload
  ): (PackageId, Ast.PackageSignature) =
    assertRight(decodeArchivePayloadSchema(payload))

  private[archive] def decodeArchivePayloadSchema(
      payload: ArchivePayload
  ): Either[Error, (PackageId, Ast.PackageSignature)] =
    decodeArchivePayload(payload, schemaMode = true).map { case (id, pkg) =>
      id -> AstUtil.toSignature(pkg)
    }
  private[this] def decodeArchivePayload(
      payload: ArchivePayload,
      schemaMode: Boolean,
  ): Either[Error, (PackageId, Ast.Package)] =
    payload match {
      case ArchivePayload.Lf2(pkgId, protoPkg, minor, patch)
          if LanguageVersion.allLfVersions.map(_.minor).contains(minor) =>
        new DecodeV2(minor)
          .decodePackage(
            pkgId,
            protoPkg,
            schemaMode,
            patch,
          )
          .map(payload.pkgId -> _)
      case ArchivePayload.Lf1(pkgId, protoPkg, minor)
          if LanguageVersion.allLegacyLfVersions.map(_.minor).contains(minor) =>
        new DecodeV1(minor)
          .decodePackage(
            pkgId,
            protoPkg,
            schemaMode,
          )
          .map(payload.pkgId -> _)
      case _ =>
        Left(
          Error.Parsing(s"Encountered unsupported LF version ${payload.version} during decoding")
        )
    }

  private type DecodeInput = (PackageId, DamlLf.Archive, Boolean)
  private type DecodeOutput = Either[Error, (PackageId, Ast.Package)]
  @VisibleForTesting
  private[archive] val uncachedDecodeValidatedArchive: DecodeInput => DecodeOutput = {
    case (hash, archive, schemaMode) =>
      Reader
        .readValidatedArchive(hash, archive, schemaMode)
        .flatMap(decodeArchivePayload(_, schemaMode))
  }
  // Memoizes `underlying` per (verified archive hash, schemaMode). The verified hash uniquely
  // identifies the payload bytes. Entries are never evicted. Errors are not cached.
  @VisibleForTesting
  private[archive] def cachedDecodeValidatedArchive(
      underlying: DecodeInput => DecodeOutput
  ): DecodeInput => DecodeOutput = {
    val cache =
      new java.util.concurrent.ConcurrentHashMap[(PackageId, Boolean), (PackageId, Ast.Package)]()
    (input: DecodeInput) =>
      val (hash, _, schemaMode) = input
      val key = (hash, schemaMode)
      Option(cache.get(key)) match {
        case Some(value) => Right(value)
        case None =>
          underlying(input).map(value => Option(cache.putIfAbsent(key, value)).getOrElse(value))
      }
  }

  // Caching is disabled by default and enabled only when the JVM is started with
  //  `-Dcom.digitalasset.daml.lf.archive.Decode.cacheDecodeValidatedArchive=true`.
  private[this] val decodeValidatedArchive: DecodeInput => DecodeOutput =
    if (
      java.lang.Boolean.getBoolean(
        "com.digitalasset.daml.lf.archive.Decode.cacheDecodeValidatedArchive"
      )
    ) {
      LoggerFactory.getLogger(getClass).warn("Caching of decoded validated archives is enabled.")
      cachedDecodeValidatedArchive(uncachedDecodeValidatedArchive)
    } else
      uncachedDecodeValidatedArchive
  def decodeArchive(
      archive: DamlLf.Archive
  ): Either[Error, (PackageId, Ast.Package)] =
    Reader
      .validateArchive(archive, schemaMode = false)
      .flatMap(hash => decodeValidatedArchive((hash, archive, false)))

  @throws[Error]
  def assertDecodeArchive(
      archive: DamlLf.Archive
  ): (PackageId, Ast.Package) =
    assertRight(decodeArchive(archive))

  def decodeArchiveSchema(
      archive: DamlLf.Archive
  ): Either[Error, (PackageId, Ast.PackageSignature)] =
    Reader
      .validateArchive(archive, schemaMode = true)
      .flatMap(hash =>
        decodeValidatedArchive((hash, archive, true)).map { case (pkgId, pkg) =>
          pkgId -> AstUtil.toSignature(pkg)
        }
      )

  @throws[Error]
  def assertDecodeArchiveSchema(
      archive: DamlLf.Archive
  ): (PackageId, Ast.PackageSignature) =
    assertRight(decodeArchiveSchema(archive))

}
