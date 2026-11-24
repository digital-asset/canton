// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.io.File
import java.util.zip.ZipInputStream

/** Test-only readers producing [[ArchivePayload]]s, which are private to the archive package. */
private[archive] object TestReaders {

  def readArchivePayload(
      archive: DamlLf.Archive,
      schemaMode: Boolean = false,
  ): Either[Error, ArchivePayload] =
    Reader.readArchive(archive, schemaMode)

  def readArchivePayloadFromFile(
      file: java.nio.file.Path
  ): Either[Error, ArchivePayload] =
    ArchiveParser.fromFile(file).flatMap(readArchivePayload(_))

  def readArchivePayloadFromBytes(
      bytes: com.google.protobuf.ByteString,
      schemaMode: Boolean,
  ): Either[Error, ArchivePayload] =
    ArchiveParser.fromByteString(bytes).flatMap(readArchivePayload(_, schemaMode))

  object DarReader {

    private def toPayloads(dar: Dar[DamlLf.Archive]): Either[Error, Dar[ArchivePayload]] =
      for {
        main <- readArchivePayload(dar.main)
        deps <- dar.dependencies.foldRight[Either[Error, List[ArchivePayload]]](Right(Nil)) {
          (archive, acc) =>
            for {
              rest <- acc
              payload <- readArchivePayload(archive)
            } yield payload :: rest
        }
      } yield Dar(main, deps)

    def readArchive(
        name: String,
        darStream: ZipInputStream,
        entrySizeThreshold: Int = GenDarReader.EntrySizeThreshold,
    ): Either[Error, Dar[ArchivePayload]] =
      DarParser.readArchive(name, darStream, entrySizeThreshold).flatMap(toPayloads)

    @throws[Error]
    def assertReadArchiveFromFile(darFile: File): Dar[ArchivePayload] =
      DarParser
        .readArchiveFromFile(darFile)
        .flatMap(toPayloads)
        .fold(throw _, identity)
  }
}
