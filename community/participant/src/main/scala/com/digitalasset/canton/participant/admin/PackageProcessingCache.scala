// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.digitalasset.canton.LfPackageId
import com.digitalasset.daml.lf.archive.{DamlLf, Decode, Error as LfArchiveError}
import com.digitalasset.daml.lf.language.Ast
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.google.protobuf.ByteString

/** JVM-wide memo of the CPU-heavy, deterministic parts of processing an uploaded DAR: decoding each
  * Daml-LF archive and typechecking each package.
  *
  * The same packages are uploaded over and over: every participant uploads the admin-workflows DAR
  * when it starts, and every DAR carries the same standard-library packages. Without the memo, each
  * upload decodes and typechecks all of them again.
  *
  *   - Decoding is a pure function of the archive. Entries are keyed by the archive's hash
  *     function, hash and payload bytes, so a hit is the result of decoding an identical archive.
  *     Only successful decodes are stored.
  *   - [[com.digitalasset.daml.lf.archive.Reader]] checks that the package id is the SHA-256 of the
  *     payload, so a package id obtained from [[decodeArchive]] identifies the package's content,
  *     and the package names its dependencies by such ids. Typechecking a package is a pure
  *     function of the package and its dependencies, so once a package with a given id passed the
  *     typecheck, it passes it again. Only packages that went through [[decodeArchive]] and a
  *     successful [[com.digitalasset.daml.lf.engine.Engine.validateDar]] are recorded, and failures
  *     are never recorded, so invalid packages are still rejected every time. This is the same
  *     trust the engine already gives to packages it has loaded.
  *
  * Decoded packages are held through soft references and both caches are bounded, so the memo
  * cannot keep memory from the rest of the node.
  */
private[admin] object PackageProcessingCache {

  private final case class ArchiveKey(
      hashFunction: DamlLf.HashFunction,
      hash: String,
      payload: ByteString,
  )

  private val MaxDecodedArchives: Long = 4096L
  private val MaxTypecheckedPackageIds: Long = 65536L

  private val decoded: Cache[ArchiveKey, (LfPackageId, Ast.Package)] =
    Caffeine
      .newBuilder()
      .maximumSize(MaxDecodedArchives)
      .softValues()
      .build[ArchiveKey, (LfPackageId, Ast.Package)]()

  private val typechecked: Cache[LfPackageId, java.lang.Boolean] =
    Caffeine
      .newBuilder()
      .maximumSize(MaxTypecheckedPackageIds)
      .build[LfPackageId, java.lang.Boolean]()

  /** Same result as [[com.digitalasset.daml.lf.archive.Decode.decodeArchive]]. */
  def decodeArchive(archive: DamlLf.Archive): Either[LfArchiveError, (LfPackageId, Ast.Package)] = {
    val key = ArchiveKey(archive.getHashFunction, archive.getHash, archive.getPayload)
    Option(decoded.getIfPresent(key)) match {
      case Some(hit) => Right(hit)
      case None =>
        Decode.decodeArchive(archive).map { result =>
          decoded.put(key, result)
          result
        }
    }
  }

  /** Whether a package with this id (obtained from [[decodeArchive]]) already passed the typecheck
    * of [[com.digitalasset.daml.lf.engine.Engine.validateDar]] in this JVM.
    */
  def isTypechecked(packageId: LfPackageId): Boolean =
    Option(typechecked.getIfPresent(packageId)).isDefined

  /** Record that all `packageIds` passed the typecheck. Only call this after
    * [[com.digitalasset.daml.lf.engine.Engine.validateDar]] itself returned `Right` for a DAR whose
    * packages all came from [[decodeArchive]].
    */
  def markTypechecked(packageIds: Iterable[LfPackageId]): Unit =
    packageIds.foreach(typechecked.put(_, java.lang.Boolean.TRUE))
}
