// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.implicits.*
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast

import java.io.{File, InputStream}
import java.util.zip.ZipInputStream

/** Wrapper that retrieves parsed packages from a DAR file consumable by the Daml interpreter.
  */
object DamlPackageLoader {

  def getPackagesFromDarFile(filename: String): Either[LoadError, Map[Ref.PackageId, Ast.Package]] =
    getPackagesFromDarFiles(Seq(filename))

  def getPackagesFromDarFiles(
      filenames: Seq[String]
  ): Either[LoadError, Map[Ref.PackageId, Ast.Package]] =
    filenames
      .traverse { filename =>
        DarDecoder
          .readArchiveFromFile(new File(filename))
          .bimap(t => LoadError(t.toString), _.all.toMap)
      }
      .map(_.foldLeft(Map.empty[Ref.PackageId, Ast.Package])(_ ++ _))

  def getPackagesFromInputStream(
      name: String,
      inputStream: InputStream,
  ): Either[LoadError, Map[Ref.PackageId, Ast.Package]] =
    DarDecoder
      .readArchive(name, new ZipInputStream(inputStream))
      .bimap(t => LoadError(t.toString), _.all.toMap)

  final case class LoadError(message: String)

}
