// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.archive.{ArchiveDecoder, DarParser}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.testing.snapshot.Snapshot.{CorpusManifest, SubmissionEntry}
import com.google.protobuf.ByteString
import com.google.protobuf.util.JsonFormat

import java.io.{BufferedInputStream, BufferedOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** A transaction recorded by a test, with everything needed to replay it except the packages, which
  * the corpus holds once for all units.
  */
final case class CorpusUnit(
    suite: String,
    test: String,
    index: Int,
    entry: Snapshot.TransactionEntry,
)

/** A directory of self-contained transaction recordings: manifest.json, packages/<pkgId>.dalf and
  * suites/<suite name>.bin. See `CorpusManifest` in snapshot.proto.
  */
final class Corpus private (val dir: Path, val manifest: CorpusManifest) {

  def suites: Seq[CorpusManifest.Suite] = manifest.getSuitesList.asScala.toSeq

  def suite(name: String): CorpusManifest.Suite =
    suites.find(_.getName == name).getOrElse(sys.error(s"no suite $name in $dir"))

  def archive(pkgId: Ref.PackageId): ByteString =
    Using.resource(Files.newInputStream(Corpus.packageFile(dir, pkgId)))(ByteString.readFrom)

  def packages(pkgIds: Iterable[Ref.PackageId]): Map[Ref.PackageId, Ast.Package] =
    pkgIds.iterator.map(id => ArchiveDecoder.assertFromByteString(archive(id))).toMap

  /** The units of a suite, in recording order. */
  def units(suiteName: String): Vector[CorpusUnit] = {
    val tests = suite(suiteName).getTestsList.asScala.toVector
    val entries = Corpus.readEntries(Corpus.suiteFile(dir, suiteName))
    val expected = tests.map(_.getUnits).sum
    if (entries.sizeIs != expected)
      sys.error(s"suite $suiteName has ${entries.size} entries, manifest says $expected")
    tests
      .flatMap(test => Vector.tabulate(test.getUnits)(index => test.getName -> index))
      .zip(entries)
      .map { case ((test, index), entry) => CorpusUnit(suiteName, test, index, entry) }
  }

  def units: Iterator[CorpusUnit] = suites.iterator.flatMap(suite => units(suite.getName))
}

object Corpus {

  val Version = 1

  def read(dir: Path): Corpus = {
    val manifest = readManifest(dir).getOrElse(sys.error(s"no corpus manifest in $dir"))
    checkVersion(dir, manifest)
    new Corpus(dir, manifest)
  }

  private def checkVersion(dir: Path, manifest: CorpusManifest): Unit =
    if (manifest.getVersion != Version)
      sys.error(s"corpus $dir has version ${manifest.getVersion}, expected $Version")

  private[snapshot] def manifestFile(dir: Path): Path = dir.resolve("manifest.json")

  private[snapshot] def packageFile(dir: Path, pkgId: String): Path =
    dir.resolve("packages").resolve(s"$pkgId.dalf")

  private[snapshot] def suiteFile(dir: Path, suite: String): Path =
    dir.resolve("suites").resolve(s"$suite.bin")

  private[snapshot] def readManifest(dir: Path): Option[CorpusManifest] =
    Option.when(Files.exists(manifestFile(dir))) {
      val builder = CorpusManifest.newBuilder()
      JsonFormat
        .parser()
        .merge(Files.readString(manifestFile(dir), StandardCharsets.UTF_8), builder)
      builder.build()
    }

  private[snapshot] def writeManifest(dir: Path, manifest: CorpusManifest): Unit =
    Files.writeString(
      manifestFile(dir),
      JsonFormat.printer().print(manifest) + "\n",
      StandardCharsets.UTF_8,
    ): Unit

  private[snapshot] def readEntries(file: Path): Vector[Snapshot.TransactionEntry] =
    Using.resource(new BufferedInputStream(Files.newInputStream(file))) { in =>
      Iterator
        .continually(SubmissionEntry.parseDelimitedFrom(in))
        .takeWhile(_ != null)
        .map { entry =>
          if (!entry.hasTransaction) sys.error(s"$file: expected only transaction entries")
          entry.getTransaction
        }
        .toVector
    }

  /** Adds the recordings of one source to a corpus directory, creating it if needed.
    *
    * Each subdirectory of `recordings` becomes a suite named `<source>/<subdirectory>`, and each
    * recording file in it (as daml-script writes them, under `<package>/<module>/<script>`) a test.
    * The packages the transactions use are taken from `dars`.
    */
  def add(
      corpusDir: Path,
      recordings: Path,
      dars: Seq[Path],
      source: CorpusManifest.Source,
  ): CorpusManifest = {
    // the source name becomes a directory under suites/, so it must not escape the corpus
    if (!source.getName.matches("[A-Za-z0-9_-][A-Za-z0-9._-]*"))
      sys.error(s"invalid source name '${source.getName}'")
    val manifest = readManifest(corpusDir)
      .getOrElse(CorpusManifest.newBuilder().setVersion(Version).build())
    checkVersion(corpusDir, manifest)
    val existing = manifest.getSuitesList.asScala.map(_.getName).toSet
    val archives = dars.iterator
      .flatMap(dar => DarParser.assertReadArchiveFromFile(dar.toFile).all)
      .map(archive => archive.getHash -> archive.toByteString)
      .toMap

    val suites = listDirs(recordings).map { suiteDir =>
      val name = s"${source.getName}/${suiteDir.getFileName}"
      if (existing(name)) sys.error(s"corpus $corpusDir already has suite $name")
      val tests = listRecordings(suiteDir).map { recording =>
        testName(suiteDir, recording) -> readEntries(recording)
      }
      tests.map(_._1).diff(tests.map(_._1).distinct).headOption.foreach { test =>
        sys.error(s"suite $name has several recordings for test $test")
      }
      val file = suiteFile(corpusDir, name)
      Files.createDirectories(file.getParent)
      Using.resource(new BufferedOutputStream(Files.newOutputStream(file))) { out =>
        tests.foreach { case (_, entries) =>
          entries.foreach(
            SubmissionEntry.newBuilder().setTransaction(_).build().writeDelimitedTo(out)
          )
        }
      }
      tests.iterator
        .flatMap(_._2)
        .flatMap(_.getUsedPackagesList.asScala)
        .foreach { pkgId =>
          val file = packageFile(corpusDir, pkgId)
          if (!Files.exists(file)) {
            val archive = archives.getOrElse(
              pkgId,
              sys.error(s"suite $name uses package $pkgId, which is in none of the given DARs"),
            )
            Files.createDirectories(file.getParent)
            Files.write(file, archive.toByteArray)
          }
        }
      CorpusManifest.Suite
        .newBuilder()
        .setName(name)
        .setSource(source)
        .addAllTests(tests.map { case (test, entries) =>
          CorpusManifest.Test.newBuilder().setName(test).setUnits(entries.size).build()
        }.asJava)
        .build()
    }

    val updated = manifest.toBuilder.addAllSuites(suites.asJava).build()
    writeManifest(corpusDir, updated)
    updated
  }

  private def listDirs(dir: Path): Seq[Path] =
    Using.resource(Files.list(dir))(_.iterator().asScala.filter(Files.isDirectory(_)).toSeq.sorted)

  private def listRecordings(dir: Path): Seq[Path] =
    Using.resource(Files.walk(dir))(
      _.iterator().asScala.filter(_.getFileName.toString.endsWith(".bin")).toSeq.sorted
    )

  // e.g. pkg/Module.Name/script/snapshot-ide-ledger.bin => pkg:Module.Name:script
  private def testName(suiteDir: Path, recording: Path): String =
    Option(suiteDir.relativize(recording).getParent)
      .fold(recording.getFileName.toString.stripSuffix(".bin"))(_.iterator().asScala.mkString(":"))
}
