// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.testing.snapshot.Snapshot.CorpusManifest

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*
import scala.sys.process.{Process, ProcessLogger}

/** The suites of corpus-tests, built by sbt (parallelDamlBuild) and recorded on the IDE ledger with
  * daml-script, as scripts/record-corpus.sh does for the canton-tests source. Recorded once per
  * test run.
  */
object CantonTestsCorpus {

  // scripts that fail on purpose; the other scripts must succeed
  val expectedFailures: Set[String] = Set("Main:failsHalfway")

  lazy val dir: Path = record()

  def read(): Corpus = Corpus.read(dir)

  private def record(): Path = {
    val dars = Paths.get(sys.props("corpusTestsDars")).resolve("suites")
    val damlVersion = sys.props("damlVersion")
    val work = Files.createTempDirectory("canton-tests")
    // dpm picks the daml-script version from the daml.yaml of the current directory
    Files.writeString(
      work.resolve("daml.yaml"),
      s"override-components:\n  daml-script:\n    version: $damlVersion\n",
    )
    val suiteDars = Files.list(dars).iterator().asScala.filter(_.toString.endsWith(".dar")).toSeq
    suiteDars.foreach { dar =>
      val suite = dar.getFileName.toString.stripSuffix(".dar")
      val output = new StringBuilder
      val status = Process(
        Seq(
          "dpm",
          "script",
          "--dar",
          dar.toString,
          "--all",
          "--ide-ledger",
          "--static-time",
          "--snapshot-dir",
          work.resolve("recordings").resolve(suite).toString,
        ),
        work.toFile,
        "DAML_VERSION" -> damlVersion,
        "DPM_REGISTRY" -> sys.props("dpmRegistry"),
      ).!(ProcessLogger(line => output.append(line).append('\n')))
      val failures =
        output.toString.linesIterator
          .filter(_.contains(" FAILURE"))
          .map(_.takeWhile(_ != ' '))
          .toSeq
      val unexpected = failures.filterNot(expectedFailures)
      // dpm script prints the results once all scripts ran, and exits with 1 if any failed
      val completed = if (failures.isEmpty) status == 0 else status == 1
      if (!completed || unexpected.nonEmpty || !output.toString.contains(" SUCCESS"))
        sys.error(
          s"recording $suite failed (exit status $status): ${unexpected.mkString(", ")}\n$output"
        )
    }
    val corpusDir = work.resolve("corpus")
    Corpus.add(
      corpusDir,
      work.resolve("recordings"),
      suiteDars,
      CorpusManifest.Source.newBuilder().setName("canton-tests").build(),
    )
    corpusDir
  }
}
