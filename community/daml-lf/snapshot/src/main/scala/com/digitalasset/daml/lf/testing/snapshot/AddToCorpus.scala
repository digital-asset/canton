// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.testing.snapshot.Snapshot.CorpusManifest
import scopt.{OptionParser, Read}

import java.nio.file.{Path, Paths}
import scala.jdk.CollectionConverters.*

/** Command line for [[Corpus.add]], used by scripts/record-corpus.sh. */
object AddToCorpus {

  final case class Config(
      corpus: Path,
      recordings: Path,
      dars: Seq[Path],
      source: String,
      repo: String,
      commit: String,
      recorder: String,
  )

  private implicit val pathRead: Read[Path] = Read.reads(Paths.get(_))

  private val parser: OptionParser[Config] = new scopt.OptionParser[Config]("add-to-corpus") {
    opt[Path]("corpus")
      .text("Corpus directory to add to; created if it doesn't exist")
      .action((x, c) => c.copy(corpus = x))
      .required()
    opt[Path]("recordings")
      .text("Directory with one subdirectory of daml-script recordings per suite")
      .action((x, c) => c.copy(recordings = x))
      .required()
    opt[Path]("dar")
      .text("DAR holding packages the recordings use; can be repeated")
      .action((x, c) => c.copy(dars = c.dars :+ x))
      .unbounded()
      .required()
    opt[String]("source")
      .text("Name of the source, prefixed to the suite names")
      .action((x, c) => c.copy(source = x))
      .required()
    opt[String]("repo").action((x, c) => c.copy(repo = x))
    opt[String]("commit").action((x, c) => c.copy(commit = x))
    opt[String]("recorder")
      .text("What recorded the transactions, e.g. the daml-script version")
      .action((x, c) => c.copy(recorder = x))
  }

  def main(args: Array[String]): Unit =
    parser.parse(args, Config(null, null, Seq.empty, "", "", "", "")) match {
      case None => sys.exit(1)
      case Some(config) =>
        val source = CorpusManifest.Source
          .newBuilder()
          .setName(config.source)
          .setRepo(config.repo)
          .setCommit(config.commit)
          .setRecorder(config.recorder)
          .build()
        val manifest = Corpus.add(config.corpus, config.recordings, config.dars, source)
        manifest.getSuitesList.asScala.filter(_.getSource == source).foreach { suite =>
          val tests = suite.getTestsList.asScala
          println(s"%%% ${suite.getName}: ${tests.size} tests, ${tests.map(_.getUnits).sum} units")
        }
    }
}
