// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.daml.logging.LoggingContext
import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.Result
import com.digitalasset.daml.lf.interpretation.InterpretationConfig
import com.digitalasset.daml.lf.testing.snapshot.Snapshot.CorpusManifest
import com.digitalasset.daml.lf.value.ContractIdVersion
import com.digitalasset.daml.lf.value.Value.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

class CorpusTest extends AnyWordSpec with Matchers with SuppressingLogging {

  implicit val logContext: LoggingContext = LoggingContext.ForTesting

  private val alice = Ref.Party.assertFromString("Alice")
  private val darFile =
    Path.of(getClass.getClassLoader.getResource("ReplayBenchmark.dar").getPath)
  private val pkgId = DarDecoder.assertReadArchiveFromFile(darFile.toFile).main._1

  // records `n` transactions into dir, as daml-script records one script
  private def record(dir: Path, n: Int): Unit = {
    Files.createDirectories(dir)
    val recordingEngine = TransactionSnapshot.compile(
      TransactionSnapshot.loadDar(darFile),
      snapshotDir = Some(dir),
      loggerFactory = loggerFactory,
    )
    val templateId = Ref.Identifier.assertFromString(s"$pkgId:ReplayBenchmark:T")
    (1 to n).foreach { i =>
      val cmd = ApiCommand.CreateAndExercise(
        templateId.toRef,
        ValueRecord(
          Some(templateId),
          ImmArray(None -> ValueParty(alice), None -> ValueInt64(i.toLong)),
        ),
        Ref.ChoiceName.assertFromString("Add"),
        ValueRecord(None, ImmArray(None -> ValueInt64(3))),
      )
      recordingEngine
        .submit(
          submitters = Set(alice),
          cmds = ApiCommands(ImmArray(cmd), Time.Timestamp.now(), "corpus-test"),
          participantId = Ref.ParticipantId.assertFromString("ide-ledger"),
          submissionSeed = crypto.Hash.hashPrivateKey(s"corpus test $dir $i"),
          contractIdVersion = ContractIdVersion.V2,
          prefetchKeys = Seq.empty,
          interpretationConfig = InterpretationConfig.DefaultForTesting,
        )
        .consume(Result.lookupHandler()) shouldBe a[Right[?, ?]]
    }
  }

  private def source(name: String) =
    CorpusManifest.Source.newBuilder().setName(name).setCommit("abc").build()

  private def recordings(): Path = {
    val dir = Files.createTempDirectory("CorpusTest")
    record(dir.resolve("suite-a/pkg/Mod/one"), 2)
    record(dir.resolve("suite-a/pkg/Mod/two"), 3)
    record(dir.resolve("suite-b/pkg/Other/three"), 1)
    dir
  }

  "Corpus.add" should {
    "group the recordings into suites and tests, with the packages they use" in {
      val corpusDir = Files.createTempDirectory("CorpusTest")
      val recordingsDir = recordings()
      Corpus.add(corpusDir, recordingsDir, Seq(darFile), source("src"))

      val corpus = Corpus.read(corpusDir)
      corpus.suites.map(_.getName) shouldBe Seq("src/suite-a", "src/suite-b")
      corpus.suite("src/suite-a").getTestsList.asScala.map(t => t.getName -> t.getUnits) shouldBe
        Seq("pkg:Mod:one" -> 2, "pkg:Mod:two" -> 3)
      corpus.suite("src/suite-b").getSource shouldBe source("src")

      val units = corpus.units.toVector
      units.map(u => (u.suite, u.test, u.index)) shouldBe Vector(
        ("src/suite-a", "pkg:Mod:one", 0),
        ("src/suite-a", "pkg:Mod:one", 1),
        ("src/suite-a", "pkg:Mod:two", 0),
        ("src/suite-a", "pkg:Mod:two", 1),
        ("src/suite-a", "pkg:Mod:two", 2),
        ("src/suite-b", "pkg:Other:three", 0),
      )
      units.map(_.entry) shouldBe
        Seq("suite-a/pkg/Mod/one", "suite-a/pkg/Mod/two", "suite-b/pkg/Other/three")
          .flatMap(dir =>
            Corpus.readEntries(recordingsDir.resolve(dir).resolve("snapshot-ide-ledger.bin"))
          )

      val used = units.flatMap(_.entry.getUsedPackagesList.asScala).toSet
      used should contain(pkgId)
      Files
        .list(corpusDir.resolve("packages"))
        .iterator()
        .asScala
        .map(_.getFileName.toString)
        .toSet shouldBe
        used.map(id => s"$id.dalf")
      corpus.packages(used.map(Ref.PackageId.assertFromString)).keySet shouldBe used
    }

    "add the suites of another source to an existing corpus" in {
      val corpusDir = Files.createTempDirectory("CorpusTest")
      val recordingsDir = recordings()
      Corpus.add(corpusDir, recordingsDir, Seq(darFile), source("src"))
      Corpus.add(corpusDir, recordingsDir, Seq(darFile), source("other"))

      Corpus.read(corpusDir).suites.map(_.getName) shouldBe
        Seq("src/suite-a", "src/suite-b", "other/suite-a", "other/suite-b")
      Corpus.read(corpusDir).units.size shouldBe 12
    }

    "refuse to add a suite twice" in {
      val corpusDir = Files.createTempDirectory("CorpusTest")
      val recordingsDir = recordings()
      Corpus.add(corpusDir, recordingsDir, Seq(darFile), source("src"))
      the[RuntimeException] thrownBy
        Corpus.add(corpusDir, recordingsDir, Seq(darFile), source("src")) should have message
        s"corpus $corpusDir already has suite src/suite-a"
    }

    "refuse to add to a corpus of another version" in {
      val corpusDir = Files.createTempDirectory("CorpusTest")
      val recordingsDir = recordings()
      Corpus.writeManifest(corpusDir, CorpusManifest.newBuilder().setVersion(2).build())
      the[RuntimeException] thrownBy
        Corpus.add(corpusDir, recordingsDir, Seq(darFile), source("src")) should have message
        s"corpus $corpusDir has version 2, expected 1"
      Files.exists(corpusDir.resolve("suites")) shouldBe false
    }

    "fail if a used package is in none of the DARs" in {
      val error = the[RuntimeException] thrownBy
        Corpus.add(Files.createTempDirectory("CorpusTest"), recordings(), Seq.empty, source("src"))
      error.getMessage should include("which is in none of the given DARs")
    }

    "refuse a source name that is not a plain directory name" in {
      Seq("", "..", "a/b", "/tmp").foreach { name =>
        the[RuntimeException] thrownBy
          Corpus.add(
            Files.createTempDirectory("CorpusTest"),
            recordings(),
            Seq(darFile),
            source(name),
          ) should have message
          s"invalid source name '$name'"
      }
    }
  }
}
