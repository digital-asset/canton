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

class CorpusReplayTest extends AnyWordSpec with Matchers with SuppressingLogging {

  implicit val logContext: LoggingContext = LoggingContext.ForTesting

  private val alice = Ref.Party.assertFromString("Alice")
  private val darFile =
    Path.of(getClass.getClassLoader.getResource("ReplayBenchmark.dar").getPath)
  private val pkgId = DarDecoder.assertReadArchiveFromFile(darFile.toFile).main._1

  private def corpus(contractIdVersion: ContractIdVersion): Corpus = {
    val recordings = Files.createTempDirectory("CorpusReplayTest")
    val dir = recordings.resolve("suite/pkg/Mod/script")
    Files.createDirectories(dir)
    val engine = TransactionSnapshot.compile(
      TransactionSnapshot.loadDar(darFile),
      snapshotDir = Some(dir),
      loggerFactory = loggerFactory,
    )
    val templateId = Ref.Identifier.assertFromString(s"$pkgId:ReplayBenchmark:T")
    (1 to 3).foreach { i =>
      val cmd = ApiCommand.CreateAndExercise(
        templateId.toRef,
        ValueRecord(
          Some(templateId),
          ImmArray(None -> ValueParty(alice), None -> ValueInt64(i.toLong)),
        ),
        Ref.ChoiceName.assertFromString("Add"),
        ValueRecord(None, ImmArray(None -> ValueInt64(3))),
      )
      engine
        .submit(
          submitters = Set(alice),
          cmds = ApiCommands(ImmArray(cmd), Time.Timestamp.now(), "corpus-replay-test"),
          participantId = Ref.ParticipantId.assertFromString("ide-ledger"),
          submissionSeed = crypto.Hash.hashPrivateKey(s"corpus replay test $i"),
          contractIdVersion = contractIdVersion,
          prefetchKeys = Seq.empty,
          interpretationConfig = InterpretationConfig.DefaultForTesting,
        )
        .consume(Result.lookupHandler()) shouldBe a[Right[?, ?]]
    }
    val corpusDir = Files.createTempDirectory("CorpusReplayTest")
    Corpus.add(
      corpusDir,
      recordings,
      Seq(darFile),
      CorpusManifest.Source.newBuilder().setName("src").build(),
    )
    Corpus.read(corpusDir)
  }

  "CorpusReplay" should {
    Seq(ContractIdVersion.V1, ContractIdVersion.V2).foreach { contractIdVersion =>
      s"replay and verify every unit, from the unit alone ($contractIdVersion)" in {
        val c = corpus(contractIdVersion)
        val replay = new CorpusReplay(c, loggerFactory)
        val units = c.units.toVector
        units should have size 3
        units.foreach(unit => replay.verify(replay.newEngine(), unit) shouldBe Right(()))
      }
    }

    "fail on a unit replayed with another transaction seed" in {
      val c = corpus(ContractIdVersion.V2)
      val replay = new CorpusReplay(c, loggerFactory)
      val unit = c.units.next()
      val tampered = unit.copy(entry =
        unit.entry.toBuilder
          .setTransactionSeed(crypto.Hash.hashPrivateKey("another seed").bytes.toByteString)
          .build()
      )
      replay.verify(replay.newEngine(), tampered) shouldBe a[Left[?, ?]]
    }
  }

  "CorpusBenchmark" should {
    "replay the selected units" in {
      val c = corpus(ContractIdVersion.V2)
      val benchmark = new CorpusBenchmark
      benchmark.corpus = c.dir.toString
      benchmark.select = "src/"
      benchmark.init()
      val counters = new CorpusBenchmark.EventCounter
      benchmark.bench(counters)
      // each unit creates T, exercises Add and creates its result
      counters.transactionNodeCount shouldBe 9L
    }

    "fail if no unit is selected" in {
      val benchmark = new CorpusBenchmark
      benchmark.corpus = corpus(ContractIdVersion.V2).dir.toString
      benchmark.select = "other/"
      an[IllegalArgumentException] should be thrownBy benchmark.init()
    }
  }
}
