// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.data.Ref
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.nio.file.{FileSystems, Files, Path}

class ExtractSnapshotChoices extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private lazy val minStepCount =
    sys.env.get("MIN_STEP_COUNT").fold[Option[Long]](None)(n => Some(n.toLong))
  private lazy val minTxNodeCount =
    sys.env.get("MIN_TX_NODE_COUNT").fold[Option[Long]](None)(n => Some(n.toLong))
  private lazy val minFetchNodeCount =
    sys.env.get("MIN_FETCH_NODE_COUNT").fold[Option[Long]](None)(n => Some(n.toLong))
  private var snapshotDir: Path = _

  override protected def beforeAll(): Unit = {
    assume(
      sys.env.contains("SNAPSHOT_DIR"),
      "The environment variable SNAPSHOT_DIR needs to be set",
    )

    snapshotDir = Path.of(sys.env("SNAPSHOT_DIR"))
  }

  lazy val participantId = Ref.ParticipantId.assertFromString("participant1")
  lazy val snapshotFileMatcher =
    FileSystems
      .getDefault()
      .getPathMatcher(s"glob:$snapshotDir/snapshot-$participantId*.bin")

  private def runWhenEnvVarSet(name: String)(testFun: => Assertion): Unit =
    if (sys.env.contains("STANDALONE")) {
      println(name)
      name.in(testFun)
    } else {
      name.ignore(testFun)
    }

  private val filteringLabel = {
    val filteringLabels = (
      minStepCount.map(stepCount => s"step count >= $stepCount")
        ++ minTxNodeCount.map(txNodeCount => s"tx node count >= $txNodeCount")
        ++ minFetchNodeCount.map(fetchNodeCount => s"fetch node count >= $fetchNodeCount")
    ).mkString(" and ")

    if (filteringLabels.isEmpty) {
      ""
    } else {
      s" - filtering with $filteringLabels"
    }
  }

  runWhenEnvVarSet(s"Extract all top level choices from snapshot data $filteringLabel") {
    val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
    snapshotFiles.size() should be(1)

    val snapshotFile = snapshotFiles.get(0)
    val choiceNames =
      TransactionSnapshot.getAllTopLevelChoiceNames(
        snapshotFile,
        minStepCount,
        minTxNodeCount,
        minFetchNodeCount,
        debug = true,
      )

    noException should be thrownBy {
      Files.writeString(
        snapshotDir.resolve(s"${snapshotFile.getFileName}.choices"),
        choiceNames.mkString("\n"),
      )
    }
  }
}
