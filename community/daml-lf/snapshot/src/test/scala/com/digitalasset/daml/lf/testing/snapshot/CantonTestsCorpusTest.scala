// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.TransactionCoder
import com.digitalasset.daml.lf.transaction.Node
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.*

/** Checks the canton-tests corpus records what its scripts are there to cover. */
class CantonTestsCorpusTest extends AnyWordSpec with Matchers {

  private lazy val corpus = CantonTestsCorpus.read()

  private def units(suite: String) = corpus.units(s"canton-tests/$suite")

  private def nodes(unit: CorpusUnit): Seq[Node] =
    TransactionCoder
      .decodeTransaction(Snapshot.Transaction.parseFrom(unit.entry.getRawTransaction))
      .fold(err => fail(err.errorMessage), _.nodes.values.toSeq)

  "The canton-tests corpus" should {
    "have a suite per package under suites/" in {
      corpus.suites.map(_.getName) shouldBe
        Seq("features", "interfaces", "keys", "structure", "upgrades").map("canton-tests/" + _)
    }

    "record key lookups" in {
      units("keys").exists(_.entry.getKeyLookupsCount > 0) shouldBe true
      // several contracts with the same key
      units("keys").exists(
        _.entry.getKeyLookupsList.asScala.exists(_.getContractIdsCount > 1)
      ) shouldBe
        true
    }

    "record rollback nodes" in {
      units("features").exists(nodes(_).exists(_.isInstanceOf[Node.Rollback])) shouldBe true
    }

    "record interface exercises" in {
      units("interfaces").exists(nodes(_).exists {
        case exe: Node.Exercise => exe.interfaceId.isDefined
        case _ => false
      }) shouldBe true
    }

    "use two versions of the same package" in {
      val used = units("upgrades").flatMap(_.entry.getUsedPackagesList.asScala).toSet
      val versions = corpus
        .packages(used.map(Ref.PackageId.assertFromString))
        .values
        .map(_.metadata)
        .filter(_.name == Ref.PackageName.assertFromString("corpus-upgrades-model"))
        .map(_.version)
      versions.toSet should have size 2
    }

    "keep the transactions of a failing script up to the failure, and skip empty scripts" in {
      val tests = corpus.suite("canton-tests/structure").getTestsList.asScala
      tests.map(_.getName.split(':').last) should not contain "noTransactions"
      tests.find(_.getName.endsWith(":failsHalfway")).map(_.getUnits) shouldBe Some(2)
    }
  }
}
