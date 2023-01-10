// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.processing.TransactionAuthorizationValidator.AuthorizationChain
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyChangeOp}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}

import scala.concurrent.Future

class SnapshotAuthorizationValidatorTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext {

  /** our test scenario looks like:
    *
    * ns1k1_k1(root) <- ns1k2_k1(root) <- ns1k3_k2(noroot) <- ns1k8_k3_fail
    *                                                      <- okm1ak5_k3
    *                                  <- id1ak4_k2 <- okm1bk5_k4
    *                                  <- p1p2F_k2
    *                                  <- okm1ak5_k2
    *
    *                <- okm1bk5_k1
    *
    * ns6k6_k6(root) <- id6k4_k1
    *                <- p1p2T_k6
    */

  lazy val factory = new TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)

  class Env() {

    val store =
      new InMemoryTopologyStore(DomainStore(DefaultTestIdentities.domainId), loggerFactory)
    val ts = CantonTimestamp.Epoch
    val validator = new SnapshotAuthorizationValidator(ts.immediateSuccessor, store, loggerFactory)

    def append(txs: SignedTopologyTransaction[TopologyChangeOp]*): Future[Unit] = {
      store.append(
        SequencedTime(ts),
        EffectiveTime(ts),
        txs.map(sit => ValidatedTopologyTransaction(sit, None)),
      )
    }

  }

  override type FixtureParam = Env
  override def withFixture(test: OneArgAsyncTest): FutureOutcome = test(new Env())

  private def check(
      chainO: Option[AuthorizationChain],
      nsds: Seq[SignedTopologyTransaction[TopologyChangeOp]],
      ids: Seq[SignedTopologyTransaction[TopologyChangeOp]] = Seq.empty,
  ): Assertion = {
    chainO.fold(fail("chain is empty!")) { chain =>
      chain.namespaceDelegations.map(_.transaction.transaction.element.mapping) shouldBe nsds.map(
        _.transaction.element.mapping
      )
      chain.identifierDelegation.map(_.transaction.transaction.element.mapping) shouldBe ids.map(
        _.transaction.element.mapping
      )
    }

  }

  "happy cases" should {
    import factory.*
    "simple root certificate" in { fix =>
      import fix.*
      for {
        _ <- append(ns1k1_k1, okm1bk5_k1)
        empty1 <- validator.authorizedBy(p1p2F_k2)
        empty2 <- validator.authorizedBy(okm1ak5_k2)
        empty3 <- validator.authorizedBy(ns1k3_k2)
        chain <- validator.authorizedBy(okm1bk5_k1)
        chain2 <- validator.authorizedBy(ns1k1_k1)
      } yield {
        empty1 shouldBe empty
        empty2 shouldBe empty
        empty3 shouldBe empty
        check(chain, Seq(ns1k1_k1))
        check(chain2, Seq()) // root cert is
      }

    }

    "several root certificates" in { fix =>
      import fix.*
      for {
        _ <- append(ns1k1_k1, ns1k2_k1, okm1ak5_k2)
        chain <- validator.authorizedBy(okm1ak5_k2)
      } yield {
        check(chain, Seq(ns1k1_k1, ns1k2_k1))
      }
    }

    "root certificates with root delegations" in { fix =>
      import fix.*
      for {
        _ <- append(ns1k1_k1, ns1k2_k1, ns1k3_k2, okm1ak5_k3)
        chain <- validator.authorizedBy(okm1bk5_k1)
        chain2 <- validator.authorizedBy(okm1ak5_k3)
      } yield {
        check(chain, Seq(ns1k1_k1))
        check(chain2, Seq(ns1k1_k1, ns1k2_k1, ns1k3_k2))
      }
    }

    "intermediate certificates" in { fix =>
      import fix.*
      for {
        _ <- append(ns1k1_k1, ns1k2_k1, id1ak4_k2)
        chain <- validator.authorizedBy(okm1bk5_k4)
        empty1 <- validator.authorizedBy(okm1ak5_k3)
      } yield {
        check(chain, Seq(ns1k1_k1, ns1k2_k1), Seq(id1ak4_k2))
        empty1 shouldBe empty
      }
    }

    "to/from aggregation" in { fix =>
      import fix.*
      for {
        _ <- append(ns1k1_k1, ns1k2_k1, ns6k6_k6)
        chain1 <- validator.authorizedBy(p1p2T_k6)
        chain2 <- validator.authorizedBy(p1p2F_k2)
      } yield {
        check(chain1, Seq(ns6k6_k6))
        check(chain2, Seq(ns1k1_k1, ns1k2_k1))
      }
    }

    "both sides at once" in { fix =>
      import fix.*
      for {
        _ <- append(ns1k1_k1, ns1k2_k1, ns1k3_k2, ns6k6_k6, ns6k3_k6)
        chain1 <- validator.authorizedBy(p1p2B_k3)
      } yield {
        check(chain1, Seq(ns1k1_k1, ns1k2_k1, ns1k3_k2, ns6k6_k6, ns6k3_k6))
      }
    }

    "out of order chains" in { fix =>
      import fix.*
      for {
        _ <- append(ns1k2_k1, ns1k1_k1)
        chain1 <- validator.authorizedBy(okm1ak5_k2)
      } yield {
        check(chain1, Seq(ns1k1_k1, ns1k2_k1))
      }
    }

  }

  "unhappy cases" should {
    import factory.*
    "missing root cert" in { fix =>
      import fix.*
      for {
        empty1 <- validator.authorizedBy(okm1ak5_k2)
      } yield {
        empty1 shouldBe empty
      }
    }
    "missing namespace delegation" in { fix =>
      import fix.*
      for {
        _ <- append(ns1k2_k1)
        empty1 <- validator.authorizedBy(okm1ak5_k2)
      } yield {
        empty1 shouldBe empty
      }
    }
    "missing intermediate certificate" in { fix =>
      import fix.*
      for {
        _ <- append(ns1k1_k1, ns1k2_k1)
        empty1 <- validator.authorizedBy(okm1bk5_k4)
      } yield {
        empty1 shouldBe empty
      }
    }
    "with intermediate but missing root delegation" in { fix =>
      import fix.*
      for {
        _ <- append(ns1k1_k1, id1ak4_k2)
        empty1 <- validator.authorizedBy(okm1bk5_k4)
      } yield {
        empty1 shouldBe empty
      }
    }
    "broken root delegation cert chain" in { fix =>
      import fix.*
      for {
        _ <- append(ns1k1_k1, ns1k3_k2)
        empty1 <- loggerFactory.assertLogs(
          validator.authorizedBy(okm1ak5_k3),
          _.warningMessage should include("are dangling"),
        )
      } yield {
        empty1 shouldBe empty
      }
    }
    "missing side" in { fix =>
      import fix.*
      for {
        _ <- append(ns1k1_k1, ns1k2_k1, ns1k3_k2)
        empty1 <- validator.authorizedBy(p1p2B_k3)
      } yield {
        empty1 shouldBe empty
      }
    }
  }

}
