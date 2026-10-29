// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.HashMap

class GraphsSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import Graphs.*

  "topoSort" should {

    val dags = Table[Graph[Int]](
      "directed acyclic graphs",
      HashMap.empty,
      HashMap(
        3 -> Set(8, 10),
        5 -> Set(11),
        7 -> Set(8, 11),
        8 -> Set(9),
        11 -> Set(2, 9, 10),
      ),
    )

    val dcgs = Table[Graph[String]](
      "directed cyclic graphs",
      HashMap("1" -> Set("1")),
      HashMap("A" -> Set("B"), "B" -> Set("A")),
      HashMap(
        "A" -> Set("B", "C", "E"),
        "B" -> Set("C", "E"),
        "C" -> Set("D"),
        "D" -> Set("B", "E"),
        "E" -> Set("E"),
      ),
    )

    "successfully sort all edges of directed acyclic graph" in {
      dags.forEvery { dag =>
        val result = topoSort(dag)
        result shouldBe a[Right[?, ?]]

        val Right(sortedEdges) = result

        val allEdges = dag.values.foldLeft(dag.keySet)(_ | _)
        sortedEdges.toSet shouldBe allEdges

        val edgeRank = sortedEdges.zipWithIndex.toMap
        for {
          e <- dag.keys
          e_ <- dag(e)
        } edgeRank(e_) should be < edgeRank(e)
      }
    }

    "fail on cyclic graph and return a proper cycle" in {
      dcgs.forEvery { dcg =>
        val result = topoSort(dcg)
        result shouldBe a[Left[?, ?]]

        val Left(Cycle(loop)) = result

        ((loop.last :: loop) zip loop).foreach { case (e, e_) =>
          dcg(e) should contain(e_)
        }
      }
    }

    "return a proper cycle for a simple cycle of length 3" in {
      val graph: Graph[Int] = HashMap(1 -> Set(2), 2 -> Set(3), 3 -> Set(1))
      val result = topoSort(graph)
      result shouldBe a[Left[?, ?]]
      val Left(Cycle(loop)) = result
      loop should contain theSameElementsAs List(1, 2, 3)
      ((loop.last :: loop) zip loop).foreach { case (e, e_) =>
        graph(e) should contain(e_)
      }
    }

    "return Left with a proper cycle for random cyclic graphs" in {
      val rnd = new scala.util.Random(7)
      for (_ <- 0 until 500) {
        val n = 2 + rnd.nextInt(8)
        val density = rnd.nextDouble() * 0.5
        val base: Map[Int, Set[Int]] =
          (0 until n).map(u => u -> (0 until n).filter(_ => rnd.nextDouble() < density).toSet).toMap
        // force a cycle through a random walk
        val walk = rnd.shuffle((0 until n).toList).take(2 + rnd.nextInt(n - 1))
        val graph: Graph[Int] = (walk zip (walk.tail :+ walk.head)).foldLeft(base) {
          case (g, (u, v)) => g.updated(u, g(u) + v)
        }
        withClue(s"graph = $graph: ") {
          val result = topoSort(graph)
          result shouldBe a[Left[?, ?]]
          val Left(Cycle(loop)) = result
          loop should not be empty
          ((loop.last :: loop) zip loop).foreach { case (e, e_) =>
            graph(e) should contain(e_)
          }
        }
      }
    }

    "sort random directed acyclic graphs with arbitrary labels and missing sink keys" in {
      val rnd = new scala.util.Random(42)

      for (_ <- 0 until 2000) {
        val n = 1 + rnd.nextInt(12)
        // random labels, so that label order is unrelated to the topological order
        val labels = rnd.shuffle((0 until 100).toList).take(n)
        val density = rnd.nextDouble()
        // edges only go from earlier to later position in `labels`, hence acyclic
        val full: Map[Int, Set[Int]] = labels.zipWithIndex.map { case (u, i) =>
          u -> labels.drop(i + 1).filter(_ => rnd.nextDouble() < density).toSet
        }.toMap
        // randomly drop keys of vertices without successors (they may remain in the values)
        val graph: Graph[Int] = full.filter { case (_, succs) =>
          succs.nonEmpty || rnd.nextBoolean()
        }

        withClue(s"graph = $graph: ") {
          val result = topoSort(graph)
          result shouldBe a[Right[?, ?]]

          val Right(order) = result
          order.distinct shouldBe order
          order.toSet shouldBe (graph.keySet | graph.values.flatten.toSet)

          val rank = order.zipWithIndex.toMap
          for {
            (u, succs) <- graph
            v <- succs
          } rank(v) should be < rank(u)
        }
      }
    }

    "be stack safe on a long acyclic chain" in {
      val n = 10000
      // 0 -> 1 -> ... -> n, `n` is a sink and is not a key
      val graph: Graph[Int] = HashMap.from((0 until n).map(i => i -> Set(i + 1)))

      topoSort(graph) shouldBe Right((0 to n).reverse.toList)
    }

    "be stack safe on a long cycle" in {
      val n = 10000
      // 0 -> 1 -> ... -> n-1 -> 0
      val graph: Graph[Int] = HashMap.from((0 until n).map(i => i -> Set((i + 1) % n)))

      val result = topoSort(graph)
      result shouldBe a[Left[?, ?]]
      val Left(Cycle(loop)) = result

      loop should have length n.toLong
      ((loop.last :: loop) zip loop).foreach { case (e, e_) =>
        graph(e) should contain(e_)
      }
    }
  }

}
