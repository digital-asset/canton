// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.instances.order.*
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.AuthorizedNamespaceDelegation
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTestWordSpec, ProtocolVersionChecksAnyWordSpec}
import org.scalatest.wordspec.AnyWordSpec

class DecentralizedNamespaceAuthorizationGraphTest
    extends AnyWordSpec
    with BaseTestWordSpec
    with ProtocolVersionChecksAnyWordSpec
    with BaseAuthorizationGraphTest {

  import DecentralizedNamespaceAuthorizationGraphTest.*
  import factory.SigningKeys.*

  private def mkGraph: DecentralizedNamespaceAuthorizationGraph =
    DecentralizedNamespaceAuthorizationGraph(
      decentralizedNamespaceDefinition,
      owners
        .map(new AuthorizationGraph(_, extraDebugInfo = false, loggerFactory = loggerFactory))
        .forgetNE
        .toSeq,
    )

  "authorization graph for a decentralized namespace" when {

    "only having namespace delegations for its constituents" should {
      "work for a simple quorum" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns3k3k3)

        // Individual keys are not enough
        for {
          key <- Seq(key1, key2, key3)
        } {
          forAll(allMappings)(check(graph, _, valid = false)(key))
        }

        // at least quorum number of signatures is enough
        Seq(key1, key2, key3)
          .combinations(decentralizedNamespaceDefinition.threshold.value)
          .foreach { keys =>
            forAll(allMappings)(check(graph, _, valid = true)(keys*))
          }
      }
      "support longer chains" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns3k3k3)

        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k8k5)
        forAll(allMappings)(check(graph, _, valid = true)(key1, key8, key3))
      }

      "support removal" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns3k3k3)

        graph.removeAuth(ns2k2k2_remove)
        forAll(allMappings)(check(graph, _, valid = false)(key1, key2))
        forAll(allMappings)(check(graph, _, valid = true)(key1, key3))
      }

      "support breaking and re-creating chains" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)

        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k8k5)
        forAll(allMappings)(check(graph, _, valid = true)(key1, key2))
        forAll(allMappings)(check(graph, _, valid = true)(key1, key5))
        forAll(allMappings)(check(graph, _, valid = true)(key1, key8))
        loggerFactory.assertLogs(
          graph.removeAuth(ns2k5k2_remove),
          _.warningMessage should (include regex s"dangling.*${key8.fingerprint}"),
        )
        forAll(allMappings)(check(graph, _, valid = false)(key1, key5))
        forAll(allMappings)(check(graph, _, valid = false)(key1, key8))
        graph.addAuth(ns2k5k2)
        forAll(allMappings)(check(graph, _, valid = true)(key1, key5))
        forAll(allMappings)(check(graph, _, valid = true)(key1, key8))
      }

      "not support several chains" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)

        graph.addAuth(ns2k5k2)
        // this is ns2k8 serial=1
        graph.addAuth(ns2k8k5)
        forAll(allMappings)(check(graph, _, valid = true)(key1, key8))

        // this is ns2k8 serial=2 and overwrites the previous mapping ns2k8 signed by k5
        graph.addAuth(ns2k8k2_nonRoot)
        forAll(allButNSD)(check(graph, _, valid = true)(key1, key8))

        // this is ns2k8 serial=3 and removes the ns2k8 mapping entirely
        graph.removeAuth(ns2k8k2_nonRoot_remove)
        forAll(allMappings)(check(graph, _, valid = false)(key1, key8))
      }

      "deal with cycles" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k8k5)

        val danglingKeys = List(key5, key8).map(_.fingerprint).sorted.mkString(", ")
        loggerFactory.assertLogs(
          // this overwrites ns2k5k2, leading to a break in the authorization chain for the now dangling k5 and k8
          graph.addAuth(ns2k5k8),
          _.warningMessage should (include regex s"dangling.*$danglingKeys"),
        )
        forAll(allMappings)(check(graph, _, valid = true)(key1, key2))
        forAll(allMappings)(check(graph, _, valid = false)(key1, key5))
        forAll(allMappings)(check(graph, _, valid = false)(key1, key8))
      }

      "deal with root revocations" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns3k3k3)

        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k8k5)

        val danglingKeys = List(key5, key8).map(_.fingerprint).sorted.mkString(", ")
        loggerFactory.assertLogs(
          graph.removeAuth(ns2k2k2_remove),
          _.warningMessage should (include regex s"dangling.*$danglingKeys"),
        )
        forAll(allMappings)(check(graph, _, valid = false)(key1, key2))
        forAll(allMappings)(check(graph, _, valid = false)(key1, key5))
        forAll(allMappings)(check(graph, _, valid = false)(key1, key8))

        // however, key1 and key3 can still reach quorum
        forAll(allMappings)(check(graph, _, valid = true)(key1, key3))
      }

      "correctly distinguish on root delegations" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns2k8k2_nonRoot)
        forAll(allMappings)(check(graph, _, valid = true)(key1, key2))
        check(graph, Code.NamespaceDelegation, valid = false)(key1, key8)
        forAll(allButNSD)(check(graph, _, valid = true)(key1, key8))
      }

      "deal with same mappings used twice" in {
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)
        graph.addAuth(ns2k5k2)
        forAll(allMappings)(check(graph, _, valid = true)(key1, key5))
        // test that random key is not authorized
        forAll(allMappings)(check(graph, _, valid = false)(key1, key3))
        // remove first certificate
        graph.removeAuth(ns2k5k2_remove)
        forAll(allMappings)(check(graph, _, valid = false)(key1, key5))
        // add other certificate (we don't remember removes, so we can do that in this test)
        graph.addAuth(ns2k5k2)
        forAll(allMappings)(check(graph, _, valid = true)(key1, key5))
      }

      "test removal of transactions authorized with different keys" in {
        // can actually do it (add k2 with one key, remove k2 permission with another, but fail to remove it with the other is not valid)
        val graph = mkGraph
        graph.addAuth(ns1k1k1)
        graph.addAuth(ns2k2k2)

        graph.addAuth(ns2k5k2)
        graph.addAuth(ns2k8k5)
        forAll(allMappings)(check(graph, _, valid = true)(key1, key8))

        graph.removeAuth(replaceSignature(ns2k8k5_remove, key2))
        forAll(allMappings)(check(graph, _, valid = false)(key1, key8))
      }
    }
  }
}

object DecentralizedNamespaceAuthorizationGraphTest {
  implicit class DecentralizedNamespaceAuthorizationGraphExtension(
      val dns: DecentralizedNamespaceAuthorizationGraph
  ) extends AnyVal {
    def addAuth(
        authorizedNSD: AuthorizedNamespaceDelegation
    )(implicit traceContext: TraceContext): Unit =
      dns.ownerGraphs
        .find(_.namespace == authorizedNSD.mapping.namespace)
        .foreach(_.replace(authorizedNSD))

    def removeAuth(
        authorizedNSD: AuthorizedNamespaceDelegation
    )(implicit traceContext: TraceContext): Unit =
      dns.ownerGraphs
        .find(_.namespace == authorizedNSD.mapping.namespace)
        .foreach(_.remove(authorizedNSD))
  }
}
