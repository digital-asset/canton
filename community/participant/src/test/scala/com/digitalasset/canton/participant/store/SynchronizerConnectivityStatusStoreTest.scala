// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import org.scalatest.wordspec.AsyncWordSpec

trait SynchronizerConnectivityStatusStoreTest extends FailOnShutdown {
  this: AsyncWordSpec & BaseTest =>

  private val synchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizerId::synchronizerId")
  ).toPhysical

  private def anotherProtocolVersion(testedProtocolVersion: ProtocolVersion): ProtocolVersion =
    if (testedProtocolVersion.isDev)
      ProtocolVersion.minimum
    else
      ProtocolVersion.dev

  def synchronizerConnectivityStatusStore(
      mk: PhysicalSynchronizerId => SynchronizerConnectivityStatusStore
  ): Unit = {

    "setParameters" should {
      "store new parameters" in {
        val store = mk(synchronizerId)
        val params = defaultStaticSynchronizerParameters
        for {
          _ <- store.setParameters(params)
          last <- store.lastParameters
        } yield {
          last shouldBe Some(params)
          store.isTopologyInitialized shouldBe false
        }
      }

      "be idempotent" in {
        val store = mk(synchronizerId)
        val params =
          BaseTest.defaultStaticSynchronizerParametersWith(
            topologyChangeDelay = StaticSynchronizerParameters.defaultTopologyChangeDelay,
            protocolVersion = anotherProtocolVersion(testedProtocolVersion),
          )
        for {
          _ <- store.setParameters(params)
          _ <- store.setParameters(params)
          last <- store.lastParameters
        } yield {
          last shouldBe Some(params)
        }
      }

      "not overwrite changed synchronizer parameters" in {
        val store = mk(synchronizerId)
        val params = defaultStaticSynchronizerParameters
        val modified =
          BaseTest.defaultStaticSynchronizerParametersWith(
            topologyChangeDelay = StaticSynchronizerParameters.defaultTopologyChangeDelay,
            protocolVersion = anotherProtocolVersion(testedProtocolVersion),
          )
        for {
          _ <- store.setParameters(params)
          ex <- store.setParameters(modified).failed
          last <- store.lastParameters
        } yield {
          ex shouldBe an[IllegalArgumentException]
          last shouldBe Some(params)
        }
      }
    }

    "lastParameters" should {
      "return None for the empty store" in {
        val store = mk(synchronizerId)
        for {
          last <- store.lastParameters
        } yield {
          last shouldBe None
        }
      }
    }

    "setTopologyInitialized" should {
      "flip the flag and survive subsequent setParameters calls" in {
        val store = mk(synchronizerId)
        val params = defaultStaticSynchronizerParameters
        for {
          _ <- store.setParameters(params)
          _ = store.isTopologyInitialized shouldBe false

          _ <- store.setTopologyInitialized()
          _ = store.isTopologyInitialized shouldBe true

          _ <- store.setParameters(params)
          _ = store.isTopologyInitialized shouldBe true
        } yield succeed
      }
    }
  }
}
