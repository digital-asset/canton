// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config.store

import cats.data.NonEmptyList
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import io.functionmeta.functionFullName
import org.scalatest.wordspec.{AsyncWordSpec, AsyncWordSpecLike}

import scala.concurrent.Future

trait DomainNodeSequencerConfigStoreTest {
  this: AsyncWordSpecLike with BaseTest =>

  def domainManagerNodeSequencerConfigStoreTest(
      mkStore: => DomainManagerNodeSequencerConfigStore
  ): Unit = {
    "when unset returns nothing" in {
      val store = mkStore

      for {
        config <- valueOrFail(store.fetchConfiguration)("fetchConfiguration")
      } yield config shouldBe None
    }

    "when set returns set value" in {
      val store = mkStore
      val originalConfig = DomainNodeSequencerConfig(
        GrpcSequencerConnection(
          NonEmptyList.of(
            Endpoint("sequencer", Port.tryCreate(100)),
            Endpoint("sequencer", Port.tryCreate(200)),
          ),
          false,
          None,
        )
      )

      for {
        _ <- valueOrFail(store.saveConfiguration(originalConfig))("saveConfiguration")
        persistedConfig <- valueOrFail(store.fetchConfiguration)("fetchConfiguration").map(_.value)
      } yield persistedConfig shouldBe originalConfig
    }

    "supports updating the config" in {
      val store = mkStore
      val originalConfig = DomainNodeSequencerConfig(
        GrpcSequencerConnection(
          NonEmptyList.of(
            Endpoint("sequencer", Port.tryCreate(100)),
            Endpoint("sequencer", Port.tryCreate(200)),
          ),
          false,
          None,
        )
      )
      val updatedConfig = DomainNodeSequencerConfig(
        GrpcSequencerConnection(
          NonEmptyList.of(
            Endpoint("sequencer", Port.tryCreate(300)),
            Endpoint("sequencer", Port.tryCreate(400)),
          ),
          false,
          None,
        )
      )

      for {
        _ <- valueOrFail(store.saveConfiguration(originalConfig))("save original config")
        persistedConfig1 <- valueOrFail(store.fetchConfiguration)("fetch original config")
          .map(_.value)
        _ = persistedConfig1 shouldBe originalConfig
        _ <- valueOrFail(store.saveConfiguration(updatedConfig))("save updated config")
        persistedConfig2 <- valueOrFail(store.fetchConfiguration)("fetch updated config")
          .map(_.value)
      } yield persistedConfig2 shouldBe updatedConfig
    }
  }

}

class DomainManagerNodeSequencerConfigStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with DomainNodeSequencerConfigStoreTest {

  behave like domainManagerNodeSequencerConfigStoreTest(
    new InMemoryDomainManagerNodeSequencerConfigStore()
  )
}

trait DbDomainManagerNodeSequencerConfigStoreTest
    extends AsyncWordSpec
    with BaseTest
    with DomainNodeSequencerConfigStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api._
    storage.update(
      DBIO.seq(
        sqlu"truncate table domain_sequencer_config"
      ),
      functionFullName,
    )
  }

  behave like domainManagerNodeSequencerConfigStoreTest(
    new DbDomainManagerNodeSequencerConfigStore(storage, timeouts, loggerFactory)
  )

}

class DomainManagerNodeSequencerConfigStoreTestPostgres
    extends DbDomainManagerNodeSequencerConfigStoreTest
    with PostgresTest

class DomainManagerNodeSequencerConfigStoreTestH2
    extends DbDomainManagerNodeSequencerConfigStoreTest
    with H2Test
