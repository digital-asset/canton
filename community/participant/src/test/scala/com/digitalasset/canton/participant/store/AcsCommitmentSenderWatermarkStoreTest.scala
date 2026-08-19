// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.commitment.Timepoint
import com.digitalasset.canton.participant.store.db.DbAcsCommitmentSenderWatermarkStore
import com.digitalasset.canton.participant.store.memory.InMemoryAcsCommitmentSenderWatermarkStore
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.DefaultTestIdentities.synchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.wordspec.AsyncWordSpec

trait AcsCommitmentSenderWatermarkStoreTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec
    with TestDigestUtils {

  import AcsCommitmentSenderWatermarkStoreTest.*

  "AcsCommitmentSenderWatermarkStore" should {
    "retrieve None when there is no watermark" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val watermarkStore1 = mkWatermarkStore(indexedSynchronizer1)
      val watermarkStore2 = mkWatermarkStore(indexedSynchronizer2)

      watermarkStore1.lookupWatermark().futureValueUS shouldBe None
      watermarkStore2.lookupWatermark().futureValueUS shouldBe None
    }

    "increase and retrieve the expected watermark" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val watermarkStore1 = mkWatermarkStore(indexedSynchronizer1)
      val watermarkStore2 = mkWatermarkStore(indexedSynchronizer2)

      watermarkStore1.increaseWatermark(tp0).futureValueUS
      watermarkStore2.increaseWatermark(tp1).futureValueUS

      watermarkStore1.lookupWatermark().futureValueUS.value.tupled shouldBe tp0.tupled
      watermarkStore2.lookupWatermark().futureValueUS.value.tupled shouldBe tp1.tupled
    }

    "increase the already existing watermark" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val watermarkStore = mkWatermarkStore(indexedSynchronizer1)

      watermarkStore.increaseWatermark(tp0).futureValueUS
      watermarkStore.lookupWatermark().futureValueUS.value.tupled shouldBe tp0.tupled

      watermarkStore.increaseWatermark(tp2).futureValueUS
      watermarkStore.lookupWatermark().futureValueUS.value.tupled shouldBe tp2.tupled

      watermarkStore.increaseWatermark(tp2a).futureValueUS
      watermarkStore.lookupWatermark().futureValueUS.value.tupled shouldBe tp2a.tupled
    }

    "not increase the already existing watermark if the existing value is higher" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val watermarkStore = mkWatermarkStore(indexedSynchronizer1)

      watermarkStore.increaseWatermark(tp2).futureValueUS
      watermarkStore.lookupWatermark().futureValueUS.value.tupled shouldBe tp2.tupled

      watermarkStore.increaseWatermark(tp0).futureValueUS
      watermarkStore
        .lookupWatermark()
        .futureValueUS
        .value
        .tupled shouldBe tp2.tupled // Still the previous value
    }
  }

  protected def mkWatermarkStore(
      indexedSynchronizer: IndexedSynchronizer
  ): AcsCommitmentSenderWatermarkStore
}

object AcsCommitmentSenderWatermarkStoreTest extends TestDigestUtils {
  private lazy val indexedSynchronizer1 =
    IndexedSynchronizer.tryCreate(synchronizerId, 1)
  private lazy val indexedSynchronizer2 =
    IndexedSynchronizer.tryCreate(synchronizerId, 2)

  lazy val tp0: Timepoint = tp(10)
  lazy val tp1: Timepoint = tp(20)
  lazy val tp2: Timepoint = tp(30)
  lazy val tp2a: Timepoint = tp2.copy()(ts(123))
}

abstract class DbAcsCommitmentSenderWatermarkStoreTest
    extends AcsCommitmentSenderWatermarkStoreTest { self: DbTest =>
  override protected def mkWatermarkStore(
      indexedSynchronizer: IndexedSynchronizer
  ): AcsCommitmentSenderWatermarkStore =
    new DbAcsCommitmentSenderWatermarkStore(
      storage = storage,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
      indexedSynchronizer = indexedSynchronizer,
    )

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*

    storage
      .update_(
        sqlu"truncate table par_acs_commitment_sender_watermark",
        functionFullName,
      )
  }
}

@AcsCommitmentTest
class DbAcsCommitmentSenderWatermarkStoreTestPostgres
    extends DbAcsCommitmentSenderWatermarkStoreTest
    with PostgresTest

@AcsCommitmentTest
class DbAcsCommitmentSenderWatermarkStoreTestH2
    extends DbAcsCommitmentSenderWatermarkStoreTest
    with H2Test

@AcsCommitmentTest
class AcsCommitmentSenderWatermarkStoreTestInMemory extends AcsCommitmentSenderWatermarkStoreTest {

  // @nowarn("cat=unused")
  override protected def mkWatermarkStore(
      indexedSynchronizer: IndexedSynchronizer
  ): AcsCommitmentSenderWatermarkStore =
    new InMemoryAcsCommitmentSenderWatermarkStore(loggerFactory)
}
