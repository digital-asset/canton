// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.syntax.option.*
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.participant.domain.DomainAliasResolution
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.ParticipantEventLog.ProductionParticipantEventLogId
import com.digitalasset.canton.participant.store.memory.{
  InMemoryMultiDomainEventLog,
  InMemoryParticipantEventLog,
}
import com.digitalasset.canton.participant.store.{
  DomainConnectionConfigStore,
  MultiDomainEventLog,
  ParticipantEventLog,
  SingleDimensionEventLogTest,
}
import com.digitalasset.canton.participant.sync.TimestampedEvent.{EventId, TimelyRejectionEventId}
import com.digitalasset.canton.participant.{LedgerSyncEvent, LedgerSyncRecordTime, LocalOffset}
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.{BaseTest, DomainAlias}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import java.util.UUID
import scala.concurrent.Future

class ParticipantEventPublisherTest extends AsyncWordSpec with BaseTest {

  case class Fixture(
      publisher: ParticipantEventPublisher,
      eventLog: ParticipantEventLog,
      multiDomainEventLog: MultiDomainEventLog,
  )

  lazy val participantId: ParticipantId =
    ParticipantId.tryFromProtoPrimitive("PAR::participant::id")
  lazy val clock = new SimClock(loggerFactory = loggerFactory)

  def withPublisher(test: Fixture => Future[Assertion]): Future[Assertion] = {
    val eventLog = new InMemoryParticipantEventLog(ProductionParticipantEventLogId, loggerFactory)
    val noDomainResolution = new DomainAliasResolution {
      override def domainIdForAlias(alias: DomainAlias): Option[DomainId] = None
      override def aliasForDomainId(id: DomainId): Option[DomainAlias] = None
      override def close(): Unit = ()
      override def connectionStateForDomain(
          id: DomainId
      ): Option[DomainConnectionConfigStore.Status] = Some(DomainConnectionConfigStore.Active)
    }
    val persistentStateManager = new SyncDomainPersistentStateManager(
      noDomainResolution,
      loggerFactory,
    )
    val indexedStringStore = InMemoryIndexedStringStore()
    val multiDomainEventLog = InMemoryMultiDomainEventLog(
      persistentStateManager,
      eventLog,
      clock,
      DefaultProcessingTimeouts.testing,
      indexedStringStore,
      ParticipantTestMetrics,
      loggerFactory,
    )
    val publisher = new ParticipantEventPublisher(
      participantId,
      eventLog,
      multiDomainEventLog,
      clock,
      Duration.ofDays(1),
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
    val fixture = new Fixture(publisher, eventLog, multiDomainEventLog)
    test(fixture)
  }

  "allocateAndInsert" should {
    "allocate offsets and insert" in withPublisher { fixture =>
      val domainId = DomainId.tryFromString("domain::id")
      val count = 10

      def mkEvent(index: Int): Traced[(EventId, LedgerSyncEvent)] = Traced {
        val eventId = TimelyRejectionEventId(domainId, new UUID(0L, index.toLong))
        val event =
          SingleDimensionEventLogTest
            .generateEvent(
              LedgerSyncRecordTime.assertFromLong(index.toLong * 1000 * 1000),
              index.toLong,
            )
            .event
        eventId -> event
      }

      val events = (1 to count).map(mkEvent)
      val moreEvents = (1 to count).map(i => mkEvent(i + count))

      for {
        allGood <- fixture.publisher.allocateOffsetsAndInsert(events)
        clash <- fixture.publisher.allocateOffsetsAndInsert(events)
        someClash <- fixture.publisher.allocateOffsetsAndInsert(
          moreEvents ++ events.take(count / 2)
        )
      } yield {
        allGood.size shouldBe count
        val offsets = allGood.map(_.value)
        offsets.sorted shouldBe offsets // new offsets are ascending
        offsets.distinct shouldBe offsets // new offsets are distinct

        def mkTimestamped(
            eventIdAndEvent: Traced[(EventId, LedgerSyncEvent)],
            offset: LocalOffset,
        ): TimestampedEvent = {
          val Traced((eventId, event)) = eventIdAndEvent
          TimestampedEvent(event, offset, None, eventId.some)(eventIdAndEvent.traceContext)
        }

        clash.size shouldBe count
        clash.map(_.left.value) shouldBe events.lazyZip(offsets).map(mkTimestamped)

        someClash.size shouldBe count + count / 2
        val newOffsets = someClash.take(count).map(_.value)
        newOffsets.sorted shouldBe newOffsets
        newOffsets.distinct shouldBe newOffsets
        newOffsets.minOption.value should be > offsets.maxOption.value
        val clashes = someClash.drop(count)
        clashes.map(_.left.value) shouldBe events
          .take(count / 2)
          .lazyZip(offsets.take(count / 2))
          .map(mkTimestamped)
      }
    }
  }
}
