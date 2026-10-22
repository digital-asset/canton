// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.ingestion

import com.daml.ledger.api.v2.event.{CreatedEvent, Event, ExercisedEvent}
import com.daml.ledger.api.v2.offset_checkpoint.{OffsetCheckpoint, SynchronizerTime}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, TransactionShape}
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.ledger.api.v2.value.{
  Identifier,
  Record,
  RecordField,
  Value,
  Optional as DamlOptional,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.client.services.state.StateServiceClient
import com.digitalasset.canton.ledger.client.services.updates.UpdateServiceClient
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig.ProjectionConfig
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TopUpNotCredited
import com.digitalasset.canton.tea.ingestion.TrafficPurchaseModel.Template
import com.digitalasset.canton.tea.projection.{
  AccountDelta,
  AccountId,
  EventId,
  ProjectionEvent,
  TrafficDelta,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.*
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.nonempty.NonEmpty
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.mockito.ArgumentCaptor
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class TeaCreditIngestionServiceTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private implicit val actorSystem: ActorSystem = ActorSystem(getClass.getSimpleName)
  private implicit val materializer: Materializer = Materializer(actorSystem)

  override def afterAll(): Unit = {
    materializer.shutdown()
    Await.result(actorSystem.terminate(), 30.seconds)
    super.afterAll()
  }

  private val alice = AccountId.tryCreate("alice")
  private val bob = AccountId.tryCreate("bob")

  private val recordTime: CantonTimestamp = CantonTimestamp.ofEpochSecond(1000)

  private val paymaster = PartyId.tryFromProtoPrimitive("paymaster::default")
  private val defaultConfig = ProjectionConfig(topUpPaymasters = Set(paymaster))

  private def serviceWith(
      updates: Seq[GetUpdatesResponse] = Seq.empty,
      prunedUpTo: Long = 0L,
      projectionConfig: ProjectionConfig = defaultConfig,
  ): (TeaCreditIngestionService, UpdateServiceClient, StateServiceClient) = {
    val updateServiceClient = mock[UpdateServiceClient]
    val stateServiceClient = mock[StateServiceClient]

    when(
      stateServiceClient.getParticipantPrunedUpToOffset(any[Option[String]])(any[TraceContext])
    ).thenReturn(Future.successful(prunedUpTo))
    when(
      updateServiceClient.getUpdatesSource(
        any[Long],
        any[EventFormat],
        any[TransactionShape],
        any[Option[Long]],
        any[Option[String]],
      )(any[TraceContext])
    ).thenReturn(Source(updates.toList))

    val service = new TeaCreditIngestionService(
      updateServiceClient,
      stateServiceClient,
      projectionConfig,
      loggerFactory,
    )
    (service, updateServiceClient, stateServiceClient)
  }

  private def record(fields: (String, Value)*): Value =
    Value(Value.Sum.Record(Record(fields = fields.map { case (label, value) =>
      RecordField(label, Some(value))
    })))

  private def text(value: String): Value = Value(Value.Sum.Text(value))
  private def numeric(value: String): Value = Value(Value.Sum.Numeric(value))
  private def int64(value: Long): Value = Value(Value.Sum.Int64(value))

  /** Mimics the shape of `TrafficPurchasedResult`. */
  private def topUpResult(
      account: String,
      trafficAmount: Value,
      paymasterParty: String = paymaster.toProtoPrimitive,
  ): Value =
    record(
      "paymaster" -> Value(Value.Sum.Party(paymasterParty)),
      "purchaser" -> Value(Value.Sum.Party("purchaser::default")),
      "targetAccount" -> record("accountId" -> text(account), "scope" -> text("tenant")),
      "paymentAsset" -> Value(Value.Sum.Optional(DamlOptional(None))),
      "trafficAmount" -> trafficAmount,
      "requestId" -> text("request-1"),
    )

  private def exercised(
      nodeId: Int,
      result: Option[Value],
      choice: String = Template.ChoiceName,
  ): Event =
    Event(
      Event.Event.Exercised(
        ExercisedEvent.defaultInstance.copy(
          nodeId = nodeId,
          templateId = Some(
            Identifier(
              packageId = "#some-package-id",
              moduleName = Template.ModuleName,
              entityName = Template.TemplateName,
            )
          ),
          choice = choice,
          exerciseResult = result,
          packageName = Template.PackageName,
        )
      )
    )

  private def topUp(nodeId: Int, account: String, trafficAmount: Value): Event =
    exercised(nodeId, Some(topUpResult(account, trafficAmount)))

  private def transaction(offset: Long, events: Event*): GetUpdatesResponse =
    TraceContext.withNewTraceContext("test-tc") { tc =>
      GetUpdatesResponse(
        GetUpdatesResponse.Update.Transaction(
          Transaction.defaultInstance.copy(
            updateId = s"update-$offset",
            offset = offset,
            recordTime = Some(recordTime.toProtoTimestamp),
            events = events,
            traceContext = SerializableTraceContext(tc).toDamlProto,
          )
        )
      )
    }

  private def checkpoint(offset: Long, recordTimes: CantonTimestamp*): GetUpdatesResponse =
    GetUpdatesResponse(
      GetUpdatesResponse.Update.OffsetCheckpoint(
        OffsetCheckpoint(
          offset = offset,
          synchronizerTimes = recordTimes.zipWithIndex.map { case (rt, i) =>
            SynchronizerTime(s"synchronizer-$i::tea", Some(rt.toProtoTimestamp))
          },
        )
      )
    )

  private def credit(account: AccountId, amount: Long): AccountDelta =
    AccountDelta(account, TrafficDelta.creditBalanceDelta(amount))

  private def projectionEvent(
      offset: Long,
      nodeIdAndDelta: (Int, AccountDelta),
      otherNodeIdAndDeltas: (Int, AccountDelta)*
  ): ProjectionEvent =
    ProjectionEvent.Multiple(
      offset,
      recordTime,
      NonEmpty(Map, nodeIdAndDelta, otherNodeIdAndDeltas*),
    )

  private def runEvents(source: Source[Traced[ProjectionEvent], NotUsed]): Seq[ProjectionEvent] =
    source.runWith(Sink.seq).futureValue.map(_.value)

  private def decode(updates: GetUpdatesResponse*): Seq[ProjectionEvent] = {
    val (service, _, _) = serviceWith(updates)
    runEvents(service.grpcSource(beginOffset = Some(1L)))
  }

  "TeaCreditIngestionService" should {

    "credit the account with the traffic amount of a top-up choice result" in {
      decode(transaction(10L, topUp(nodeId = 3, "alice", int64(1000L)))) shouldBe Seq(
        projectionEvent(10L, 3 -> credit(alice, 1000L))
      )
    }

    "emit one event per transaction with all its top-ups" in {
      decode(
        transaction(
          10L,
          topUp(nodeId = 0, "alice", int64(1L)),
          topUp(nodeId = 1, "bob", int64(2L)),
        ),
        transaction(11L, topUp(nodeId = 0, "alice", int64(3L))),
      ) shouldBe Seq(
        projectionEvent(10L, 0 -> credit(alice, 1L), 1 -> credit(bob, 2L)),
        projectionEvent(11L, 0 -> credit(alice, 3L)),
      )
    }

    "derive the event ids of top-ups from their node ids" in {
      val events = decode(
        transaction(
          10L,
          topUp(nodeId = 2, "alice", int64(1L)),
          topUp(nodeId = 7, "alice", int64(1L)),
        )
      )

      events.flatMap(event =>
        event.deltasWithEventIds("credit-ingestion").map(_._2)
      ) should contain theSameElementsAs Seq(
        EventId.tryCreate("credit-ingestion-10-2"),
        EventId.tryCreate("credit-ingestion-10-7"),
      )
    }

    "ignore events that are not a top-up choice" in {
      val result = Some(topUpResult("alice", int64(1L)))
      decode(
        transaction(
          10L,
          Event(Event.Event.Created(CreatedEvent.defaultInstance)),
          exercised(0, result, choice = "TrafficPurchaser_GrantTraffic"),
        ),
        GetUpdatesResponse(
          GetUpdatesResponse.Update.OffsetCheckpoint(OffsetCheckpoint.defaultInstance)
        ),
      ) shouldBe empty
    }

    "skip top-ups with an invalid result and keep the valid ones" in {
      val tooLongAccount = "a" * 256
      loggerFactory.assertLogsUnordered(
        decode(
          transaction(
            10L,
            topUp(nodeId = 0, "alice", numeric("1000.0")),
            topUp(nodeId = 1, "alice", int64(0L)),
            topUp(nodeId = 2, "alice", int64(-3L)),
            topUp(nodeId = 3, tooLongAccount, int64(1L)),
            topUp(nodeId = 4, "alice", text("10")),
            exercised(5, Some(record("trafficAmount" -> int64(1L)))),
            exercised(6, result = None),
            topUp(nodeId = 7, "bob", int64(5L)),
            exercised(8, Some(record("paymaster" -> text(paymaster.toProtoPrimitive)))),
          )
        ) shouldBe Seq(projectionEvent(10L, 7 -> credit(bob, 5L))),
        _.errorMessage should include("node 0: expected an Int64 traffic amount"),
        _.errorMessage should include("node 1: traffic amount must be positive, got 0"),
        _.errorMessage should include("node 2: traffic amount must be positive, got -3"),
        _.errorMessage should include("node 3: invalid account id"),
        _.errorMessage should include("node 4: expected an Int64 traffic amount"),
        _.errorMessage should include("node 5: missing field 'paymaster'"),
        _.errorMessage should include("node 6: missing exercise result"),
        _.errorMessage should include("node 8: expected a party"),
      )
    }

    "skip top-ups made on a contract of a paymaster that is not configured" in {
      loggerFactory.assertLogs(
        decode(
          transaction(
            10L,
            exercised(0, Some(topUpResult("alice", int64(1L), paymasterParty = "intruder::x"))),
            topUp(nodeId = 1, "bob", int64(2L)),
          )
        ) shouldBe Seq(projectionEvent(10L, 1 -> credit(bob, 2L))),
        _.warningMessage should include(
          "node 0: paymaster intruder::x is not one of the configured top-up paymasters"
        ),
      )
    }

    "emit no event for a transaction without valid top-ups" in {
      loggerFactory.assertLogs(
        decode(transaction(10L, topUp(nodeId = 0, "alice", int64(0L)))) shouldBe empty,
        entry => {
          entry.errorMessage should include("node 0: traffic amount must be positive, got 0")
          entry.shouldBeCantonError(
            TopUpNotCredited,
            contextAssertion = context => {
              context.get("update_id") shouldBe Some("update-10")
              context.get("offset") shouldBe Some("10")
            },
          )
        },
      )
    }

    "subscribe to the configured template for the paymasters with verbose ledger effects" in {
      val otherPaymaster = PartyId.tryFromProtoPrimitive("paymaster::other")
      val (service, updateServiceClient, _) = serviceWith(projectionConfig =
        ProjectionConfig(topUpPaymasters = Set(paymaster, otherPaymaster))
      )
      runEvents(service.grpcSource(beginOffset = Some(1L)))

      val formatCaptor = ArgumentCaptor.forClass(classOf[EventFormat])
      verify(updateServiceClient).getUpdatesSource(
        any[Long],
        formatCaptor.capture(),
        eqTo(TRANSACTION_SHAPE_LEDGER_EFFECTS),
        any[Option[Long]],
        any[Option[String]],
      )(any[TraceContext])

      val format = formatCaptor.getValue
      format.verbose shouldBe true
      format.filtersForAnyParty shouldBe empty
      format.filtersByParty.keySet shouldBe Set(paymaster, otherPaymaster).map(_.toProtoPrimitive)
      forAll(format.filtersByParty.values) { filters =>
        val templateIds = filters.cumulative
          .map(_.identifierFilter)
          .collect { case IdentifierFilter.TemplateFilter(filter) => filter.templateId }
        templateIds shouldBe Seq(
          Some(
            Identifier(
              packageId = s"#${Template.PackageName}",
              moduleName = Template.ModuleName,
              entityName = Template.TemplateName,
            )
          )
        )
      }
    }

    "resume from the persisted offset" in {
      val (service, updateServiceClient, stateServiceClient) = serviceWith(prunedUpTo = 42L)
      runEvents(service.grpcSource(beginOffset = Some(10L)))

      verify(stateServiceClient, never).getParticipantPrunedUpToOffset(any[Option[String]])(
        any[TraceContext]
      )
      verify(updateServiceClient).getUpdatesSource(
        eqTo(10L),
        any[EventFormat],
        any[TransactionShape],
        any[Option[Long]],
        any[Option[String]],
      )(any[TraceContext])
    }

    "use the configured initial offset" in {
      val (service, updateServiceClient, stateServiceClient) = serviceWith(
        prunedUpTo = 42L,
        projectionConfig = defaultConfig.copy(initialTopUpOffsetBeginExclusive = Some(31380L)),
      )
      runEvents(service.grpcSource(beginOffset = None))

      verify(stateServiceClient, never).getParticipantPrunedUpToOffset(any[Option[String]])(
        any[TraceContext]
      )
      verify(updateServiceClient).getUpdatesSource(
        eqTo(31380L),
        any[EventFormat],
        any[TransactionShape],
        any[Option[Long]],
        any[Option[String]],
      )(any[TraceContext])
    }

    "start from the participant pruned up to offset when no offset is known" in {
      val (service, updateServiceClient, _) = serviceWith(prunedUpTo = 42L)
      runEvents(service.grpcSource(beginOffset = None))

      verify(updateServiceClient).getUpdatesSource(
        eqTo(42L),
        any[EventFormat],
        any[TransactionShape],
        any[Option[Long]],
        any[Option[String]],
      )(any[TraceContext])
    }

    "emit a checkpoint event at the latest synchronizer record time" in {
      val later = recordTime.plusSeconds(5)
      decode(checkpoint(20L, recordTime, later)) shouldBe Seq(
        ProjectionEvent.Checkpoint(20L, later)
      )
    }

    "skip checkpoints without synchronizer times" in {
      decode(checkpoint(20L)) shouldBe empty
    }
  }
}
