// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.ingestion

import com.daml.ledger.api.v2.event.{CreatedEvent, Event, ExercisedEvent}
import com.daml.ledger.api.v2.offset_checkpoint.{OffsetCheckpoint, SynchronizerTime}
import com.daml.ledger.api.v2.reassignment.{AssignedEvent, Reassignment, ReassignmentEvent}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, InterfaceFilter, TransactionShape}
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.ledger.api.v2.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.client.services.state.StateServiceClient
import com.digitalasset.canton.ledger.client.services.updates.UpdateServiceClient
import com.digitalasset.canton.platform.config.TopUpConfig
import com.digitalasset.canton.tea.AccountId
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TopUpNotCredited
import com.digitalasset.canton.tea.daml.java.canton.traffic.credit.{
  PublishCredit,
  TrafficCreditIssuer,
  AccountId as DamlAccountId,
}
import com.digitalasset.canton.tea.projection.{AccountDelta, EventId, ProjectionEvent, TrafficDelta}
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
  private val defaultConfig = TopUpConfig.DamlCredit(paymasters = NonEmpty(Set, paymaster))
  private val interfaceId =
    Identifier.fromJavaProto(TrafficCreditIssuer.INTERFACE_ID_WITH_PACKAGE_ID.toProto)
  private val issuerTemplateId = Identifier("b" * 64, "Wallet", "TrafficPurchaser")

  private def serviceWith(
      updates: Seq[GetUpdatesResponse] = Seq.empty,
      prunedUpTo: Long = 0L,
      topUpConfig: TopUpConfig.DamlCredit = defaultConfig,
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
      topUpConfig,
      loggerFactory,
    )
    (service, updateServiceClient, stateServiceClient)
  }

  private def text(value: String): Value = Value(Value.Sum.Text(value))
  private def numeric(value: String): Value = Value(Value.Sum.Numeric(value))
  private def int64(value: Long): Value = Value(Value.Sum.Int64(value))

  private def record(value: com.daml.ledger.javaapi.data.DamlRecord): Value =
    Value(Value.Sum.Record(Record.fromJavaProto(value.toProtoRecord)))

  private def published(
      nodeId: Int,
      account: String = "alice",
      amount: Long = 1L,
      paymasterParty: String = paymaster.toProtoPrimitive,
  ): ExercisedEvent =
    ExercisedEvent.defaultInstance.copy(
      nodeId = nodeId,
      contractId = s"issuer-$nodeId",
      templateId = Some(issuerTemplateId),
      interfaceId = Some(interfaceId),
      choice = TrafficCreditIssuer.CHOICE_PublishCredit.name,
      consuming = false,
      choiceArgument = Some(record(new PublishCredit(new DamlAccountId(account), amount).toValue)),
      actingParties = Seq(paymasterParty),
      witnessParties = Seq(paymasterParty),
      exerciseResult = Some(Value(Value.Sum.Unit(com.google.protobuf.empty.Empty()))),
      packageName = "test-wallet",
    )

  private def asEvent(exercised: ExercisedEvent): Event =
    Event(Event.Event.Exercised(exercised))

  private def topUp(nodeId: Int, account: String, amount: Long): Event =
    asEvent(published(nodeId, account, amount))

  private def withArgumentFields(
      exercised: ExercisedEvent,
      update: Seq[RecordField] => Seq[RecordField],
  ): ExercisedEvent = {
    val arguments = exercised.getChoiceArgument.getRecord
    exercised.copy(choiceArgument =
      Some(Value(Value.Sum.Record(arguments.copy(fields = update(arguments.fields)))))
    )
  }

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
    ProjectionEvent.TopUps(
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

    "credit the account with the amount of a PublishCredit exercise" in {
      decode(transaction(10L, topUp(nodeId = 3, "alice", 1000L))) shouldBe Seq(
        projectionEvent(10L, 3 -> credit(alice, 1000L))
      )
    }

    "read the credit from the choice argument" in {
      val exercised = published(nodeId = 3, amount = 1000L).copy(exerciseResult = None)
      decode(transaction(10L, asEvent(exercised))) shouldBe Seq(
        projectionEvent(10L, 3 -> credit(alice, 1000L))
      )
    }

    "ignore creates and other choices on an implementing template" in {
      val credit = published(nodeId = 0)
      val otherChoice = credit.copy(
        nodeId = 1,
        interfaceId = None,
        choice = "TrafficPurchaser_PurchaseCredits",
        choiceArgument = Some(Value(Value.Sum.Record(Record.defaultInstance))),
      )
      decode(
        transaction(
          10L,
          Event(
            Event.Event.Created(
              CreatedEvent.defaultInstance.copy(nodeId = 2, templateId = Some(issuerTemplateId))
            )
          ),
          asEvent(otherChoice),
          // A template choice that happens to have the same name as the interface choice
          asEvent(otherChoice.copy(nodeId = 3, choice = credit.choice)),
          asEvent(credit.copy(nodeId = 4, choice = "Archive", consuming = true)),
          asEvent(credit),
        )
      ) shouldBe Seq(projectionEvent(10L, 0 -> this.credit(alice, 1L)))
    }

    "ignore reassignment of an implementing contract" in {
      val reassignment = Reassignment.defaultInstance.copy(
        offset = 10L,
        events = Seq(
          ReassignmentEvent(
            ReassignmentEvent.Event.Assigned(
              AssignedEvent.defaultInstance.copy(createdEvent =
                Some(CreatedEvent.defaultInstance.copy(templateId = Some(issuerTemplateId)))
              )
            )
          )
        ),
      )
      decode(
        GetUpdatesResponse(GetUpdatesResponse.Update.Reassignment(reassignment))
      ) shouldBe empty
    }

    "decode arguments without record labels" in {
      val credit = withArgumentFields(published(nodeId = 3), _.map(_.copy(label = "")))
      decode(transaction(10L, asEvent(credit))) shouldBe Seq(
        projectionEvent(10L, 3 -> this.credit(alice, 1L))
      )
    }

    "emit one event per transaction with all its top-ups" in {
      decode(
        transaction(
          10L,
          topUp(nodeId = 0, "alice", 1L),
          topUp(nodeId = 1, "bob", 2L),
        ),
        transaction(11L, topUp(nodeId = 0, "alice", 3L)),
      ) shouldBe Seq(
        projectionEvent(10L, 0 -> credit(alice, 1L), 1 -> credit(bob, 2L)),
        projectionEvent(11L, 0 -> credit(alice, 3L)),
      )
    }

    "derive the event ids of top-ups from their node ids" in {
      val events = decode(
        transaction(
          10L,
          topUp(nodeId = 2, "alice", 1L),
          topUp(nodeId = 7, "alice", 1L),
        )
      )

      events.flatMap {
        case topUps: ProjectionEvent.TopUps =>
          topUps.deltasWithEventIds("credit-ingestion").map(_._2)
        case other => fail(s"expected top-ups, got $other")
      } should contain theSameElementsAs Seq(
        EventId.tryCreate("credit-ingestion-10-2"),
        EventId.tryCreate("credit-ingestion-10-7"),
      )
    }

    "skip invalid credits and keep the valid ones" in {
      def withArgument(
          credit: ExercisedEvent,
          index: Int,
          value: Option[Value],
      ): ExercisedEvent =
        withArgumentFields(
          credit,
          fields => fields.updated(index, fields(index).copy(value = value)),
        )
      val tooLongAccount = "a" * 256
      val credits = Seq(
        withArgument(published(nodeId = 0), 1, Some(numeric("1000.0"))),
        published(nodeId = 1, amount = 0L),
        published(nodeId = 2, amount = -3L),
        published(nodeId = 3, account = tooLongAccount),
        withArgument(published(nodeId = 4), 1, Some(text("10"))),
        published(nodeId = 5).copy(choiceArgument = Some(Value(Value.Sum.Record(Record())))),
        published(nodeId = 6).copy(choiceArgument = None),
        published(nodeId = 7, account = "bob", amount = 5L),
        published(nodeId = 8).copy(choiceArgument = Some(text("alice"))),
        published(nodeId = 9, account = ""),
        withArgument(published(nodeId = 10), 1, None),
        withArgument(published(nodeId = 11), 1, Some(Value.defaultInstance)),
        withArgument(published(nodeId = 12), 0, Some(int64(1L))),
        published(nodeId = 13, account = "\ud800\udc00" * 128),
        published(nodeId = 14, account = "\u00e9" * 256),
        published(nodeId = 15, account = "acme//billing"),
      )
      loggerFactory.assertLogs(
        decode(transaction(10L, credits.map(asEvent)*)) shouldBe Seq(
          projectionEvent(10L, 7 -> credit(bob, 5L))
        ),
        _.errorMessage should include(
          "node 0: cannot decode: Expected field to be of type com.daml.ledger.javaapi.data.Int64"
        ),
        _.errorMessage should include("node 1: traffic amount must be positive, got 0"),
        _.errorMessage should include("node 2: traffic amount must be positive, got -3"),
        _.errorMessage should include("node 3: invalid account id"),
        _.errorMessage should include(
          "node 4: cannot decode: Expected field to be of type com.daml.ledger.javaapi.data.Int64"
        ),
        _.errorMessage should include("node 5: cannot decode: Expected 2 fields, got 0"),
        _.errorMessage should include("node 6: missing choice argument"),
        _.errorMessage should include(
          "node 8: cannot decode: Contracts must be constructed from Records"
        ),
        _.errorMessage should include("node 9: account id must not be empty"),
        _.errorMessage should include("node 10: cannot decode: Sum not set for value"),
        _.errorMessage should include("node 11: cannot decode: Sum not set for value"),
        _.errorMessage should include(
          "node 12: cannot decode: Contracts must be constructed from Records"
        ),
        _.errorMessage should include("node 13: invalid account id"),
        _.errorMessage should include("node 14: invalid account id"),
        _.errorMessage should include("node 15: invalid account id"),
      )
    }

    "reject credits without exactly one acting party as invalid" in {
      val credit = published(nodeId = 0)
      loggerFactory.assertLogs(
        decode(
          transaction(
            10L,
            asEvent(credit.copy(actingParties = Seq.empty)),
            asEvent(
              credit.copy(
                nodeId = 1,
                actingParties = Seq(paymaster.toProtoPrimitive, "paymaster::other"),
              )
            ),
          )
        ) shouldBe empty,
        _.errorMessage should include("node 0: expected exactly one acting party, got "),
        _.errorMessage should include(
          s"node 1: expected exactly one acting party, got ${paymaster.toProtoPrimitive}, paymaster::other"
        ),
      )
    }

    "skip credits from unconfigured paymasters, even when a trusted party is a witness" in {
      val untrustedCredit = published(nodeId = 0, paymasterParty = "intruder::x").copy(
        witnessParties = Seq("intruder::x", paymaster.toProtoPrimitive)
      )
      loggerFactory.assertLogs(
        decode(
          transaction(
            10L,
            asEvent(untrustedCredit),
            topUp(nodeId = 1, "bob", 2L),
          )
        ) shouldBe Seq(projectionEvent(10L, 1 -> credit(bob, 2L))),
        _.warningMessage should include(
          "node 0: paymaster intruder::x is not one of the configured top-up paymasters"
        ),
      )
    }

    "emit no event for a transaction without valid top-ups" in {
      loggerFactory.assertLogs(
        decode(transaction(10L, topUp(nodeId = 0, "alice", 0L))) shouldBe empty,
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

    "subscribe to TrafficCreditIssuer by package name for the paymasters with non-verbose ledger effects" in {
      val otherPaymaster = PartyId.tryFromProtoPrimitive("paymaster::other")
      val (service, updateServiceClient, _) = serviceWith(topUpConfig =
        TopUpConfig.DamlCredit(paymasters = NonEmpty(Set, paymaster, otherPaymaster))
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
      format.verbose shouldBe false
      format.filtersForAnyParty shouldBe empty
      format.filtersByParty.keySet shouldBe Set(paymaster, otherPaymaster).map(_.toProtoPrimitive)
      forAll(format.filtersByParty.values) { filters =>
        filters.cumulative.map(_.identifierFilter) shouldBe Seq(
          IdentifierFilter.InterfaceFilter(
            InterfaceFilter(
              Some(
                Identifier("#canton-traffic-credit", "Canton.Traffic.Credit", "TrafficCreditIssuer")
              ),
              includeInterfaceView = false,
              includeCreatedEventBlob = false,
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
        topUpConfig = defaultConfig.copy(initialOffsetBeginExclusive = Some(31380L)),
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
