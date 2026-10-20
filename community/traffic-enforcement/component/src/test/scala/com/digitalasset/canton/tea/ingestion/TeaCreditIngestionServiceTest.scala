// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.ingestion

import com.daml.ledger.api.v2.event.{CreatedEvent, Event, ExercisedEvent}
import com.daml.ledger.api.v2.offset_checkpoint.{OffsetCheckpoint, SynchronizerTime}
import com.daml.ledger.api.v2.reassignment.{AssignedEvent, Reassignment, ReassignmentEvent}
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
import com.digitalasset.canton.platform.config.TopUpConfig
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TopUpNotCredited
import com.digitalasset.canton.tea.daml.java.canton.traffic.credit.{
  TrafficCredit,
  AccountId as DamlAccountId,
}
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
  private val defaultConfig = TopUpConfig.DamlCredit(paymasters = NonEmpty(Set, paymaster))
  private val templateId =
    Identifier.fromJavaProto(TrafficCredit.TEMPLATE_ID_WITH_PACKAGE_ID.toProto)

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

  private def created(
      nodeId: Int,
      account: String = "alice",
      amount: Long = 1L,
      paymasterParty: String = paymaster.toProtoPrimitive,
  ): CreatedEvent =
    CreatedEvent.defaultInstance.copy(
      nodeId = nodeId,
      contractId = s"credit-$nodeId",
      templateId = Some(templateId),
      createArguments = Some(
        Record.fromJavaProto(
          new TrafficCredit(
            paymasterParty,
            new DamlAccountId(account),
            amount,
          ).toValue.toProtoRecord
        )
      ),
      signatories = Seq(paymasterParty),
      witnessParties = Seq(paymasterParty),
      packageName = TrafficCredit.PACKAGE_NAME,
    )

  private def asEvent(credit: CreatedEvent): Event =
    Event(Event.Event.Created(credit))

  private def archive(
      credit: CreatedEvent,
      nodeId: Int,
  ): Event =
    Event(
      Event.Event.Exercised(
        ExercisedEvent.defaultInstance.copy(
          nodeId = nodeId,
          contractId = credit.contractId,
          templateId = credit.templateId,
          choice = TrafficCredit.CHOICE_Archive.name,
          consuming = true,
          actingParties = credit.signatories,
          packageName = credit.packageName,
        )
      )
    )

  private def topUp(nodeId: Int, account: String, amount: Long): Event =
    asEvent(created(nodeId, account, amount))

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

    "credit the account with the amount of a TrafficCredit creation" in {
      decode(transaction(10L, topUp(nodeId = 3, "alice", 1000L))) shouldBe Seq(
        projectionEvent(10L, 3 -> credit(alice, 1000L))
      )
    }

    "credit a contract created and archived in the same transaction exactly once" in {
      val credit = created(nodeId = 3, amount = 1000L)
      decode(transaction(10L, asEvent(credit), archive(credit, nodeId = 7))) shouldBe Seq(
        projectionEvent(10L, 3 -> this.credit(alice, 1000L))
      )
    }

    "ignore archives without credit creations" in {
      decode(transaction(10L, archive(created(nodeId = 3), nodeId = 7))) shouldBe empty
    }

    "ignore reassignment of an existing credit" in {
      val reassignment = Reassignment.defaultInstance.copy(
        offset = 10L,
        events = Seq(
          ReassignmentEvent(
            ReassignmentEvent.Event.Assigned(
              AssignedEvent.defaultInstance.copy(createdEvent = Some(created(nodeId = 3)))
            )
          )
        ),
      )
      decode(
        GetUpdatesResponse(GetUpdatesResponse.Update.Reassignment(reassignment))
      ) shouldBe empty
    }

    "decode arguments without record labels" in {
      val credit = created(nodeId = 3)
      val arguments = credit.getCreateArguments
      decode(
        transaction(
          10L,
          asEvent(
            credit.copy(createArguments =
              Some(
                arguments.copy(recordId = None, fields = arguments.fields.map(_.copy(label = "")))
              )
            )
          ),
        )
      ) shouldBe Seq(projectionEvent(10L, 3 -> this.credit(alice, 1L)))
    }

    "warn about another package id for the same package name and template and keep matching credits" in {
      val credit = created(nodeId = 3).copy(
        templateId = Some(templateId.copy(packageId = "a" * 64))
      )
      loggerFactory.assertLogs(
        decode(transaction(10L, asEvent(credit), topUp(nodeId = 4, "bob", 2L))) shouldBe Seq(
          projectionEvent(10L, 4 -> this.credit(bob, 2L))
        ),
        _.warningMessage should include(
          s"node 3 uses package id ${"a" * 64}, which is not an approved package id"
        ),
      )
    }

    "warn about another package id even with compatible trailing empty optional fields" in {
      val credit = created(nodeId = 3)
      val arguments = credit.getCreateArguments
      val upgraded = credit.copy(
        templateId = Some(templateId.copy(packageId = "a" * 64)),
        createArguments = Some(
          arguments.copy(fields =
            arguments.fields :+ RecordField(
              "futureField",
              Some(Value(Value.Sum.Optional(DamlOptional(None)))),
            )
          )
        ),
      )
      loggerFactory.assertLogs(
        decode(transaction(10L, asEvent(upgraded))) shouldBe empty,
        _.warningMessage should include("which is not an approved package id"),
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

      events.flatMap(event =>
        event.deltasWithEventIds("credit-ingestion").map(_._2)
      ) should contain theSameElementsAs Seq(
        EventId.tryCreate("credit-ingestion-10-2"),
        EventId.tryCreate("credit-ingestion-10-7"),
      )
    }

    "ignore exercise results, even if they contain credit-shaped data" in {
      val result = Value(Value.Sum.Record(created(nodeId = 0).getCreateArguments))
      decode(
        transaction(
          10L,
          Event(
            Event.Event.Exercised(
              ExercisedEvent.defaultInstance.copy(
                nodeId = 0,
                templateId = Some(templateId),
                packageName = TrafficCredit.PACKAGE_NAME,
                choice = "TrafficPurchaser_PurchaseCredits",
                exerciseResult = Some(result),
              )
            )
          ),
        )
      ) shouldBe empty
    }

    "skip invalid credits and keep the valid ones" in {
      def withArgument(
          credit: CreatedEvent,
          index: Int,
          value: Option[Value],
      ): CreatedEvent = {
        val arguments = credit.getCreateArguments
        credit.copy(createArguments =
          Some(
            arguments.copy(fields =
              arguments.fields.updated(index, arguments.fields(index).copy(value = value))
            )
          )
        )
      }
      val tooLongAccount = "a" * 256
      val credits = Seq(
        withArgument(created(nodeId = 0), 2, Some(numeric("1000.0"))),
        created(nodeId = 1, amount = 0L),
        created(nodeId = 2, amount = -3L),
        created(nodeId = 3, account = tooLongAccount),
        withArgument(created(nodeId = 4), 2, Some(text("10"))),
        created(nodeId = 5).copy(createArguments = Some(Record.defaultInstance)),
        created(nodeId = 6).copy(createArguments = None),
        created(nodeId = 7, account = "bob", amount = 5L),
        withArgument(created(nodeId = 8), 0, Some(text(paymaster.toProtoPrimitive))),
        created(nodeId = 9, account = ""),
        withArgument(created(nodeId = 10), 2, None),
        withArgument(created(nodeId = 11), 2, Some(Value.defaultInstance)),
        withArgument(created(nodeId = 12), 1, Some(int64(1L))),
        created(nodeId = 13, account = "\ud800\udc00" * 128),
        created(nodeId = 14, account = "\u00e9" * 256),
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
        _.errorMessage should include("node 5: cannot decode: Expected 3 fields, got 0"),
        _.errorMessage should include("node 6: cannot decode: Expected 3 fields, got 0"),
        _.errorMessage should include(
          "node 8: cannot decode: Expected field to be of type com.daml.ledger.javaapi.data.Party"
        ),
        _.errorMessage should include("node 9: account id must not be empty"),
        _.errorMessage should include("node 10: cannot decode: Sum not set for value"),
        _.errorMessage should include("node 11: cannot decode: Sum not set for value"),
        _.errorMessage should include(
          "node 12: cannot decode: Contracts must be constructed from Records"
        ),
        _.errorMessage should include("node 13: invalid account id"),
        _.errorMessage should include("node 14: invalid account id"),
      )
    }

    "skip credits from unconfigured paymasters, even when a trusted party is an observer and witness" in {
      val untrustedCredit = created(nodeId = 0, paymasterParty = "intruder::x").copy(
        observers = Seq(paymaster.toProtoPrimitive),
        witnessParties = Seq(paymaster.toProtoPrimitive),
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

    "subscribe to TrafficCredit by package name for the paymasters with non-verbose ledger effects" in {
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
        val templateIds = filters.cumulative
          .map(_.identifierFilter)
          .collect { case IdentifierFilter.TemplateFilter(filter) => filter.templateId }
        templateIds shouldBe Seq(
          Some(templateId.withPackageId(s"#${TrafficCredit.PACKAGE_NAME}"))
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
