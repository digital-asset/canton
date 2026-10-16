// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.ingestion

import cats.syntax.either.*
import com.daml.ledger.api.v2.event.ExercisedEvent
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
}
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse.Update
import com.daml.ledger.api.v2.value.{Identifier, Record, Value}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.client.services.state.StateServiceClient
import com.digitalasset.canton.ledger.client.services.updates.UpdateServiceClient
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig.ProjectionConfig
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TopUpNotCredited
import com.digitalasset.canton.tea.ingestion.TrafficPurchaseModel.{ResultFields, Template}
import com.digitalasset.canton.tea.projection.{
  AccountDelta,
  AccountId,
  ProjectionEvent,
  TrafficDelta,
}
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.fromDamlProtoSafeOpt
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.nonempty.NonEmpty
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Builds the credit delta stream from the results of the top-up choices exercised on the ledger.
  *
  * Restart and offset-resume are handled by the Pekko projection that consumes [[grpcSource]].
  */
final class TeaCreditIngestionService(
    updateServiceClient: UpdateServiceClient,
    stateServiceClient: StateServiceClient,
    projectionConfig: ProjectionConfig,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  import TeaCreditIngestionService.*

  private val templateId = Identifier(
    packageId = s"#${Template.PackageName}",
    moduleName = Template.ModuleName,
    entityName = Template.TemplateName,
  )

  private val filter = CumulativeFilter(
    IdentifierFilter.TemplateFilter(
      TemplateFilter(Some(templateId), includeCreatedEventBlob = false)
    )
  )

  // Convert the paymasters to the primitive representation used in the ledger API filter
  private val paymasters = projectionConfig.topUpPaymasters.map(_.toProtoPrimitive)

  private val eventFormat = EventFormat(
    filtersByParty = paymasters.map(_ -> Filters(Seq(filter))).toMap,
    filtersForAnyParty = None,
    // Record labels are needed to decode the choice result by field name
    verbose = true,
  )

  /** Offset-resume aware source factory: builds the credit delta source starting from the offset
    * persisted by the projection, or from the configured offset or the participant pruned up to
    * offset.
    */
  def grpcSource(beginOffset: Option[Long]): Source[Traced[ProjectionEvent], NotUsed] =
    TraceContext.withNewTraceContext("tea-credit-ingestion") { implicit tc =>
      val offsetF = beginOffset
        .map(Future.successful)
        .orElse(projectionConfig.initialTopUpOffsetBeginExclusive.map(Future.successful))
        // Without a known offset or specific config, we start from the current participant
        // pruned up to offset, which is as far back as we can go.
        .getOrElse(stateServiceClient.getParticipantPrunedUpToOffset())

      Source
        .future(offsetF)
        .flatMap { offset =>
          logger.info(s"(Re)connecting top-up update stream at offset $offset")
          updateServiceClient
            .getUpdatesSource(
              begin = offset,
              eventFormat = eventFormat,
              transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
            )
            .mapConcat(decodeUpdate)
        }
    }

  private[ingestion] def decodeUpdate(
      response: GetUpdatesResponse
  ): Option[Traced[ProjectionEvent]] = response.update match {
    case Update.Transaction(tx) =>
      implicit val tc: TraceContext =
        fromDamlProtoSafeOpt(noTracingLogger)(tx.traceContext).traceContext
      decodeTransaction(tx)

    case Update.OffsetCheckpoint(checkpoint) =>
      implicit val tc: TraceContext = TraceContext.empty
      logger.debug(s"Received a checkpoint at offset ${checkpoint.offset}")
      checkpoint.synchronizerTimes
        .flatMap(_.recordTime)
        .flatMap(CantonTimestamp.fromProtoTimestamp(_).toOption)
        .maxOption
        .map(ts => Traced(ProjectionEvent.Checkpoint(checkpoint.offset, ts)))

    case Update.Empty | _: Update.Reassignment | _: Update.TopologyTransaction =>
      None
  }

  // TODO(#36625) Might need to rewrite this if we move the Daml contract to the canton repo.
  private def decodeTransaction(
      tx: Transaction
  )(implicit traceContext: TraceContext): Option[Traced[ProjectionEvent]] = {
    val topUps = decodeTopUps(tx, paymasters).flatMap {
      case Right(topUp) => Some(topUp)
      case Left(UntrustedPaymaster(message)) =>
        logger.warn(
          s"Skipping top-up in transaction ${tx.updateId} at offset ${tx.offset}: $message"
        )
        None
      case Left(InvalidResult(message)) =>
        TopUpNotCredited.Reject(tx.updateId, tx.offset, message).discard
        None
    }

    NonEmpty.from(topUps).map { nonEmptyTopUps =>
      val timestamp = tx.recordTime
        .toRight("missing recordTime")
        .flatMap(CantonTimestamp.fromProtoTimestamp(_).leftMap(_.toString))
        .valueOr(err =>
          throw new IllegalStateException(s"Invalid transaction ${tx.updateId}: $err")
        )
      Traced(
        ProjectionEvent.Multiple(
          offset = tx.offset,
          timestamp = timestamp,
          deltas = nonEmptyTopUps
            .map(topUp =>
              topUp.nodeId ->
                AccountDelta(
                  account = topUp.account,
                  delta = TrafficDelta.creditBalanceDelta(topUp.amount),
                )
            )
            .toMap,
        )
      )
    }
  }
}

object TeaCreditIngestionService {

  private final case class TopUp(nodeId: Int, account: AccountId, amount: Long)

  /** Why a top-up choice is not credited. */
  private sealed trait Rejection extends Product with Serializable

  /** The top-up was made on a contract of a paymaster that is not configured, so it is not for this
    * TEA to credit.
    */
  private final case class UntrustedPaymaster(message: String) extends Rejection

  /** The result does not have the expected shape: TEA and the Daml model disagree, and a top-up
    * that may have been paid for is not credited.
    */
  private final case class InvalidResult(message: String) extends Rejection

  /** Decodes the top-ups of a transaction. Exercised events that are not one of the configured
    * top-up choices are ignored, while top-up choices with an unexpected result or a paymaster that
    * is not one of `paymasters` yield a `Left`.
    */
  private def decodeTopUps(
      tx: Transaction,
      paymasters: Set[String],
  ): Seq[Either[Rejection, TopUp]] =
    tx.events
      .flatMap(_.event.exercised)
      .filter(isTopUpChoice)
      .map(decodeTopUp(_, paymasters))

  private def isTopUpChoice(exercised: ExercisedEvent): Boolean =
    Template.ChoiceName == exercised.choice

  private def decodeTopUp(
      exercised: ExercisedEvent,
      paymasters: Set[String],
  ): Either[Rejection, TopUp] = {
    def describe(err: String): String =
      s"choice ${exercised.choice} at node ${exercised.nodeId}: $err"
    def invalid(err: String): Rejection = InvalidResult(describe(err))

    for {
      result <- exercised.exerciseResult
        .toRight("missing exercise result")
        .flatMap(asRecord)
        .leftMap(invalid)
      paymaster <- field(result, ResultFields.PaymasterField).flatMap(asParty).leftMap(invalid)
      _ <- Either.cond(
        paymasters.contains(paymaster),
        (),
        UntrustedPaymaster(
          describe(s"paymaster $paymaster is not one of the configured top-up paymasters")
        ),
      )
      topUp <- decodeCredit(exercised.nodeId, result).leftMap(invalid)
    } yield topUp
  }

  private def decodeCredit(nodeId: Int, result: Record): Either[String, TopUp] =
    for {
      targetAccount <- field(result, ResultFields.TargetAccountField).flatMap(asRecord)
      accountIdText <- field(targetAccount, ResultFields.AccountIdField).flatMap(asText)
      account <- AccountId.create(accountIdText).leftMap(err => s"invalid account id: $err")
      amount <- field(result, ResultFields.TrafficAmountField).flatMap(asTrafficAmount)
    } yield TopUp(nodeId, account, amount)

  private def field(record: Record, label: String): Either[String, Value] =
    record.fields
      .find(_.label == label)
      .flatMap(_.value)
      .toRight(s"missing field '$label'")

  private def asRecord(value: Value): Either[String, Record] = value.sum match {
    case Value.Sum.Record(record) => Right(record)
    case other => Left(s"expected a record, got $other")
  }

  private def asParty(value: Value): Either[String, String] = value.sum match {
    case Value.Sum.Party(party) => Right(party)
    case other => Left(s"expected a party, got $other")
  }

  private def asText(value: Value): Either[String, String] = value.sum match {
    case Value.Sum.Text(text) => Right(text)
    case other => Left(s"expected a text, got $other")
  }

  private def asTrafficAmount(value: Value): Either[String, Long] = value.sum match {
    case Value.Sum.Int64(amount) =>
      Either.cond(amount > 0, amount, s"traffic amount must be positive, got $amount")
    case other => Left(s"expected an Int64 traffic amount, got $other")
  }
}
