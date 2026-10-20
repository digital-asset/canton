// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.ingestion

import cats.syntax.either.*
import com.daml.ledger.api.v2.event.CreatedEvent
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
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi.data.CreatedEvent as JavaCreatedEvent
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.client.services.state.StateServiceClient
import com.digitalasset.canton.ledger.client.services.updates.UpdateServiceClient
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.config.TopUpConfig
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TopUpNotCredited
import com.digitalasset.canton.tea.daml.java.canton.traffic.credit.TrafficCredit
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

/** Builds the credit delta stream from TrafficCredit creations on the ledger, including contracts
  * created and archived in the same transaction.
  *
  * Restart and offset-resume are handled by the Pekko projection that consumes [[grpcSource]].
  */
final class TeaCreditIngestionService(
    updateServiceClient: UpdateServiceClient,
    stateServiceClient: StateServiceClient,
    topUpConfig: TopUpConfig.DamlCredit,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  import TeaCreditIngestionService.*

  private val filter = CumulativeFilter(
    IdentifierFilter.TemplateFilter(
      TemplateFilter(
        Some(templateId.withPackageId(s"#${TrafficCredit.PACKAGE_NAME}")),
        includeCreatedEventBlob = false,
      )
    )
  )

  // Convert the paymasters to the primitive representation used in the ledger API filter
  private val paymasters: Set[String] = topUpConfig.paymasters.forgetNE.map(_.toProtoPrimitive)

  private val eventFormat = EventFormat(
    filtersByParty = paymasters.map(_ -> Filters(Seq(filter))).toMap,
    filtersForAnyParty = None,
    verbose = false,
  )

  /** Offset-resume aware source factory: builds the credit delta source starting from the offset
    * persisted by the projection, or from the configured offset or the participant pruned up to
    * offset.
    */
  def grpcSource(beginOffset: Option[Long]): Source[Traced[ProjectionEvent], NotUsed] =
    TraceContext.withNewTraceContext("tea-credit-ingestion") { implicit tc =>
      val offsetF = beginOffset
        .map(Future.successful)
        .orElse(topUpConfig.initialOffsetBeginExclusive.map(Future.successful))
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
              // ACS deltas omit credits created and archived atomically by creditTraffic.
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
      case Left(UnapprovedPackageId(message)) =>
        logger.warn(
          s"Skipping top-up in transaction ${tx.updateId} at offset ${tx.offset}: $message"
        )
        None
      case Left(InvalidCredit(message)) =>
        // This prints an ERROR to the log
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

  /** Classpath resource of the canton-traffic-credit DAR that the generated bindings come from. */
  val DarResourceFileName: String =
    s"dar/${TrafficCredit.PACKAGE_NAME}-${TrafficCredit.PACKAGE_VERSION}.dar"

  private val templateId =
    Identifier.fromJavaProto(TrafficCredit.TEMPLATE_ID_WITH_PACKAGE_ID.toProto)

  private final case class TopUp(nodeId: Int, account: AccountId, amount: Long)

  /** Why a TrafficCredit creation is not credited. */
  private sealed trait Rejection extends Product with Serializable

  /** The top-up was made on a contract of a paymaster that is not configured, so it is not for this
    * TEA to credit.
    */
  private final case class UntrustedPaymaster(message: String) extends Rejection

  /** The credit was created from a version of the canton-traffic-credit package that this TEA does
    * not accept.
    */
  private final case class UnapprovedPackageId(message: String) extends Rejection

  /** The credit does not have the expected shape or signatories: TEA and the Daml model disagree,
    * and a top-up that may have been paid for is not credited.
    */
  private final case class InvalidCredit(message: String) extends Rejection

  /** Decodes TrafficCredit creations. Other events are ignored, while credits from an unapproved
    * package id, invalid credits, or credits signed by a paymaster that is not one of `paymasters`
    * yield a `Left`.
    */
  private def decodeTopUps(
      tx: Transaction,
      paymasters: Set[String],
  ): Seq[Either[Rejection, TopUp]] =
    // Events are already filtered down to template package when setting up subscription
    tx.events
      .flatMap(_.event.created)
      .map { created =>
        val packageId = created.templateId.fold("")(_.packageId)
        Either
          .cond(
            packageId == templateId.packageId,
            created,
            UnapprovedPackageId(
              s"TrafficCredit creation at node ${created.nodeId} uses package id $packageId, " +
                s"which is not an approved package id (expected ${templateId.packageId})"
            ),
          )
          .flatMap(decodeTopUp(_, paymasters))
      }

  private def decodeTopUp(
      created: CreatedEvent,
      paymasters: Set[String],
  ): Either[Rejection, TopUp] = {
    def describe(err: String): String =
      s"TrafficCredit creation at node ${created.nodeId}: $err"
    def invalid(err: String): Rejection = InvalidCredit(describe(err))

    for {
      credit <- Either
        .catchNonFatal(
          TrafficCredit.COMPANION
            .fromCreatedEvent(JavaCreatedEvent.fromProto(CreatedEvent.toJavaProto(created)))
            .data
        )
        .leftMap(err => invalid(s"cannot decode: ${err.getMessage}"))
      _ <- Either.cond(
        created.signatories.contains(credit.paymaster),
        (),
        invalid(s"paymaster '${credit.paymaster}' is not in signatories"),
      )
      _ <- Either.cond(
        paymasters.contains(credit.paymaster),
        (),
        UntrustedPaymaster(
          describe(s"paymaster ${credit.paymaster} is not one of the configured top-up paymasters")
        ),
      )
      _ <- Either.cond(credit.accountId.value.nonEmpty, (), invalid("account id must not be empty"))
      account <- AccountId
        .create(credit.accountId.value)
        .leftMap(err => invalid(s"invalid account id: $err"))
      _ <- Either.cond(
        credit.amount > 0,
        (),
        invalid(s"traffic amount must be positive, got ${credit.amount}"),
      )
    } yield TopUp(created.nodeId, account, credit.amount)
  }
}
