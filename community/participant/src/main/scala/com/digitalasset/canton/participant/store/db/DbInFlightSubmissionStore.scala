// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{EitherT, OptionT}
import cats.syntax.alternative._
import cats.syntax.option._
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.protocol.submission._
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.{
  InFlightByMessageId,
  InFlightBySequencingInfo,
  InFlightReference,
}
import com.digitalasset.canton.participant.store.{InFlightSubmissionStore, SerializableSubmissionId}
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.DbStorage.DbAction.ReadOnly
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.store.db.DbBulkUpdateProcessor.BulkUpdatePendingCheck
import com.digitalasset.canton.store.db.{DbBulkUpdateProcessor, DbSerializationException}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util.retry.RetryUtil.NoExnRetryable
import com.digitalasset.canton.util.{BatchAggregator, ErrorUtil, OptionUtil, SingleUseCell, retry}
import com.digitalasset.canton.version.ProtocolVersion
import io.functionmeta.functionFullName
import slick.jdbc.SetParameter

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class DbInFlightSubmissionStore(
    override protected val storage: DbStorage,
    maxItemsInSqlInClause: PositiveNumeric[Int],
    registerBatchAggregatorConfig: BatchAggregatorConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends InFlightSubmissionStore
    with DbStore {

  import storage.api._
  import storage.converters._

  private val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("in-flight-submission-store")

  private implicit val setParameterSubmissionTrackingData: SetParameter[SubmissionTrackingData] =
    SubmissionTrackingData.getVersionedSetParameter(ProtocolVersion.v2Todo_i8793)

  override def lookup(changeIdHash: ChangeIdHash)(implicit
      traceContext: TraceContext
  ): OptionT[Future, InFlightSubmission[SubmissionSequencingInfo]] =
    OptionT(processingTime.metric.event {
      storage.query(lookupQuery(changeIdHash), "lookup in-flight submission")
    })

  override def lookupUnsequencedUptoUnordered(
      domainId: DomainId,
      observedSequencingTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Seq[InFlightSubmission[UnsequencedSubmission]]] =
    processingTime.metric.event {
      val query =
        sql"""
        select change_id_hash, submission_id, submission_domain, message_id, sequencing_timeout, tracking_data, trace_context
        from in_flight_submission where submission_domain = $domainId and sequencing_timeout <= $observedSequencingTime
        """.as[InFlightSubmission[UnsequencedSubmission]]
      storage.query(query, "lookup unsequenced in-flight submission")
    }

  override def lookupSequencedUptoUnordered(
      domainId: DomainId,
      sequencingTimeInclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Seq[InFlightSubmission[SequencedSubmission]]] =
    processingTime.metric.event {
      val query =
        sql"""
        select change_id_hash, submission_id, submission_domain, message_id, sequencer_counter, sequencing_time, trace_context
        from in_flight_submission where submission_domain = $domainId and sequencing_time <= $sequencingTimeInclusive
        """.as[InFlightSubmission[SequencedSubmission]]
      storage.query(query, "lookup sequenced in-flight submission")
    }

  override def lookupSomeMessageId(domainId: DomainId, messageId: MessageId)(implicit
      traceContext: TraceContext
  ): Future[Option[InFlightSubmission[SubmissionSequencingInfo]]] =
    processingTime.metric.event {
      val query =
        sql"""
        select change_id_hash, submission_id, submission_domain, message_id, sequencing_timeout, sequencer_counter, sequencing_time, tracking_data, trace_context
        from in_flight_submission where submission_domain = $domainId and message_id = $messageId
        #${storage.limit(1)}
        """.as[InFlightSubmission[SubmissionSequencingInfo]].headOption
      storage.query(query, "lookup in-flight submission by message id")
    }

  override def lookupEarliest(
      domainId: DomainId
  )(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]] =
    processingTime.metric.event {
      val query =
        sql"""
        select min(sequencing_time), min(sequencing_timeout)
        from in_flight_submission where submission_domain = $domainId
        """.as[(Option[CantonTimestamp], Option[CantonTimestamp])].headOption
      storage
        .query(query, "lookup earliest in-flight submission")
        .map(_.flatMap { case (earliestTimeout, earliestSequencing) =>
          OptionUtil.mergeWith(earliestTimeout, earliestSequencing)(Ordering[CantonTimestamp].min)
        })
    }

  override def register(
      submission: InFlightSubmission[UnsequencedSubmission]
  ): EitherT[FutureUnlessShutdown, InFlightSubmission[SubmissionSequencingInfo], Unit] = EitherT {
    implicit val traceContext: TraceContext = submission.submissionTraceContext

    def failOnNone[A](x: Option[A]): A = x.getOrElse {
      ErrorUtil.internalError(
        new IllegalStateException(s"Retry stopped early for submission $submission")
      )
    }

    FutureUnlessShutdown(batchAggregatorRegister.run(submission).flatMap(Future.fromTry))
      .map(failOnNone)
  }

  private val batchAggregatorRegister = {
    val processor =
      new DbInFlightSubmissionStore.RegisterProcessor(
        storage,
        maxItemsInSqlInClause,
        logger,
      )
    BatchAggregator(processor, registerBatchAggregatorConfig, processingTime.some)
  }

  override def observeSequencing(
      domainId: DomainId,
      submissions: Map[MessageId, SequencedSubmission],
  )(implicit traceContext: TraceContext): Future[Unit] = processingTime.metric.event {
    val updateQuery =
      """update in_flight_submission
         set sequencing_timeout = null, tracking_data = null, sequencer_counter = ?, sequencing_time = ?
         where submission_domain = ? and message_id = ? and sequencing_timeout is not null
      """
    val batchUpdate = DbStorage.bulkOperation_(updateQuery, submissions.toSeq, storage.profile) {
      pp => submission =>
        val (messageId, SequencedSubmission(sequencerCounter, sequencingTime)) = submission
        pp >> sequencerCounter
        pp >> sequencingTime
        pp >> domainId
        pp >> messageId
    }
    // No need for synchronous commit because this method is driven by the event stream from the sequencer,
    // which is the same across all replicas of the participant
    storage.queryAndUpdate(batchUpdate, "observe sequencing")
  }

  override def delete(
      submissions: Seq[InFlightReference]
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.metric.event {
      val (byId, bySequencing) = submissions.toList.map(_.toEither).separate

      val byIdQuery =
        "delete from in_flight_submission where submission_domain = ? and message_id = ?"
      val batchById = DbStorage.bulkOperation_(byIdQuery, byId, storage.profile) {
        pp => submission =>
          val InFlightByMessageId(domainId, messageId) = submission
          pp >> domainId
          pp >> messageId
      }

      val bySequencingQuery =
        "delete from in_flight_submission where submission_domain = ? and sequencing_time = ? and sequencer_counter = ?"
      val batchBySequencing =
        DbStorage.bulkOperation_(bySequencingQuery, bySequencing, storage.profile) {
          pp => submission =>
            val InFlightBySequencingInfo(domainId, sequenced) = submission
            pp >> domainId
            pp >> sequenced.sequencingTime
            pp >> sequenced.sequencerCounter
        }

      // No need for synchronous commits across DB replicas because this is driven off the multi-domain event log,
      // which itself uses synchronous commits and therefore ensures synchronization among `delete`s.
      // For the interaction with `register`, it is enough that `register` uses synchronous commits
      // as a synchronous commit ensures that all earlier commits in the WAL such as the delete
      // have also reached the DB replica.
      for {
        _ <- storage.queryAndUpdate(batchById, "delete submission by message id")
        _ <- storage.queryAndUpdate(batchBySequencing, "delete sequenced submission")
      } yield ()
    }

  override def updateUnsequenced(
      changeIdHash: ChangeIdHash,
      submissionDomain: DomainId,
      messageId: MessageId,
      newSequencingInfo: UnsequencedSubmission,
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.metric.event {
      val updateQuery =
        sqlu"""
          update in_flight_submission
          set sequencing_timeout = ${newSequencingInfo.timeout}, tracking_data = ${newSequencingInfo.trackingData}
          where change_id_hash = $changeIdHash and submission_domain = $submissionDomain and message_id = $messageId
            and sequencing_timeout >= ${newSequencingInfo.timeout}
          """
      // No need for synchronous commit here because this method is called only from the submission phase
      // after registration, so a fail-over participant would not call this method anyway.
      // The registered submission would simply time out in such a case.
      storage.update(updateQuery, functionFullName).flatMap {
        case 1 =>
          logger.debug(
            show"Updated unsequenced submission (change ID hash $changeIdHash, message ID $messageId) on $submissionDomain to $newSequencingInfo. "
          )
          Future.unit
        case 0 =>
          // No update is reported. Let's see whether this is due to retries or races.
          lookup(changeIdHash).fold {
            // This case can happen if the max-sequencing time has already elapsed and the submission was already timely rejected.
            logger.info(
              s"Cannot update nonexistent submission (change ID hash $changeIdHash, message Id $messageId) on $submissionDomain.\nThis is OK if the submission has already timed out."
            )
          } { inFlight =>
            if (inFlight.sequencingInfo == newSequencingInfo) {
              // This can happen with underreporting or during crash recovery replay / reprocessing.
              logger.debug(
                show"Looked and found unsequenced submission (change ID hash $changeIdHash, message Id $messageId) on $submissionDomain with $newSequencingInfo"
              )
            } else
              inFlight.sequencingInfo.asUnsequenced match {
                case None =>
                  logger.warn(
                    show"Submission (change ID hash $changeIdHash, message Id $messageId) on $submissionDomain has already been sequenced. ${inFlight.sequencingInfo}"
                  )
                case Some(unsequenced) =>
                  if (unsequenced.timeout < newSequencingInfo.timeout) {
                    logger.warn(
                      show"Sequencing timeout for submission (change ID hash $changeIdHash, message Id $messageId on $submissionDomain) is at ${unsequenced.timeout} before ${newSequencingInfo.timeout}. Current data: ${unsequenced}"
                    )
                  } else {
                    // This should happen only if there are concurrent updates of unsequenced submissions.
                    // While the InFlightSubmissionTracker should be able to handle such a situation,
                    // it should not happen in practice.
                    logger.warn(
                      show"Failed to update unsequenced submission (change ID hash $changeIdHash, message Id $messageId) on $submissionDomain to $newSequencingInfo. Current data: ${inFlight.sequencingInfo}"
                    )
                  }
              }
          }
        case rowCount =>
          ErrorUtil.internalErrorAsync(
            new DbSerializationException(
              show"Failed to update unsequenced submission (change ID hash $changeIdHash, message Id $messageId) on $submissionDomain. Row count: $rowCount"
            )
          )
      }
    }

  private def lookupQuery(
      changeIdHash: ChangeIdHash
  ): DbAction.ReadTransactional[Option[InFlightSubmission[SubmissionSequencingInfo]]] =
    sql"""
        select change_id_hash, submission_id, submission_domain, message_id, sequencing_timeout, sequencer_counter, sequencing_time, tracking_data, trace_context
        from in_flight_submission where change_id_hash = $changeIdHash
        """.as[InFlightSubmission[SubmissionSequencingInfo]].headOption
}

object DbInFlightSubmissionStore {

  class RegisterProcessor(
      override protected val storage: DbStorage,
      maxItemsInSqlInClause: PositiveNumeric[Int],
      override val logger: TracedLogger,
  )(
      override protected implicit val executionContext: ExecutionContext,
      implicit val closeContext: CloseContext,
  ) extends DbBulkUpdateProcessor[InFlightSubmission[
        UnsequencedSubmission
      ], RegisterProcessor.Result] {
    import RegisterProcessor.Result
    import storage.api._
    import storage.converters._

    override def kind: String = "in-flight submission"

    private val protocolVersion = ProtocolVersion.v2Todo_i8793
    private implicit val setParameterTraceContext: SetParameter[TraceContext] =
      TraceContext.getVersionedSetParameter(protocolVersion)
    private implicit val setParameterSubmissionTrackingData: SetParameter[SubmissionTrackingData] =
      SubmissionTrackingData.getVersionedSetParameter(protocolVersion)

    override def executeBatch(
        submissions: NonEmpty[Seq[Traced[InFlightSubmission[UnsequencedSubmission]]]]
    )(implicit traceContext: TraceContext): Future[Iterable[Try[Result]]] = {

      type SubmissionAndCell =
        BulkUpdatePendingCheck[InFlightSubmission[UnsequencedSubmission], Result]

      // Allocate a cell for the result for each submission
      // The cell will be populated by one of the retries
      // or (if retrying stops prematurely due to an exception or shutdown) afterwards.
      val submissionsAndCells =
        submissions.toList.map(BulkUpdatePendingCheck(_, new SingleUseCell[Try[Result]]()))

      // Use a reference for the submissions that still need to be inserted,
      // so that we can pass information from one iteration to the next in the retry.
      val outstandingRef = new AtomicReference[List[SubmissionAndCell]](submissionsAndCells)

      def oneRound: FutureUnlessShutdown[Boolean] = FutureUnlessShutdown.outcomeF {
        val outstanding = outstandingRef.get()
        bulkUpdateWithCheck(
          outstanding.map(_.target),
          "DbInFlightSubmissionStore.register",
        ).map { results =>
          val newOutstandingB = List.newBuilder[SubmissionAndCell]
          results.lazyZip(outstanding).foreach { (result, submissionAndCell) =>
            result match {
              case Success(Outcome(None)) =>
                // Retry on None
                newOutstandingB.addOne(submissionAndCell)
              case other =>
                submissionAndCell.cell.putIfAbsent(other).discard
            }
          }
          val newOutstanding = newOutstandingB.result()
          outstandingRef.set(newOutstanding)
          newOutstanding.isEmpty // Stop retrying if all submissions have been processed
        }
      }

      // Puts the given result into all cells that haven't yet been filled
      def fillEmptyCells(result: Try[Result]): Unit =
        submissionsAndCells.foreach(_.cell.putIfAbsent(result).discard[Option[Try[Result]]])

      def unwrapCells: Seq[Try[Result]] = submissionsAndCells.map(_.cell.getOrElse {
        implicit val loggingContext = ErrorLoggingContext.fromTracedLogger(logger)
        val ex = new IllegalStateException("Bulk update did not provide a result")
        ErrorUtil.internalErrorTry(ex)
      })

      implicit val stopRetry: retry.Success[Boolean] = retry.Success[Boolean](Predef.identity)
      retry
        .Directly(logger, storage, retry.Forever, "register submission retry")
        .unlessShutdown(oneRound, NoExnRetryable)
        .onShutdown {
          fillEmptyCells(Success(AbortedDueToShutdown))
          true
        }
        .transform { result =>
          // Because we retry `Forever`, we can only get here with `result = Success(b)`
          // for `b == true`. So a cell may not yet be filled only if an exception stopped the retry.
          result.failed.foreach { ex =>
            // If all cells have already been filled previously,
            // it is safe to discard the exception because `unlessShutdown` will have already logged it.
            fillEmptyCells(Failure(ex))
          }
          Success(unwrapCells)
        }
    }

    override protected def bulkUpdateAction(
        submissions: NonEmpty[Seq[Traced[InFlightSubmission[UnsequencedSubmission]]]]
    )(implicit
        batchTraceContext: TraceContext
    ): DBIOAction[Array[Int], NoStream, Effect.All] = {
      val insertQuery = storage.profile match {
        case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
          """insert into in_flight_submission(
               change_id_hash, submission_id,
               submission_domain, message_id,
               sequencing_timeout, sequencer_counter, sequencing_time, tracking_data,
               trace_context)
             values (?, ?, 
                     ?, ?, 
                     ?, NULL, NULL, ?,
                     ?)
             on conflict do nothing"""
        case _: DbStorage.Profile.Oracle =>
          """merge into in_flight_submission
               using (
                 select
                   ? change_id_hash, ? submission_id,
                   ? submission_domain, ? message_id,
                   ? sequencing_timeout, ? tracking_data,
                   ? trace_context
                 from dual
               ) to_insert
               on (in_flight_submission.change_id_hash = to_insert.change_id_hash)
               when not matched then
                 insert (
                   change_id_hash, submission_id,
                   submission_domain, message_id,
                   sequencing_timeout, sequencer_counter, sequencing_time, tracking_data,
                   trace_context
                 ) values (
                   to_insert.change_id_hash, to_insert.submission_id,
                   to_insert.submission_domain, to_insert.message_id,
                   to_insert.sequencing_timeout, NULL, NULL, to_insert.tracking_data,
                   to_insert.trace_context
                 )
             """
      }
      implicit val loggingContext: ErrorLoggingContext =
        ErrorLoggingContext.fromTracedLogger(logger)
      val bulkQuery = DbStorage.bulkOperation(
        insertQuery,
        submissions.map(_.value),
        storage.profile,
      ) { pp => submission =>
        import DbStorage.Implicits._
        pp >> submission.changeIdHash
        pp >> submission.submissionId.map(SerializableSubmissionId(_))
        pp >> submission.submissionDomain
        pp >> submission.messageUuid
        pp >> submission.sequencingInfo.timeout
        pp >> submission.sequencingInfo.trackingData
        pp >> submission.submissionTraceContext
      }
      // We need a synchronous commit here to ensure that there can be at most one submission
      // for the same change ID in flight. Without synchronous commits,
      // a participant may have sent off a submission to the sequencer before this write reaches all DB replicas.
      // If a fail-over happens to another participant talking to the stale DB replica,
      // it may send off the same submission again to the sequencer.
      storage.withSyncCommitOnPostgres(bulkQuery)
    }

    private val success: Try[Result] = Success(Outcome(Some(Right(()))))
    override protected def onSuccessItemUpdate(
        item: Traced[InFlightSubmission[UnsequencedSubmission]]
    ): Try[Result] = success

    override protected type CheckData = InFlightSubmission[SubmissionSequencingInfo]
    override protected type ItemIdentifier = ChangeIdHash
    override protected def itemIdentifier(
        submission: InFlightSubmission[UnsequencedSubmission]
    ): ChangeIdHash = submission.changeIdHash
    override protected def dataIdentifier(submission: CheckData): ChangeIdHash =
      submission.changeIdHash

    /** A list of queries for the items that we want to check for */
    override protected def checkQuery(submissionsToCheck: NonEmpty[Seq[ChangeIdHash]])(implicit
        batchTraceContext: TraceContext
    ): Iterable[ReadOnly[Iterable[CheckData]]] = {
      DbStorage.toInClauses_("change_id_hash", submissionsToCheck, maxItemsInSqlInClause).map {
        inClause =>
          import DbStorage.Implicits.BuilderChain._
          val query = sql"""
              select change_id_hash, submission_id, submission_domain, message_id, sequencing_timeout, sequencer_counter, sequencing_time, tracking_data, trace_context
              from in_flight_submission where """ ++ inClause
          query.as[InFlightSubmission[SubmissionSequencingInfo]]
      }
    }

    override protected def analyzeFoundData(
        submission: InFlightSubmission[UnsequencedSubmission],
        foundData: Option[CheckData],
    )(implicit traceContext: TraceContext): Try[Result] = {
      // Retry if the conflicting submission has disappeared, i.e., `foundData == None`
      val response = foundData.map(existing => Either.cond(existing == submission, (), existing))
      Success(Outcome(response))
    }

    override def prettyItem: Pretty[InFlightSubmission[UnsequencedSubmission]] = implicitly
  }

  object RegisterProcessor {
    // We retry inserting until we find a conflicting submission (Left) or have inserted the submission (Right).
    // The `Option` is `None` if we need to retry for the corresponding submission.
    // Retrying stops upon shutdown.
    type Result = UnlessShutdown[Option[Either[InFlightSubmission[SubmissionSequencingInfo], Unit]]]
  }
}
