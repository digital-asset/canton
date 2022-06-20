// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.{CantonTimestamp, FullTransferOutTree}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.protocol.transfer.TransferData
import com.digitalasset.canton.participant.store.TransferStore
import com.digitalasset.canton.participant.store.TransferStore._
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.protocol.{SerializableContract, TransactionId, TransferId}
import com.digitalasset.canton.resource.DbStorage.{DbAction, Profile}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, SequencedEvent, SignedContent}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{Checked, CheckedT, ErrorUtil}
import com.digitalasset.canton.version.{ProtocolVersion, UntypedVersionedMessage, VersionedMessage}
import com.google.protobuf.ByteString
import io.functionmeta.functionFullName
import slick.jdbc.TransactionIsolation.Serializable
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class DbTransferStore(
    override protected val storage: DbStorage,
    domain: DomainId,
    protocolVersion: ProtocolVersion,
    cryptoApi: CryptoPureApi,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TransferStore
    with DbStore {
  import storage.api._
  import storage.converters._

  private val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("transfer-store")

  implicit val getResultFullTransferOutTree: GetResult[FullTransferOutTree] = GetResult(r =>
    FullTransferOutTree
      .fromByteString(cryptoApi)(ByteString.copyFrom(r.<<[Array[Byte]]))
      .fold[FullTransferOutTree](
        error =>
          throw new DbDeserializationException(s"Error deserializing transfer out request $error"),
        Predef.identity,
      )
  )
  implicit val setParameterFullTransferOutTree: SetParameter[FullTransferOutTree] =
    (r: FullTransferOutTree, pp: PositionedParameters) =>
      pp >> r.toByteString(protocolVersion).toByteArray

  private implicit val setParameterSerializableContract: SetParameter[SerializableContract] =
    SerializableContract.getVersionedSetParameter(protocolVersion)

  private val protoConverterSequencedEventOpenEnvelope =
    SignedContent.versionedProtoConverter[SequencedEvent[DefaultOpenEnvelope]](
      "OpenEnvelope[ProtocolMessage]"
    )

  private def parseSignedContentProto(
      signedContentProto: VersionedMessage[SignedContent[SequencedEvent[DefaultOpenEnvelope]]]
  ) =
    protoConverterSequencedEventOpenEnvelope.fromProtoVersioned(
      SequencedEvent.fromByteString(
        OpenEnvelope.fromProtoV0(
          EnvelopeContent.messageFromByteString(protocolVersion, cryptoApi),
          protocolVersion,
        )
      )
    )(signedContentProto)

  implicit val getResultOptionDeliveredTransferOutResult
      : GetResult[Option[DeliveredTransferOutResult]] = GetResult(
    _.<<[Option[Array[Byte]]].map(bytes =>
      (for {
        signedContentP <- ProtoConverter.protoParserArray(UntypedVersionedMessage.parseFrom)(bytes)
        signedContent <- parseSignedContentProto(VersionedMessage(signedContentP))
        result <- DeliveredTransferOutResult
          .create(signedContent)
          .leftMap(err => OtherError(err.toString))
      } yield result)
        .fold(
          error =>
            throw new DbDeserializationException(
              s"Error deserializing delivered transfer out result $error"
            ),
          identity,
        )
    )
  )

  implicit val setParameterDeliveredTransferOutResult: SetParameter[DeliveredTransferOutResult] =
    (r: DeliveredTransferOutResult, pp: PositionedParameters) => pp >> r.result.toByteArray

  implicit val setParameterOptionDeliveredTransferOutResult
      : SetParameter[Option[DeliveredTransferOutResult]] =
    (r: Option[DeliveredTransferOutResult], pp: PositionedParameters) =>
      pp >> r.map(_.result.toByteArray)

  private implicit val getResultTransferData: GetResult[TransferData] = GetResult(r =>
    TransferData(
      GetResult[CantonTimestamp].apply(r),
      r.nextLong(),
      getResultFullTransferOutTree(r),
      GetResult[CantonTimestamp].apply(r),
      GetResult[SerializableContract].apply(r),
      GetResult[TransactionId].apply(r),
      getResultOptionDeliveredTransferOutResult(r),
    )
  )

  private implicit val getResultTransferEntry: GetResult[TransferEntry] = GetResult(r =>
    TransferEntry(
      getResultTransferData(r),
      GetResult[Option[TimeOfChange]].apply(r),
    )
  )

  override def addTransfer(
      transferData: TransferData
  )(implicit traceContext: TraceContext): EitherT[Future, TransferStoreError, Unit] =
    processingTime.metric.eitherTEvent {
      ErrorUtil.requireArgument(
        transferData.targetDomain == domain,
        s"Domain ${domain.unwrap}: Transfer store cannot store transfer for domain ${transferData.targetDomain.unwrap}",
      )

      val transferId: TransferId = transferData.transferId
      val newEntry = TransferEntry(transferData, None)

      import DbStorage.Implicits._
      val insert: DBIO[Int] = sqlu"""
        insert into transfers(target_domain, origin_domain, request_timestamp, transfer_out_timestamp, transfer_out_request_counter,
        transfer_out_request, transfer_out_decision_time, contract, creating_transaction_id, transfer_out_result, submitter_lf)
        values (
          $domain,
          ${transferId.originDomain},
          ${transferId.requestTimestamp},
          ${transferData.transferOutTimestamp},
          ${transferData.transferOutRequestCounter},
          ${transferData.transferOutRequest},
          ${transferData.transferOutDecisionTime},
          ${transferData.contract},
          ${transferData.creatingTransactionId},
          ${transferData.transferOutResult},
          ${transferData.transferOutRequest.submitter})
      """

      def insertExisting(
          existingEntry: TransferEntry
      ): Checked[TransferStoreError, TransferAlreadyCompleted, Option[DBIO[Int]]] = {
        def update(entry: TransferEntry): DBIO[Int] = {
          val id = entry.transferData.transferId
          val data = entry.transferData
          sqlu"""
          update transfers
          set transfer_out_timestamp=${data.transferOutTimestamp}, transfer_out_request_counter=${data.transferOutRequestCounter},
            transfer_out_request=${data.transferOutRequest}, transfer_out_decision_time=${data.transferOutDecisionTime},
            contract=${data.contract}, creating_transaction_id=${data.creatingTransactionId},
            transfer_out_result=${data.transferOutResult}, submitter_lf=${data.transferOutRequest.submitter}
       where
          target_domain=$domain and origin_domain=${id.originDomain} and request_timestamp =${id.requestTimestamp}
        """
        }
        existingEntry.mergeWith(newEntry).map(entry => Some(update(entry)))
      }

      insertDependentDeprecated(
        entryExists(transferId),
        insertExisting,
        insert,
        dbError => throw dbError,
      )
        .map(_ => ())
        .toEitherT
    }

  override def lookup(transferId: TransferId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferStore.TransferLookupError, TransferData] =
    processingTime.metric.eitherTEvent {
      EitherT(storage.query(entryExists(transferId), functionFullName).map {
        case None => Left(UnknownTransferId(transferId))
        case Some(TransferEntry(_, Some(timeOfCompletion))) =>
          Left(TransferCompleted(transferId, timeOfCompletion))
        case Some(transferEntry) => Right(transferEntry.transferData)
      })
    }

  private def entryExists(id: TransferId): DbAction.ReadOnly[Option[TransferEntry]] = sql"""
     select transfer_out_timestamp, transfer_out_request_counter, transfer_out_request, transfer_out_decision_time,
     contract, creating_transaction_id, transfer_out_result, time_of_completion_request_counter, time_of_completion_timestamp
     from transfers where target_domain = $domain and origin_domain = ${id.originDomain} and request_timestamp =${id.requestTimestamp}
    """.as[TransferEntry].headOption

  override def addTransferOutResult(
      transferOutResult: DeliveredTransferOutResult
  )(implicit traceContext: TraceContext): EitherT[Future, TransferStoreError, Unit] =
    processingTime.metric.eitherTEvent {
      val transferId = transferOutResult.transferId

      val exists: DbAction.ReadOnly[Option[Option[DeliveredTransferOutResult]]] = sql"""
       select transfer_out_result
       from transfers
       where
          target_domain=$domain and origin_domain=${transferId.originDomain} and request_timestamp =${transferId.requestTimestamp}
        """.as[Option[DeliveredTransferOutResult]].headOption

      def update(previousResult: Option[DeliveredTransferOutResult]) = {
        previousResult
          .fold[Checked[TransferStoreError, Nothing, Option[DBIO[Int]]]](Checked.result(Some(sqlu"""
              update transfers
              set transfer_out_result = ${transferOutResult}
              where target_domain=$domain and origin_domain=${transferId.originDomain} and request_timestamp=${transferId.requestTimestamp}
              """)))(previous =>
            if (previous == transferOutResult) Checked.result(None)
            else
              Checked.abort(TransferOutResultAlreadyExists(transferId, previous, transferOutResult))
          )
      }

      updateDependentDeprecated(
        exists,
        update,
        Checked.abort(UnknownTransferId(transferId)),
        dbError => throw dbError,
      )
        .map(_ => ())
        .toEitherT
    }

  override def completeTransfer(transferId: TransferId, timeOfCompletion: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, Nothing, TransferStoreError, Unit] =
    processingTime.metric.checkedTEvent[Nothing, TransferStoreError, Unit] {

      val updateSameOrUnset = sqlu"""
              update transfers
                set time_of_completion_request_counter=${timeOfCompletion.rc}, time_of_completion_timestamp=${timeOfCompletion.timestamp}
              where
                target_domain=$domain and origin_domain=${transferId.originDomain} and request_timestamp=${transferId.requestTimestamp}
                and (time_of_completion_request_counter is NULL 
                  or (time_of_completion_request_counter = ${timeOfCompletion.rc} and time_of_completion_timestamp = ${timeOfCompletion.timestamp}))
            """

      val doneE: EitherT[Future, TransferStoreError, Unit] =
        EitherT(storage.update(updateSameOrUnset, functionFullName).map { changed =>
          if (changed > 0) {
            if (changed != 1)
              logger.error(
                s"Transfer completion query changed $changed lines. It should only change 1."
              )
            Right(())
          } else {
            if (changed != 0)
              logger.error(
                s"Transfer completion query changed $changed lines -- this should not be negative."
              )
            Left(TransferAlreadyCompleted(transferId, timeOfCompletion))
          }
        })

      CheckedT.fromEitherTNonabort((), doneE)
    }

  override def deleteTransfer(
      transferId: TransferId
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.metric.event {
      storage.update_(
        sqlu"""delete from transfers
                where target_domain=$domain and origin_domain=${transferId.originDomain} and request_timestamp=${transferId.requestTimestamp}""",
        functionFullName,
      )
    }

  override def deleteCompletionsSince(
      criterionInclusive: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] = processingTime.metric.event {
    val query = sqlu"""
       update transfers
         set time_of_completion_request_counter=null, time_of_completion_timestamp=null
         where target_domain=$domain and time_of_completion_request_counter >= $criterionInclusive
      """
    storage.update_(query, functionFullName)
  }

  private lazy val findPendingBase = sql"""
     select transfer_out_timestamp, transfer_out_request_counter, transfer_out_request, transfer_out_decision_time,
     contract, creating_transaction_id, transfer_out_result, time_of_completion_request_counter, time_of_completion_timestamp
     from transfers
     where target_domain=$domain and time_of_completion_request_counter is null and time_of_completion_timestamp is null
    """

  override def find(
      filterOrigin: Option[DomainId],
      filterTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[TransferData]] =
    processingTime.metric.event {
      storage.query(
        {
          import DbStorage.Implicits.BuilderChain._
          import DbStorage.Implicits._

          val originFilter = filterOrigin.fold(sql"")(domain => sql" and origin_domain=${domain}")
          val timestampFilter = filterTimestamp.fold(sql"")(ts => sql" and request_timestamp=${ts}")
          val submitterFilter =
            filterSubmitter.fold(sql"")(submitter => sql" and submitter_lf=${submitter}")
          val limitSql = storage.limitSql(limit)
          (findPendingBase ++ originFilter ++ timestampFilter ++ submitterFilter ++ limitSql)
            .as[TransferData]
        },
        functionFullName,
      )
    }

  override def findAfter(
      requestAfter: Option[(CantonTimestamp, DomainId)],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[TransferData]] =
    processingTime.metric.event {
      storage.query(
        {
          import DbStorage.Implicits.BuilderChain._

          val timestampFilter =
            requestAfter.fold(sql"")({ case (requestTimestamp, originDomain) =>
              storage.profile match {
                case Profile.Oracle(_) =>
                  sql"and (request_timestamp > ${requestTimestamp} or (request_timestamp = ${requestTimestamp} and origin_domain > ${originDomain}))"
                case _ =>
                  sql" and (request_timestamp, origin_domain) > (${requestTimestamp}, ${originDomain}) "
              }
            })
          val order = sql" order by request_timestamp, origin_domain "
          val limitSql = storage.limitSql(limit)

          (findPendingBase ++ timestampFilter ++ order ++ limitSql).as[TransferData]
        },
        functionFullName,
      )
    }

  private def insertDependentDeprecated[E, W, A, R](
      exists: DBIO[Option[A]],
      insertExisting: A => Checked[E, W, Option[DBIO[R]]],
      insertFresh: DBIO[R],
      errorHandler: Throwable => E,
      operationName: String = "insertDependentDeprecated",
  )(implicit traceContext: TraceContext): CheckedT[Future, E, W, Option[R]] =
    updateDependentDeprecated(
      exists,
      insertExisting,
      Checked.result(Some(insertFresh)),
      errorHandler,
      operationName,
    )

  private def updateDependentDeprecated[E, W, A, R](
      exists: DBIO[Option[A]],
      insertExisting: A => Checked[E, W, Option[DBIO[R]]],
      insertNonExisting: Checked[E, W, Option[DBIO[R]]],
      errorHandler: Throwable => E,
      operationName: String = "updateDependentDeprecated",
  )(implicit traceContext: TraceContext): CheckedT[Future, E, W, Option[R]] = {
    import DbStorage.Implicits._
    import storage.api.{DBIO => _, _}

    val readAndInsert =
      exists
        .flatMap(existing =>
          existing.fold(insertNonExisting)(insertExisting(_)).traverse {
            case None => DBIO.successful(None): DBIO[Option[R]]
            case Some(action) => action.map(Some(_)): DBIO[Option[R]]
          }
        )
    val compoundAction = readAndInsert.transactionally.withTransactionIsolation(Serializable)

    val result = storage.queryAndUpdate(compoundAction, operationName = operationName)

    CheckedT(result.recover[Checked[E, W, Option[R]]] { case NonFatal(x) =>
      Checked.abort(errorHandler(x))
    })
  }
}
