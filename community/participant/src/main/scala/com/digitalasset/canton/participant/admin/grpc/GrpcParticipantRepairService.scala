// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import better.files.*
import cats.data.EitherT
import cats.syntax.all.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.grpc.util.AcsUtil
import com.digitalasset.canton.participant.admin.v0.*
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.protocol.{SerializableContract, SerializableContractWithWitnesses}
import com.digitalasset.canton.topology.{DomainId, PartyId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import java.io.ByteArrayOutputStream
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object GrpcParticipantRepairService {
  val defaultChunkSize: Int =
    1024 * 1024 * 2 // 2MB - This is half of the default max message size of gRPC

  private val groupBy = 1000
}

class GrpcParticipantRepairService(
    sync: CantonSyncService,
    aliasManager: DomainAliasManager,
    processingTimeout: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ParticipantRepairServiceGrpc.ParticipantRepairService
    with NamedLogging {

  /** get contracts for a party
    */
  override def download(
      request: DownloadRequest,
      responseObserver: StreamObserver[AcsSnapshotChunk],
  ): Unit = {
    TraceContext.withNewTraceContext { implicit traceContext =>
      val suffix = if (request.gzipFormat) ".gz" else ""
      val temporaryFile =
        File.newTemporaryFile(prefix = "temporary-canton-acs-snapshot", suffix = suffix)
      lazy val acsFile = for {
        parties <- EitherT.fromEither[Future](
          request.parties
            .map(x => UniqueIdentifier.fromProtoPrimitive_(x).map(PartyId.apply))
            .sequence
        )
        timestamp <- EitherT.fromEither[Future](
          request.timestamp
            .map(CantonTimestamp.fromProtoPrimitive)
            .sequence
            .left
            .map(x => x.message)
        )
        pv <- EitherT.fromEither[Future](
          OptionUtil
            .emptyStringAsNone(request.protocolVersion)
            .map(ProtocolVersion.create)
            .sequence
        )
        acs <-
          sync.stateInspection
            .storeActiveContractsToFile(
              parties.toSet,
              temporaryFile.toJava,
              _.filterString.startsWith(request.filterDomainId),
              timestamp,
              pv,
            )

      } yield acs

      // Create a context that will be automatically cancelled after the processing timeout deadline
      val context = io.grpc.Context
        .current()
        .withCancellation()

      context.run { () =>
        val response = acsFile.map { file =>
          val chunkSize = request.chunkSize.getOrElse(GrpcParticipantRepairService.defaultChunkSize)
          File(file.toPath).newInputStream
            .buffered(chunkSize)
            .autoClosed { s =>
              Iterator
                .continually(s.readNBytes(chunkSize))
                .takeWhile(_.nonEmpty && !context.isCancelled)
                .foreach { chunk =>
                  responseObserver.onNext(AcsSnapshotChunk(ByteString.copyFrom(chunk)))
                }
            }
        }

        Await
          .result(response.value, processingTimeout.unbounded.duration) match {
          case Right(_) =>
            if (!context.isCancelled) responseObserver.onCompleted()
            else {
              context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
              ()
            }
          case Left(error) =>
            responseObserver.onError(new Exception(s"error $error"))
            context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
            ()
        }
        // clean the temporary file used to store the acs
        temporaryFile.delete()
      }
    }
  }

  /** upload contracts for a party
    */
  override def upload(
      responseObserver: StreamObserver[UploadResponse]
  ): StreamObserver[UploadRequest] = {
    // TODO(i12481): This buffer will contain the whole ACS snapshot.
    val outputStream = new ByteArrayOutputStream()

    new StreamObserver[UploadRequest] {
      override def onNext(value: UploadRequest): Unit = {
        Try(outputStream.write(value.acsSnapshot.toByteArray)) match {
          case Failure(exception) =>
            outputStream.close()
            responseObserver.onError(exception)
          case Success(_) =>
        }
      }

      override def onError(t: Throwable): Unit = {
        responseObserver.onError(t)
        outputStream.close()
      }

      // TODO(i12481): implement a solution to prevent the client from sending infinite streams
      override def onCompleted(): Unit = {
        val res = convertAndAddContractsToStore(ByteString.copyFrom(outputStream.toByteArray))
        Try(Await.result(res, processingTimeout.unbounded.duration)) match {
          case Failure(exception) => responseObserver.onError(exception)
          case Success(_) =>
            responseObserver.onNext(UploadResponse())
            responseObserver.onCompleted()
        }
        outputStream.close()
      }
    }
  }

  private def convertAndAddContractsToStore(content: ByteString): Future[UploadResponse] = {
    TraceContext.withNewTraceContext { implicit traceContext =>
      val resultE = for {
        lazyContracts <- AcsUtil.loadFromByteString(content)
        grouped = lazyContracts
          .grouped(GrpcParticipantRepairService.groupBy)
          .map(_.groupMap(_.domainId)(_.contract))
        _ <- LazyList
          .from(grouped)
          .traverse(_.toList.traverse { case (domainId, contracts) =>
            addContractToStore(domainId, contracts)
          })
      } yield ()

      resultE match {
        case Left(error) => Future.failed(new RuntimeException(error))
        case Right(_) => Future.successful(UploadResponse())
      }
    }
  }

  private def addContractToStore(domainId: DomainId, contracts: LazyList[SerializableContract])(
      implicit traceContext: TraceContext
  ): Either[String, Unit] = {
    for {
      alias <- aliasManager
        .aliasForDomainId(domainId)
        .toRight(s"Not able to find domain alias for ${domainId.filterString}")
      _ <- sync.repairService.addContracts(
        alias,
        contracts.map(SerializableContractWithWitnesses(_, Set.empty)),
        ignoreAlreadyAdded = true,
        ignoreStakeholderCheck = true,
      )
    } yield ()
  }
}
