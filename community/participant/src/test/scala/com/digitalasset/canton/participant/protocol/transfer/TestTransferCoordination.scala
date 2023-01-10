// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, SyncCryptoApiProvider}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.transfer.TransferCoordination.{
  DomainData,
  TimeProofSourceError,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.TransferProcessorError
import com.digitalasset.canton.participant.store.memory.InMemoryTransferStore
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, TestingTopology}
import com.digitalasset.canton.tracing.{TraceContext, Traced}

import scala.concurrent.{ExecutionContext, Future}

object TestTransferCoordination {
  def apply(
      domains: Set[DomainId],
      timeProofTimestamp: CantonTimestamp,
      snapshotOverride: Option[DomainSnapshotSyncCryptoApi] = None,
      awaitTimestampOverride: Option[Option[Future[Unit]]] = None,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): TransferCoordination = {
    def mkDomainData(domain: DomainId): DomainData = {
      val timeProof = TimeProofTestUtil.mkTimeProof(timeProofTimestamp)
      val transferStore = new InMemoryTransferStore(domain, loggerFactory)

      DomainData(
        transferStore,
        () => EitherT.rightT[FutureUnlessShutdown, TimeProofSourceError](timeProof),
      )
    }

    val domainDataMap = domains.map(domain => domain -> mkDomainData(domain)).toMap
    val transferInBySubmission = { _: DomainId => None }
    val protocolVersionGetter = (_: Traced[DomainId]) =>
      Future.successful(Some(BaseTest.testedProtocolVersion))

    new TransferCoordination(
      domainDataMap.get,
      transferInBySubmission,
      protocolVersion = protocolVersionGetter,
      defaultSyncCryptoApi(domains.toSeq, loggerFactory),
      loggerFactory,
    ) {

      override def awaitTransferOutTimestamp(
          domain: DomainId,
          timestamp: CantonTimestamp,
      )(implicit
          traceContext: TraceContext
      ): Either[TransferProcessingSteps.UnknownDomain, Future[Unit]] = {
        awaitTimestampOverride match {
          case None =>
            super.awaitTransferOutTimestamp(domain, timestamp)
          case Some(overridden) => Right(overridden.getOrElse(Future.unit))
        }
      }

      override def awaitTimestamp(
          domainId: DomainId,
          timestamp: CantonTimestamp,
          waitForEffectiveTime: Boolean,
      )(implicit
          traceContext: TraceContext
      ) =
        awaitTimestampOverride match {
          case None =>
            super.awaitTimestamp(domainId, timestamp, waitForEffectiveTime)
          case Some(overridden) => Right(overridden)
        }

      override def cryptoSnapshot(domain: DomainId, timestamp: CantonTimestamp)(implicit
          traceContext: TraceContext
      ): EitherT[Future, TransferProcessorError, DomainSnapshotSyncCryptoApi] = {
        snapshotOverride match {
          case None => super.cryptoSnapshot(domain, timestamp)
          case Some(cs) => EitherT.pure[Future, TransferProcessorError](cs)
        }
      }
    }
  }

  def defaultSyncCryptoApi(
      domains: Seq[DomainId],
      loggerFactory: NamedLoggerFactory,
  ): SyncCryptoApiProvider =
    TestingTopology(domains = domains.toSet)
      .withReversedTopology(defaultTopology)
      .build(loggerFactory)
      .forOwner(submitterParticipant)

  val observerParticipant1: ParticipantId = ParticipantId("observerParticipant1")
  val observerParticipant2: ParticipantId = ParticipantId("observerParticipant2")

  val defaultTopology = Map(
    submitterParticipant -> Map(submitter -> Submission),
    signatoryParticipant -> Map(signatory -> Submission),
    observerParticipant1 -> Map(observer -> Confirmation),
    observerParticipant2 -> Map(observer -> Observation),
  )

}
