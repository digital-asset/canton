// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.implicits._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.routing.Transfers.TransferArgs
import com.digitalasset.canton.participant.sync.TransactionRoutingError.AutomaticTransferForTransactionFailure
import com.digitalasset.canton.participant.sync.{SyncDomain, TransactionRoutingError}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

private[routing] class ContractsTransferer(
    connectedDomains: TrieMap[DomainId, SyncDomain],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  def transfer(
      domainRankTarget: DomainRank
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, Unit] = {
    if (domainRankTarget.transfers.nonEmpty) {
      logger.info(
        s"Automatic transaction transfer into domain ${domainRankTarget.domainId}"
      )
      domainRankTarget.transfers.toSeq.traverse_ { case (cid, (lfParty, sourceDomainId)) =>
        perform(
          TransferArgs(
            sourceDomainId,
            domainRankTarget.domainId,
            lfParty,
            cid,
            traceContext,
          )
        )
      }
    } else {
      EitherT.pure[Future, TransactionRoutingError](())
    }
  }

  private def perform(args: TransferArgs): EitherT[Future, TransactionRoutingError, Unit] = {
    val TransferArgs(sourceDomain, targetDomain, submittingParty, contractId, _traceContext) = args
    implicit val traceContext = _traceContext

    val transfer = for {
      sourceSyncDomain <- EitherT.fromEither[Future](
        connectedDomains.get(sourceDomain).toRight("Not connected to the source domain")
      )

      targetSyncDomain <- EitherT.fromEither[Future](
        connectedDomains.get(targetDomain).toRight("Not connected to the target domain")
      )

      _unit <- EitherT
        .cond[Future](sourceSyncDomain.ready, (), "The source domain is not ready for submissions")

      outResult <- sourceSyncDomain
        .submitTransferOut(
          submittingParty,
          contractId,
          targetDomain,
          TargetProtocolVersion(targetSyncDomain.staticDomainParameters.protocolVersion),
        )
        .leftMap(_.toString)
      outStatus <- EitherT.right[String](outResult.transferOutCompletionF)
      _outApprove <- EitherT.cond[Future](
        outStatus.code == com.google.rpc.Code.OK_VALUE,
        (),
        s"The transfer out for ${outResult.transferId} failed with status $outStatus",
      )

      _unit <- EitherT
        .cond[Future](targetSyncDomain.ready, (), "The target domain is not ready for submission")

      inResult <- targetSyncDomain
        .submitTransferIn(
          submittingParty,
          outResult.transferId,
          SourceProtocolVersion(sourceSyncDomain.staticDomainParameters.protocolVersion),
        )
        .leftMap[String](err => s"Transfer in failed with error ${err}")

      inStatus <- EitherT.right[String](inResult.transferInCompletionF)
      _inApprove <- EitherT.cond[Future](
        inStatus.code == com.google.rpc.Code.OK_VALUE,
        (),
        s"The transfer in for ${outResult.transferId} failed with verdict $inStatus",
      )
    } yield ()

    transfer.leftMap[TransactionRoutingError](str =>
      AutomaticTransferForTransactionFailure.Failed(str)
    )
  }
}

private[routing] object Transfers {
  case class TransferArgs(
      sourceDomain: DomainId,
      targetDomain: DomainId,
      submitter: LfPartyId,
      contract: LfContractId,
      traceContext: TraceContext,
  )
}
