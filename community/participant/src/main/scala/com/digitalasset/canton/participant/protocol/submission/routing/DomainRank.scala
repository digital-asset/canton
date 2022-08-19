// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.Order._
import cats.data.EitherT
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.routing.DomainRouter.ContractData
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessingSteps
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.AutomaticTransferForTransactionFailure
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

private[routing] class DomainRankComputation(
    participantId: ParticipantId,
    priorityOfDomain: DomainId => Int,
    snapshot: DomainId => Option[TopologySnapshot],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  import com.digitalasset.canton.util.ShowUtil._

  // Includes check that submitting party has a participant with submission rights on source and target domain
  def compute(
      contracts: Seq[ContractData],
      targetDomain: DomainId,
      submitters: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, TransactionRoutingError, DomainRank] = {
    val ts = snapshot(targetDomain)
    val maybeTransfers: EitherT[Future, TransactionRoutingError, Seq[
      Option[(LfContractId, (LfPartyId, DomainId))]
    ]] =
      contracts.traverse { c =>
        if (c.domain == targetDomain) EitherT.fromEither[Future](Right(None))
        else {
          for {
            sourceSnapshot <- EitherT.fromEither[Future](
              snapshot(c.domain).toRight(
                AutomaticTransferForTransactionFailure.Failed(s"No snapshot for domain ${c.domain}")
              )
            )
            targetSnapshot <- EitherT.fromEither[Future](
              ts.toRight(
                AutomaticTransferForTransactionFailure.Failed(
                  s"No snapshot for domain $targetDomain"
                )
              )
            )
            submitter <- findSubmitterThatCanTransferContract(
              sourceSnapshot,
              targetSnapshot,
              c,
              submitters,
            )
          } yield Some(c.id -> (submitter, c.domain))
        }
      }

    maybeTransfers.map(transfers =>
      DomainRank(
        transfers.flattenOption.toMap,
        priorityOfDomain(targetDomain),
        targetDomain,
      )
    )
  }

  private def findSubmitterThatCanTransferContract(
      sourceSnapshot: TopologySnapshot,
      targetSnapshot: TopologySnapshot,
      c: ContractData,
      submitters: Set[LfPartyId],
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, LfPartyId] = {

    // Building the transfer out requests lets us check whether contract can be transferred to target domain
    def go(
        submitters: List[LfPartyId],
        errAccum: List[String] = List.empty,
    ): EitherT[Future, String, LfPartyId] = {
      submitters match {
        case Nil =>
          EitherT.leftT(show"Cannot transfer contract ${c.id}: ${errAccum.mkString(",")}")
        case submitter :: rest =>
          TransferOutProcessingSteps
            .transferOutRequestData(
              participantId,
              submitter,
              c.stakeholders,
              sourceSnapshot,
              targetSnapshot,
              logger,
            )
            .biflatMap(
              left => go(rest, errAccum :+ show"Submitter ${submitter} cannot transfer: $left"),
              _canTransfer => EitherT.rightT(submitter),
            )
      }
    }

    go(submitters.intersect(c.stakeholders).toList).leftMap(errors =>
      AutomaticTransferForTransactionFailure.Failed(errors)
    )
  }
}

private[routing] final case class DomainRank(
    transfers: Map[LfContractId, (LfPartyId, DomainId)],
    priority: Int,
    domainId: DomainId,
)

private[routing] object DomainRank {
  //The highest priority domain should be picked first, so negate the priority
  implicit val domainRanking: Ordering[DomainRank] =
    Ordering.by(x => (x.transfers.size, -x.priority, x.domainId))
}
