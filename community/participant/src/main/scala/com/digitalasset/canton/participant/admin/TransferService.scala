// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.transfer.{
  TransferData,
  TransferSubmissionHandle,
}
import com.digitalasset.canton.participant.store.TransferLookup
import com.digitalasset.canton.protocol.{LfContractId, TransferId}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.{DomainAlias, LfPartyId}

import scala.concurrent.{ExecutionContext, Future}

class TransferService(
    domainIdOfAlias: DomainAlias => Option[DomainId],
    submissionHandles: DomainId => Option[TransferSubmissionHandle],
    transferLookups: DomainId => Option[TransferLookup],
)(implicit ec: ExecutionContext) {
  def transferOut(
      submittingParty: LfPartyId,
      contractId: LfContractId,
      originDomain: DomainAlias,
      targetDomain: DomainAlias,
  )(implicit traceContext: TraceContext): EitherT[Future, String, TransferId] =
    for {
      submissionHandle <- EitherT.fromEither[Future](submissionHandleFor(originDomain))
      targetDomainId <- EitherT.fromEither[Future](domainIdFor(targetDomain))
      transferId <- submissionHandle
        .submitTransferOut(submittingParty, contractId, targetDomainId)
        .biflatMap(
          error => EitherT.leftT[Future, TransferId](error.toString),
          result =>
            EitherT(
              result.transferOutCompletionF.map(status =>
                Either.cond(
                  status.code == com.google.rpc.Code.OK_VALUE,
                  result.transferId,
                  s"Transfer-out failed with status $status",
                )
              )
            ),
        )
    } yield transferId

  def transferIn(submittingParty: LfPartyId, targetDomain: DomainAlias, transferId: TransferId)(
      implicit traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    for {
      submisisonHandle <- EitherT.fromEither[Future](submissionHandleFor(targetDomain))
      result <- submisisonHandle.submitTransferIn(submittingParty, transferId).leftMap(_.toString)
      _ <- EitherT(
        result.transferInCompletionF.map(status =>
          Either.cond(
            status.code == com.google.rpc.Code.OK_VALUE,
            (),
            s"Transfer-in failed with status $status. ID: $transferId",
          )
        )
      )
    } yield ()

  def transferSearch(
      searchDomainAlias: DomainAlias,
      filterOriginDomainAlias: Option[DomainAlias],
      filterTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): EitherT[Future, String, Seq[TransferData]] = {
    for {
      searchDomainId <- EitherT.fromEither[Future](domainIdFor(searchDomainAlias))
      transferLookup <- EitherT.fromEither[Future](
        transferLookups(searchDomainId).toRight(s"Unknown domain alias $searchDomainAlias")
      )
      filterDomain <- EitherT.fromEither[Future](filterOriginDomainAlias match {
        case None => Right(None)
        case Some(value) =>
          domainIdOfAlias(value).toRight(s"Unknown domain alias $value").map(x => Some(x))
      })
      result <- EitherT.liftF(
        transferLookup.find(filterDomain, filterTimestamp, filterSubmitter, limit)
      )
    } yield result
  }

  private[this] def domainIdFor(alias: DomainAlias): Either[String, DomainId] =
    domainIdOfAlias(alias).toRight(s"Unknown domain alias $alias")

  private[this] def submissionHandleFor(
      alias: DomainAlias
  ): Either[String, TransferSubmissionHandle] =
    (for {
      domainId <- domainIdOfAlias(alias)
      sync <- submissionHandles(domainId)
    } yield sync).toRight(s"Unknown domain alias $alias")
}
