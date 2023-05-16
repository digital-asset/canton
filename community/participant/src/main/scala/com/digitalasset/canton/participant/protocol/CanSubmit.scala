// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.{
  NoSubmissionPermissionOut,
  TransferProcessorError,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission

import scala.concurrent.ExecutionContext

private[protocol] object CanSubmit {

  def apply(
      contractId: LfContractId,
      topologySnapshot: TopologySnapshot,
      submitter: LfPartyId,
      participantId: ParticipantId,
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Unit] =
    EitherT(
      topologySnapshot
        .hostedOn(submitter, participantId)
        .map { attributes =>
          if (attributes.exists(_.permission == Submission))
            Right(())
          else
            Left(NoSubmissionPermissionOut(contractId, submitter, participantId))
        }
    ).mapK(FutureUnlessShutdown.outcomeK).leftWiden[TransferProcessorError]

}
