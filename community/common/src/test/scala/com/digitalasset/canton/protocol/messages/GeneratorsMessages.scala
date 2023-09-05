// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.error.ErrorCategory
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestampSecond
import com.digitalasset.canton.error.GeneratorsError
import com.digitalasset.canton.protocol.messages.LocalReject.ConsistencyRejections.{
  DuplicateKey,
  InactiveContracts,
  InconsistentKey,
  LockedContracts,
  LockedKeys,
}
import com.digitalasset.canton.protocol.messages.LocalReject.MalformedRejects.{
  BadRootHashMessages,
  CreatesExistingContracts,
  MalformedRequest,
  ModelConformance,
  Payloads,
}
import com.digitalasset.canton.protocol.messages.LocalReject.TimeRejects.{
  LedgerTime,
  LocalTimeout,
  SubmissionTime,
}
import com.digitalasset.canton.protocol.messages.LocalReject.TransferInRejects.{
  AlreadyCompleted,
  ContractAlreadyActive,
  ContractAlreadyArchived,
  ContractIsLocked,
}
import com.digitalasset.canton.protocol.messages.LocalReject.TransferOutRejects.ActivenessCheckFailed
import com.digitalasset.canton.protocol.messages.Verdict.ParticipantReject
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.version.RepresentativeProtocolVersion
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsMessages {
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import com.digitalasset.canton.version.GeneratorsVersion.*
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.data.GeneratorsData.*

  implicit val acsCommitmentArb = Arbitrary(
    for {
      domainId <- implicitly[Arbitrary[DomainId]].arbitrary
      sender <- implicitly[Arbitrary[ParticipantId]].arbitrary
      counterParticipant <- implicitly[Arbitrary[ParticipantId]].arbitrary

      periodFrom <- implicitly[Arbitrary[CantonTimestampSecond]].arbitrary
      periodDuration <- Gen.choose(1, 86400L).map(PositiveSeconds.tryOfSeconds)
      period = CommitmentPeriod(periodFrom, periodDuration)

      commitment <- byteStringArb.arbitrary
      protocolVersion <- representativeProtocolVersionGen(AcsCommitment)
    } yield AcsCommitment.create(
      domainId,
      sender,
      counterParticipant,
      period,
      commitment,
      protocolVersion.representative,
    )
  )

  // TODO(#14515) Check that the generator is exhaustive
  implicit val mediatorRejectArb: Arbitrary[Verdict.MediatorReject] = Arbitrary(
    Gen.oneOf(
      GeneratorsError.timeoutRejectArb.arbitrary,
      GeneratorsError.invalidMessageRejectArb.arbitrary,
      GeneratorsError.genericErrorRejectArb.arbitrary,
      GeneratorsError.malformedMessageRejectArb.arbitrary,
    )
  )

  // TODO(#14515) Check that the generator is exhaustive
  private lazy val localRejectImplGen: Gen[LocalRejectImpl] = {
    import LocalReject.*
    val resources = List("resource1", "resource2")
    val details = "details"

    val builders: Seq[RepresentativeProtocolVersion[LocalVerdict.type] => LocalRejectImpl] = Seq(
      LockedContracts.Reject(resources),
      LockedKeys.Reject(resources),
      InactiveContracts.Reject(resources),
      DuplicateKey.Reject(resources),
      InconsistentKey.Reject(resources),
      LedgerTime.Reject(details),
      SubmissionTime.Reject(details),
      LocalTimeout.Reject(),
      ActivenessCheckFailed.Reject(details),
      ContractAlreadyArchived.Reject(details),
      ContractAlreadyActive.Reject(details),
      ContractIsLocked.Reject(details),
      AlreadyCompleted.Reject(details),
      GenericReject("cause", details, resources, "some id", ErrorCategory.TransientServerFailure),
    )

    for {
      pv <- representativeProtocolVersionGen(LocalVerdict)
      builder <- Gen.oneOf(builders)
    } yield builder(pv)
  }

  // TODO(#14515) Check that the generator is exhaustive
  private lazy val localVerdictMalformedGen: Gen[Malformed] = {
    val resources = List("resource1", "resource2")
    val details = "details"

    val builders: Seq[RepresentativeProtocolVersion[LocalVerdict.type] => Malformed] = Seq(
      MalformedRequest.Reject(details),
      Payloads.Reject(details),
      ModelConformance.Reject(details),
      BadRootHashMessages.Reject(details),
      CreatesExistingContracts.Reject(resources),
    )

    for {
      pv <- representativeProtocolVersionGen(LocalVerdict)
      builder <- Gen.oneOf(builders)
    } yield builder(pv)
  }

  // TODO(#14515) Check that the generator is exhaustive
  implicit val localRejectArb: Arbitrary[LocalReject] = Arbitrary(
    Gen.oneOf(localRejectImplGen, localVerdictMalformedGen)
  )

  implicit val verdictApproveArb: Arbitrary[Verdict.Approve] = Arbitrary(
    representativeProtocolVersionGen(Verdict).map(Verdict.Approve())
  )

  implicit val participantRejectReasonArb: Arbitrary[(Set[LfPartyId], LocalReject)] = Arbitrary(
    for {
      parties <- Gen.containerOf[Set, LfPartyId](implicitly[Arbitrary[LfPartyId]].arbitrary)
      reject <- localRejectArb.arbitrary
    } yield (parties, reject)
  )

  implicit val participantRejectArb: Arbitrary[ParticipantReject] = Arbitrary(for {
    pv <- representativeProtocolVersionGen(Verdict)
    reasons <- nonEmptyListGen[(Set[LfPartyId], LocalReject)](participantRejectReasonArb)
  } yield ParticipantReject(reasons)(pv))

  // TODO(#14515) Check that the generator is exhaustive
  implicit val verdictArb: Arbitrary[Verdict] = Arbitrary(
    Gen.oneOf(
      verdictApproveArb.arbitrary,
      mediatorRejectArb.arbitrary,
      participantRejectArb.arbitrary,
    )
  )
}
