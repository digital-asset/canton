// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{Hash as CantonHash, HashPurpose}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.platform.indexer.parallel.{PostPublishData, PublishSource}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration
import io.grpc.Status
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

private[backend] trait StorageBackendTestsCompletions
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (completions)"

  import StorageBackendTestValues.*

  it should "correctly find completions by offset range" in {
    TraceContext.withNewTraceContext("test") { aTraceContext =>
      val party = someParty
      val userId = someUserId
      val emptyTraceContext = SerializableTraceContext(TraceContext.empty).toSerializedDamlProto
      val serializableTraceContext = SerializableTraceContext(aTraceContext).toSerializedDamlProto

      val dtos = Vector(
        dtoCompletion(offset(1), submitters = Set(party)),
        dtoCompletion(offset(2), submitters = Set(party), traceContext = emptyTraceContext),
        dtoCompletion(
          offset(3),
          submitters = Set(party),
          traceContext = serializableTraceContext,
        ),
      )

      executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
      executeSql(ingest(dtos, _))
      executeSql(updateLedgerEnd(offset(3), 3L))
      val completions0to2 = executeSql(
        backend.completion
          .commandCompletions(
            Offset.firstOffset,
            offset(2),
            Some(userId),
            Set(party),
            limit = 10,
          )
      )
      val completions1to2 = executeSql(
        backend.completion
          .commandCompletions(
            offset(2),
            offset(2),
            Some(userId),
            Set(party),
            limit = 10,
          )
      )
      val completions0to9 = executeSql(
        backend.completion
          .commandCompletions(
            Offset.firstOffset,
            offset(9),
            Some(userId),
            Set(party),
            limit = 10,
          )
      )

      completions0to2 should have length 2
      completions1to2 should have length 1
      completions0to9 should have length 3

      completions0to9.head.completionResponse.completion.map(_.traceContext) shouldBe Some(
        SerializableTraceContext(testTraceContext).toDamlProto
      )
      completions0to9(1).completionResponse.completion.map(_.traceContext) shouldBe Some(None)
      completions0to9(2).completionResponse.completion.map(_.traceContext) shouldBe Some(
        SerializableTraceContext(aTraceContext).toDamlProto
      )
    }
  }

  it should "return completions considering the userId filtering" in {
    TraceContext.withNewTraceContext("test") { _ =>
      val party = someParty
      val otherUserId = Ref.UserId.assertFromString("other_user_id")

      val dtos = Vector(
        dtoCompletion(offset(1), submitters = Set(party), userId = someUserId),
        dtoCompletion(offset(2), submitters = Set(party), userId = otherUserId),
      )

      executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
      executeSql(ingest(dtos, _))
      executeSql(updateLedgerEnd(offset(2), 2L))

      val completionsNoUserFilter = executeSql(
        backend.completion
          .commandCompletions(
            Offset.firstOffset,
            offset(2),
            None,
            Set(party),
            limit = 10,
          )
      )
      val completionsSomeUserFilter = executeSql(
        backend.completion
          .commandCompletions(
            Offset.firstOffset,
            offset(2),
            Some(someUserId),
            Set(party),
            limit = 10,
          )
      )

      completionsNoUserFilter should have length 2
      completionsSomeUserFilter should have length 1
    }
  }

  it should "correctly persist and retrieve user IDs" in {
    val party = someParty
    val userId = someUserId

    val dtos = Vector(
      dtoCompletion(offset(1), submitters = Set(party))
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(1), 1L))

    val completions = executeSql(
      backend.completion
        .commandCompletions(
          Offset.firstOffset,
          offset(1),
          Some(userId),
          Set(party),
          limit = 10,
        )
    )

    completions should not be empty
    completions.head.completionResponse.completion should not be empty
    completions.head.completionResponse.completion.toList.head.userId should be(
      userId
    )
  }

  it should "correctly persist and retrieve submission IDs" in {
    val party = someParty
    val submissionId = Some(someSubmissionId)

    val dtos = Vector(
      dtoCompletion(offset(1), submitters = Set(party), submissionId = submissionId),
      dtoCompletion(offset(2), submitters = Set(party), submissionId = None),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val completions = executeSql(
      backend.completion
        .commandCompletions(
          Offset.firstOffset,
          offset(2),
          Some(someUserId),
          Set(party),
          limit = 10,
        )
    ).toList

    completions should have length 2
    inside(completions) { case List(completionWithSubmissionId, completionWithoutSubmissionId) =>
      completionWithSubmissionId.completionResponse.completion should not be empty
      completionWithSubmissionId.completionResponse.completion.toList.head.submissionId should be(
        someSubmissionId
      )
      completionWithoutSubmissionId.completionResponse.completion should not be empty
      completionWithoutSubmissionId.completionResponse.completion.toList.head.submissionId should be(
        ""
      )
    }
  }

  it should "correctly persist and retrieve command deduplication offsets" in {
    val party = someParty
    val anOffset = 1L

    val dtos = Vector(
      dtoCompletion(
        offset(1),
        submitters = Set(party),
        deduplicationOffset = Some(anOffset),
      ),
      dtoCompletion(offset(2), submitters = Set(party), deduplicationOffset = None),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))

    executeSql(updateLedgerEnd(offset(2), 2L))
    val completions = executeSql(
      backend.completion
        .commandCompletions(
          Offset.firstOffset,
          offset(2),
          Some(someUserId),
          Set(party),
          limit = 10,
        )
    ).toList

    completions should have length 2
    inside(completions) {
      case List(completionWithDeduplicationOffset, completionWithoutDeduplicationOffset) =>
        completionWithDeduplicationOffset.completionResponse.completion should not be empty
        completionWithDeduplicationOffset.completionResponse.completion.toList.head.deduplicationPeriod.deduplicationOffset should be(
          Some(anOffset)
        )
        completionWithoutDeduplicationOffset.completionResponse.completion should not be empty
        completionWithoutDeduplicationOffset.completionResponse.completion.toList.head.deduplicationPeriod.deduplicationOffset should not be defined
    }
  }

  it should "correctly persist and retrieve command deduplication durations" in {
    val party = someParty
    val seconds = 100L
    val nanos = 10
    val expectedDuration = Duration.of(seconds, nanos)

    val dtos = Vector(
      dtoCompletion(
        offset(1),
        submitters = Set(party),
        deduplicationDurationSeconds = Some(seconds),
        deduplicationDurationNanos = Some(nanos),
      ),
      dtoCompletion(
        offset(2),
        submitters = Set(party),
        deduplicationDurationSeconds = None,
        deduplicationDurationNanos = None,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))

    executeSql(updateLedgerEnd(offset(2), 2L))
    val completions = executeSql(
      backend.completion
        .commandCompletions(
          Offset.firstOffset,
          offset(2),
          Some(someUserId),
          Set(party),
          limit = 10,
        )
    ).toList

    completions should have length 2
    inside(completions) {
      case List(completionWithDeduplicationOffset, completionWithoutDeduplicationOffset) =>
        completionWithDeduplicationOffset.completionResponse.completion should not be empty
        completionWithDeduplicationOffset.completionResponse.completion.toList.head.deduplicationPeriod.deduplicationDuration should be(
          Some(expectedDuration)
        )
        completionWithoutDeduplicationOffset.completionResponse.completion should not be empty
        completionWithoutDeduplicationOffset.completionResponse.completion.toList.head.deduplicationPeriod.deduplicationDuration should not be defined
    }
  }

  it should "correctly persist and retrieve submitters/act_as" in {
    val party = someParty
    val party2 = someParty2
    val party3 = someParty3

    val dtos = Vector(
      dtoCompletion(
        offset(1),
        submitters = Set(party, party2, party3),
      ),
      dtoCompletion(
        offset(2),
        submitters = Set(party),
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))

    executeSql(updateLedgerEnd(offset(2), 2L))
    val completions = executeSql(
      backend.completion
        .commandCompletions(
          Offset.firstOffset,
          offset(2),
          Some(someUserId),
          Set(party, party2),
          limit = 10,
        )
    ).toList

    completions should have length 2
    inside(completions) { case List(completion1, completion2) =>
      completion1.completionResponse.completion should not be empty
      completion1.completionResponse.completion.toList.head.actAs.toSet should be(
        Set(party, party2)
      )
      completion2.completionResponse.completion should not be empty
      completion2.completionResponse.completion.toList.head.actAs.toSet should be(
        Set(party)
      )
    }
  }

  it should "correctly persist and retrieve the transaction hash of a rejection" in {
    val party = someParty
    val transactionHash = someExternalTransactionHashBinary

    val dtos = Vector(
      // A rejection (update_id IS NULL) carrying a transaction hash.
      dtoCompletion(
        offset(1),
        submitters = Set(party),
        updateId = None,
        rejectionStatusCode = Some(Status.Code.ABORTED.value()),
        rejectionStatusMessage = Some("rejected"),
        transactionHash = Some(transactionHash),
      ),
      // A rejection without a transaction hash.
      dtoCompletion(
        offset(2),
        submitters = Set(party),
        updateId = None,
        rejectionStatusCode = Some(Status.Code.ABORTED.value()),
        rejectionStatusMessage = Some("rejected"),
        transactionHash = None,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    val completions = executeSql(
      backend.completion
        .commandCompletions(
          Offset.firstOffset,
          offset(2),
          Some(someUserId),
          Set(party),
          limit = 10,
        )
    ).toList

    completions should have length 2
    inside(completions) { case List(completionWithHash, completionWithoutHash) =>
      completionWithHash.completionResponse.completion should not be empty
      completionWithHash.completionResponse.completion.toList.head.transactionHash should be(
        Some(ByteString.copyFrom(transactionHash))
      )
      completionWithoutHash.completionResponse.completion should not be empty
      completionWithoutHash.completionResponse.completion.toList.head.transactionHash.isEmpty should be(
        true
      )
    }
  }

  it should "fail on broken command deduplication durations in DB" in {
    val party = someParty
    val seconds = 100L
    val nanos = 10

    val expectedErrorMessage =
      "One of deduplication duration seconds and nanos has been provided " +
        "but they must be either both provided or both absent"

    val dtos1 = Vector(
      dtoCompletion(
        offset(1),
        submitters = Set(party),
        deduplicationDurationSeconds = Some(seconds),
        deduplicationDurationNanos = None,
      )
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos1, _))
    executeSql(updateLedgerEnd(offset(1), 1L))
    val caught = intercept[IllegalArgumentException](
      executeSql(
        backend.completion.commandCompletions(
          Offset.firstOffset,
          offset(1),
          Some(someUserId),
          Set(party),
          limit = 10,
        )
      )
    )

    caught.getMessage should be(expectedErrorMessage)

    val dtos2 = Vector(
      dtoCompletion(
        offset(2),
        submitters = Set(party),
        deduplicationDurationSeconds = None,
        deduplicationDurationNanos = Some(nanos),
      )
    )

    executeSql(ingest(dtos2, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val caught2 = intercept[IllegalArgumentException](
      executeSql(
        backend.completion.commandCompletions(
          offset(2),
          offset(2),
          Some(someUserId),
          Set(party),
          limit = 10,
        )
      )
    )
    caught2.getMessage should be(expectedErrorMessage)
  }

  it should "correctly retrieve completions for post processing recovery" in {
    val messageUuid = UUID.randomUUID()
    val commandId = UUID.randomUUID().toString
    val publicationTime = Timestamp.now()
    val recordTime = Timestamp.now().addMicros(15)
    val submissionId = UUID.randomUUID().toString
    val synchronizerId = SynchronizerId.tryFromString("x::synchronizer1")
    val dtos = Vector(
      dtoCompletion(
        offset(1)
      ),
      dtoCompletion(
        offset = offset(2),
        submitters = Set(someParty),
        commandId = commandId,
        userId = Ref.UserId.assertFromString("userid1"),
        submissionId = Some(submissionId),
        synchronizerId = synchronizerId,
        messageUuid = Some(messageUuid.toString),
        publicationTime = publicationTime,
        isTransaction = true,
      ),
      dtoCompletion(
        offset = offset(5),
        submitters = Set(someParty),
        commandId = commandId,
        userId = Ref.UserId.assertFromString("userid1"),
        submissionId = Some(submissionId),
        synchronizerId = synchronizerId,
        messageUuid = Some(messageUuid.toString),
        publicationTime = publicationTime,
        isTransaction = false,
      ),
      dtoCompletion(
        offset = offset(9),
        submitters = Set(someParty),
        commandId = commandId,
        userId = Ref.UserId.assertFromString("userid1"),
        submissionId = Some(submissionId),
        synchronizerId = synchronizerId,
        recordTime = recordTime,
        messageUuid = None,
        updateId = None,
        publicationTime = publicationTime,
        isTransaction = true,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      backend.completion.commandCompletionsForRecovery(offset(2), offset(10))
    ) shouldBe Vector(
      PostPublishData(
        submissionSynchronizerId = SynchronizerId.tryFromString("x::synchronizer1"),
        publishSource = PublishSource.Local(messageUuid),
        userId = Ref.UserId.assertFromString("userid1"),
        commandId = Ref.CommandId.assertFromString(commandId),
        actAs = Set(someParty),
        offset = offset(2),
        publicationTime = CantonTimestamp(publicationTime),
        submissionId = Some(Ref.SubmissionId.assertFromString(submissionId)),
        accepted = true,
        traceContext = testTraceContext,
      ),
      PostPublishData(
        submissionSynchronizerId = SynchronizerId.tryFromString("x::synchronizer1"),
        publishSource = PublishSource.Sequencer(
          sequencerTimestamp = CantonTimestamp(recordTime)
        ),
        userId = Ref.UserId.assertFromString("userid1"),
        commandId = Ref.CommandId.assertFromString(commandId),
        actAs = Set(someParty),
        offset = offset(9),
        publicationTime = CantonTimestamp(publicationTime),
        submissionId = Some(Ref.SubmissionId.assertFromString(submissionId)),
        accepted = false,
        traceContext = testTraceContext,
      ),
    )
  }

  it should "correctly persist and read back transaction_hash" in {
    val party = someParty
    val hashBytes = someExternalTransactionHashBinary

    val dtos = Vector(
      dtoCompletion(offset(1), submitters = Set(party), transactionHash = Some(hashBytes)),
      dtoCompletion(offset(2), submitters = Set(party), transactionHash = None),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val completions = executeSql(
      backend.completion
        .commandCompletions(
          Offset.firstOffset,
          offset(2),
          Some(someUserId),
          Set(party),
          limit = 10,
        )
    ).toList

    completions should have length 2
    inside(completions) { case List(completionWithHash, completionWithoutHash) =>
      completionWithHash.completionResponse.completion.toList.head.transactionHash shouldBe
        Some(ByteString.copyFrom(hashBytes))
      completionWithoutHash.completionResponse.completion.toList.head.transactionHash shouldBe
        None
    }
  }

  it should "look up rejected completions by hash" in {
    val party = someParty
    val hashBytes = someExternalTransactionHashBinary

    val rejectedCompletion = dtoCompletion(
      offset(1),
      submitters = Set(party),
      transactionHash = Some(hashBytes),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("some rejection"),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(Vector(rejectedCompletion), _))
    executeSql(updateLedgerEnd(offset(1), 1L))

    val (accepted, rejected) = executeSql(connection =>
      (
        backend.completion.acceptedCompletionByHash(hashBytes, Set.empty[Ref.Party])(
          connection
        ),
        backend.completion
          .rejectedCompletionsByHash(hashBytes, 10, None, Set.empty[Ref.Party])(
            connection
          ),
      )
    )

    accepted shouldBe None
    rejected should have length 1
  }

  it should "return empty results for non-existent hash" in {
    val party = someParty
    val existingHash = someExternalTransactionHashBinary
    val nonExistentHash = CantonHash
      .digest(HashPurpose.PreparedSubmission, ByteString.copyFromUtf8("non_existent"), Sha256)
      .getCryptographicEvidence
      .toByteArray

    val completion = dtoCompletion(
      offset(1),
      submitters = Set(party),
      transactionHash = Some(existingHash),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("some rejection"),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(Vector(completion), _))
    executeSql(updateLedgerEnd(offset(1), 1L))

    val (accepted, rejected) = executeSql(connection =>
      (
        backend.completion.acceptedCompletionByHash(nonExistentHash, Set.empty[Ref.Party])(
          connection
        ),
        backend.completion
          .rejectedCompletionsByHash(nonExistentHash, 10, None, Set.empty[Ref.Party])(
            connection
          ),
      )
    )

    accepted shouldBe None
    rejected shouldBe empty
  }

  it should "return both accepted and rejected completions by hash, respecting rejection limit" in {
    val party = someParty
    val hashBytes = someExternalTransactionHashBinary

    val acceptedCompletion = dtoCompletion(
      offset(4),
      submitters = Set(party),
      transactionHash = Some(hashBytes),
    )
    val meta = dtoTransactionMeta(
      offset(4),
      event_sequential_id_first = 1L,
      event_sequential_id_last = 1L,
      transactionHash = Some(hashBytes),
    )
    val rejectedCompletion1 = dtoCompletion(
      offset(1),
      submitters = Set(party),
      transactionHash = Some(hashBytes),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("rejection 1"),
    )
    val rejectedCompletion2 = dtoCompletion(
      offset(2),
      submitters = Set(party),
      transactionHash = Some(hashBytes),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("rejection 2"),
    )
    val rejectedCompletion3 = dtoCompletion(
      offset(3),
      submitters = Set(party),
      transactionHash = Some(hashBytes),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("rejection 3"),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(
      ingest(
        Vector(
          rejectedCompletion1,
          rejectedCompletion2,
          rejectedCompletion3,
          acceptedCompletion,
          meta,
        ),
        _,
      )
    )
    executeSql(updateLedgerEnd(offset(4), 4L))

    val (accepted, rejected) = executeSql(connection =>
      (
        backend.completion.acceptedCompletionByHash(hashBytes, Set.empty[Ref.Party])(
          connection
        ),
        backend.completion.rejectedCompletionsByHash(hashBytes, 2, None, Set.empty[Ref.Party])(
          connection
        ),
      )
    )

    accepted shouldBe defined
    inside(accepted) { case Some(completion) =>
      completion.transactionHash shouldBe
        Some(ByteString.copyFrom(hashBytes))
    }
    rejected should have length 2
  }

  it should "exclude hash lookups for completions above the ledger end" in {
    val party = someParty
    val hashBytes = someExternalTransactionHashBinary

    val acceptedCompletion = dtoCompletion(
      offset(3),
      submitters = Set(party),
      transactionHash = Some(hashBytes),
    )
    val meta = dtoTransactionMeta(
      offset(3),
      event_sequential_id_first = 1L,
      event_sequential_id_last = 1L,
      transactionHash = Some(hashBytes),
    )
    val rejectedCompletion = dtoCompletion(
      offset(2),
      submitters = Set(party),
      transactionHash = Some(hashBytes),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("some rejection"),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(Vector(rejectedCompletion, acceptedCompletion, meta), _))

    // Ledger end is below both rows (offset 1 < offsets 2 and 3): they must be invisible.
    executeSql(updateLedgerEnd(offset(1), 1L))
    val (acceptedBefore, rejectedBefore) = executeSql(connection =>
      (
        backend.completion.acceptedCompletionByHash(hashBytes, Set.empty[Ref.Party])(
          connection
        ),
        backend.completion
          .rejectedCompletionsByHash(hashBytes, 10, None, Set.empty[Ref.Party])(
            connection
          ),
      )
    )
    acceptedBefore shouldBe None
    rejectedBefore shouldBe empty

    // After advancing the ledger end past both rows, the lookups return them.
    executeSql(updateLedgerEnd(offset(3), 3L))
    val (acceptedAfter, rejectedAfter) = executeSql(connection =>
      (
        backend.completion.acceptedCompletionByHash(hashBytes, Set.empty[Ref.Party])(
          connection
        ),
        backend.completion
          .rejectedCompletionsByHash(hashBytes, 10, None, Set.empty[Ref.Party])(
            connection
          ),
      )
    )
    acceptedAfter shouldBe defined
    rejectedAfter should have length 1
  }

  it should "honor the party filter for by-hash rejected lookups, dropping non-overlapping submitters" in {
    val hashBytes = someExternalTransactionHashBinary
    val partyA = Ref.Party.assertFromString("party_alpha")
    val partyB = Ref.Party.assertFromString("party_beta")

    val rejectedA = dtoCompletion(
      offset(1),
      submitters = Set(partyA),
      transactionHash = Some(hashBytes),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("rejection a"),
    )
    val rejectedB = dtoCompletion(
      offset(2),
      submitters = Set(partyB),
      transactionHash = Some(hashBytes),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("rejection b"),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(Vector(rejectedA, rejectedB), _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    val rejectedForA = executeSql(
      backend.completion.rejectedCompletionsByHash(
        hashBytes,
        10,
        None,
        Set(partyA),
      )
    )
    rejectedForA should have length 1
    rejectedForA.head.actAs should contain theSameElementsAs Seq(partyA)
  }

  it should "honor the party filter for by-hash accepted lookups, dropping non-overlapping submitters" in {
    val hashBytes = someExternalTransactionHashBinary
    val partyA = Ref.Party.assertFromString("party_alpha")
    val partyB = Ref.Party.assertFromString("party_beta")

    val accepted = dtoCompletion(
      offset(1),
      submitters = Set(partyA),
      transactionHash = Some(hashBytes),
    )
    val meta = dtoTransactionMeta(
      offset(1),
      event_sequential_id_first = 1L,
      event_sequential_id_last = 1L,
      transactionHash = Some(hashBytes),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(Vector(accepted, meta), _))
    executeSql(updateLedgerEnd(offset(1), 1L))

    // A caller whose parties do not overlap the completion's submitters must not see it,
    // even though the completion exists and the user_id is unconstrained.
    val acceptedForB = executeSql(
      backend.completion.acceptedCompletionByHash(
        hashBytes,
        Set(partyB),
      )
    )
    acceptedForB shouldBe None

    val acceptedForA = executeSql(
      backend.completion.acceptedCompletionByHash(
        hashBytes,
        Set(partyA),
      )
    )
    acceptedForA shouldBe defined
    acceptedForA.value.actAs should contain theSameElementsAs Seq(partyA)
  }

  it should "redact non-overlapping submitters within a multi-party completion for by-hash lookups" in {
    val hashBytes = someExternalTransactionHashBinary
    val partyA = Ref.Party.assertFromString("party_alpha")
    val partyB = Ref.Party.assertFromString("party_beta")

    val accepted = dtoCompletion(
      offset(2),
      submitters = Set(partyA, partyB),
      transactionHash = Some(hashBytes),
    )
    val meta = dtoTransactionMeta(
      offset(2),
      event_sequential_id_first = 1L,
      event_sequential_id_last = 1L,
      transactionHash = Some(hashBytes),
    )
    val rejected = dtoCompletion(
      offset(1),
      submitters = Set(partyA, partyB),
      transactionHash = Some(hashBytes),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("rejection"),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(Vector(rejected, accepted, meta), _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    // A caller authorized only for partyA sees the completion, but partyB is redacted from actAs.
    val filterForA = Set(partyA)
    val (acceptedForA, rejectedForA) = executeSql(connection =>
      (
        backend.completion.acceptedCompletionByHash(hashBytes, filterForA)(connection),
        backend.completion.rejectedCompletionsByHash(hashBytes, 10, None, filterForA)(connection),
      )
    )
    acceptedForA shouldBe defined
    acceptedForA.value.actAs should contain theSameElementsAs Seq(partyA)
    rejectedForA should have length 1
    rejectedForA.head.actAs should contain theSameElementsAs Seq(partyA)
  }

  it should "drop a multi-party completion with no party overlap for by-hash lookups" in {
    val hashBytes = someExternalTransactionHashBinary
    val partyA = Ref.Party.assertFromString("party_alpha")
    val partyB = Ref.Party.assertFromString("party_beta")
    val partyC = Ref.Party.assertFromString("party_gamma")

    val accepted = dtoCompletion(
      offset(2),
      submitters = Set(partyA, partyB),
      transactionHash = Some(hashBytes),
    )
    val meta = dtoTransactionMeta(
      offset(2),
      event_sequential_id_first = 1L,
      event_sequential_id_last = 1L,
      transactionHash = Some(hashBytes),
    )
    val rejected = dtoCompletion(
      offset(1),
      submitters = Set(partyA, partyB),
      transactionHash = Some(hashBytes),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("rejection"),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(Vector(rejected, accepted, meta), _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    // A caller authorized only for an unrelated party must see nothing: the completion's metadata
    // (command id, user id, offset, transaction hash, status) must not leak once actAs is redacted
    // to empty.
    val filterForC = Set(partyC)
    val (acceptedForC, rejectedForC) = executeSql(connection =>
      (
        backend.completion.acceptedCompletionByHash(hashBytes, filterForC)(connection),
        backend.completion.rejectedCompletionsByHash(hashBytes, 10, None, filterForC)(connection),
      )
    )
    acceptedForC shouldBe None
    rejectedForC shouldBe empty
  }

  it should "return all completions for an empty party set (no filter, any-party semantics)" in {
    val party = someParty
    val hashBytes = someExternalTransactionHashBinary

    val accepted = dtoCompletion(
      offset(2),
      submitters = Set(party),
      transactionHash = Some(hashBytes),
    )
    val meta = dtoTransactionMeta(
      offset(2),
      event_sequential_id_first = 1L,
      event_sequential_id_last = 1L,
      transactionHash = Some(hashBytes),
    )
    val rejected = dtoCompletion(
      offset(1),
      submitters = Set(party),
      transactionHash = Some(hashBytes),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("rejection"),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(Vector(rejected, accepted, meta), _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    // An empty party set means "no filter" (caller has CanReadAsAnyParty): everything is returned.
    val noFilter = Set.empty[Ref.Party]
    val (acceptedResult, rejectedResult) = executeSql(connection =>
      (
        backend.completion.acceptedCompletionByHash(hashBytes, noFilter)(connection),
        backend.completion.rejectedCompletionsByHash(hashBytes, 10, None, noFilter)(connection),
      )
    )
    acceptedResult shouldBe defined
    rejectedResult should have length 1
  }

  it should "return all completions for an empty parties set regardless of user or party" in {
    val hashBytes = someExternalTransactionHashBinary
    val partyA = Ref.Party.assertFromString("party_alpha")
    val partyB = Ref.Party.assertFromString("party_beta")
    val user1 = Ref.UserId.assertFromString("user_one")
    val user2 = Ref.UserId.assertFromString("user_two")

    val accepted = dtoCompletion(
      offset(3),
      submitters = Set(partyA),
      userId = user1,
      transactionHash = Some(hashBytes),
    )
    val meta = dtoTransactionMeta(
      offset(3),
      event_sequential_id_first = 1L,
      event_sequential_id_last = 1L,
      transactionHash = Some(hashBytes),
    )
    val rejected1 = dtoCompletion(
      offset(1),
      submitters = Set(partyA),
      userId = user1,
      transactionHash = Some(hashBytes),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("rejection 1"),
    )
    val rejected2 = dtoCompletion(
      offset(2),
      submitters = Set(partyB),
      userId = user2,
      transactionHash = Some(hashBytes),
      updateId = None,
      rejectionStatusCode = Some(10),
      rejectionStatusMessage = Some("rejection 2"),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(Vector(rejected1, rejected2, accepted, meta), _))
    executeSql(updateLedgerEnd(offset(3), 3L))

    val (acceptedResult, rejectedResult) = executeSql(connection =>
      (
        backend.completion.acceptedCompletionByHash(hashBytes, Set.empty[Ref.Party])(
          connection
        ),
        backend.completion
          .rejectedCompletionsByHash(hashBytes, 10, None, Set.empty[Ref.Party])(
            connection
          ),
      )
    )
    acceptedResult shouldBe defined
    rejectedResult should have length 2
  }

  it should "return completions for all users when userId is None" in {
    val party = someParty
    val userId1 = Ref.UserId.assertFromString("user1")
    val userId2 = Ref.UserId.assertFromString("user2")

    val dtos = Vector(
      dtoCompletion(offset(1), submitters = Set(party), userId = userId1),
      dtoCompletion(offset(2), submitters = Set(party), userId = userId2),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    // With userId = None, both completions should be returned
    val completionsNoUser = executeSql(
      backend.completion
        .commandCompletions(Offset.firstOffset, offset(2), None, Set(party), limit = 10)
    )
    completionsNoUser should have length 2

    // With userId = Some(userId1), only user1's completion should be returned
    val completionsUser1 = executeSql(
      backend.completion
        .commandCompletions(Offset.firstOffset, offset(2), Some(userId1), Set(party), limit = 10)
    )
    completionsUser1 should have length 1
    completionsUser1.head.completionResponse.completion.toList.head.userId shouldBe userId1
  }

  it should "return completions with no party filtering when parties is empty" in {
    val party1 = Ref.Party.assertFromString("party_a")
    val party2 = Ref.Party.assertFromString("party_b")

    val dtos = Vector(
      dtoCompletion(offset(1), submitters = Set(party1)),
      dtoCompletion(offset(2), submitters = Set(party2)),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    // With empty parties, all completions should be returned regardless of submitter
    val completionsNoParties = executeSql(
      backend.completion
        .commandCompletions(Offset.firstOffset, offset(2), Some(someUserId), Set.empty, limit = 10)
    )
    completionsNoParties should have length 2

    // With specific party, only matching completions should be returned
    val completionsParty1 = executeSql(
      backend.completion
        .commandCompletions(
          Offset.firstOffset,
          offset(2),
          Some(someUserId),
          Set(party1),
          limit = 10,
        )
    )
    completionsParty1 should have length 1
  }

  it should "return all completions when both userId is None and parties is empty" in {
    val party1 = Ref.Party.assertFromString("party_x")
    val party2 = Ref.Party.assertFromString("party_y")
    val userId1 = Ref.UserId.assertFromString("admin_user1")
    val userId2 = Ref.UserId.assertFromString("admin_user2")

    val dtos = Vector(
      dtoCompletion(offset(1), submitters = Set(party1), userId = userId1),
      dtoCompletion(offset(2), submitters = Set(party2), userId = userId2),
      dtoCompletion(offset(3), submitters = Set(party1, party2), userId = userId1),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(3), 3L))

    // With both None/empty, all completions should be returned (admin use case)
    val allCompletions = executeSql(
      backend.completion
        .commandCompletions(Offset.firstOffset, offset(3), None, Set.empty, limit = 10)
    )
    allCompletions should have length 3
  }
}
