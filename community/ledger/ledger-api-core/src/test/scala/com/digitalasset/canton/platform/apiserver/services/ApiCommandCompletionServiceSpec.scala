// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.grpc.test.StreamConsumer
import com.daml.ledger.api.v2.command_completion_service.*
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.auth.*
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.auth.services.CommandCompletionServiceAuthorization
import com.digitalasset.canton.ledger.participant.state.index.IndexCompletionsService
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.LedgerEnd
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{
  Context,
  Contexts,
  Metadata,
  ServerCall,
  ServerCallHandler,
  ServerInterceptor,
  Status,
  StatusRuntimeException,
}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/** Auth-wrapper and service-boundary tests for CommandCompletionServiceAuthorization and
  * ApiCommandCompletionService.
  *
  * The integration auth tests (GetCompletionsAuthIT) only exercise user-management-resolved tokens.
  * This spec injects arbitrary ClaimSet.Claims via a gRPC interceptor to cover edge cases like
  * non-user-resolved tokens, and asserts on the parties set the wrapper derives and pushes down.
  * Actual visibility enforcement (actAs redaction, NOT_FOUND on no overlap) lives below the index
  * boundary and is tested in the storage and buffer specs.
  */
@SuppressWarnings(Array("com.digitalasset.canton.UnusedPrivateVal"))
class ApiCommandCompletionServiceSpec
    extends AsyncWordSpec
    with Matchers
    with BaseTest
    with PekkoBeforeAndAfterAll {

  private implicit val resourceContext: ResourceContext = ResourceContext(executionContext)

  private val metrics = LedgerApiServerMetrics.ForTesting

  private val party1 = Ref.Party.assertFromString("alice")
  private val party2 = Ref.Party.assertFromString("bob")

  private val hashRequest = GetCompletionByHashRequest(
    transactionHash = ByteString.copyFrom(Array.fill(32)(1.toByte))
  )

  private val someHashResponse = GetCompletionByHashResponse(
    acceptedCompletion = Some(
      Completion.defaultInstance.copy(
        commandId = "cmd1",
        userId = "user1",
        actAs = Seq(party1),
        status = Some(com.google.rpc.status.Status(com.google.rpc.Code.OK_VALUE)),
      )
    ),
    lastRejectedCompletions = Seq.empty,
  )

  private val claimsRef = new AtomicReference[ClaimSet.Claims](ClaimSet.Claims.Wildcard)

  private val testAuthorizer = new Authorizer(
    now = () => java.time.Instant.now(),
    participantId = "test-participant",
    loggerFactory = loggerFactory,
  )

  private val claimsInterceptor: ServerInterceptor = new ServerInterceptor {
    override def interceptCall[ReqT, RespT](
        call: ServerCall[ReqT, RespT],
        headers: Metadata,
        next: ServerCallHandler[ReqT, RespT],
    ): ServerCall.Listener[ReqT] = {
      val nextCtx =
        Context.current.withValue(AuthInterceptor.contextKeyClaimSet, claimsRef.get())
      Contexts.interceptCall(nextCtx, call, headers, next)
    }
  }

  /** Captures the parties the authorization layer derived and pushed down, so tests can assert on
    * the derivation, which is the only thing this layer owns. Actual visibility enforcement lives
    * below the index boundary and is tested in the storage and buffer specs.
    */
  private val capturedByHashParties = new AtomicReference[Option[Set[Ref.Party]]](None)

  private def completionsService(
      byHashResponse: Option[GetCompletionByHashResponse] = None
  ): IndexCompletionsService = {
    capturedByHashParties.set(None)
    new IndexCompletionsService {
      override def getCompletions(
          begin: Option[Offset],
          userId: Option[Ref.UserId],
          parties: Set[Ref.Party],
      )(implicit
          loggingContext: LoggingContextWithTrace
      ): Source[CompletionStreamResponse, NotUsed] =
        Source.empty

      override def getCompletionByHash(
          hash: ByteString,
          parties: Set[Ref.Party],
      )(implicit loggingContext: LoggingContextWithTrace): Future[GetCompletionByHashResponse] = {
        capturedByHashParties.set(Some(parties))
        byHashResponse.fold(
          Future.failed[GetCompletionByHashResponse](
            io.grpc.Status.NOT_FOUND.asRuntimeException()
          )
        )(Future.successful)
      }

      override def currentLedgerEnd(): Option[LedgerEnd] =
        Some(
          LedgerEnd(
            lastOffset = Offset.tryFromLong(100L),
            lastEventSeqId = 0L,
            lastStringInterningId = 0,
            lastPublicationTime = CantonTimestamp.MinValue,
            synchronizerIndices = Map.empty,
          )
        )
    }
  }

  private def openChannel(
      service: ApiCommandCompletionService
  ): ResourceOwner[CommandCompletionServiceGrpc.CommandCompletionServiceStub] = {
    val authWrapper =
      new CommandCompletionServiceAuthorization(service, testAuthorizer)
    for {
      name <- ResourceOwner.forValue(() => UUID.randomUUID().toString)
      _ <- ResourceOwner.forServer(
        InProcessServerBuilder
          .forName(name)
          .intercept(claimsInterceptor)
          .addService(() =>
            CommandCompletionServiceGrpc.bindService(authWrapper, executionContext)
          ),
        shutdownTimeout = 5.seconds,
      )
      channel <- ResourceOwner.forChannel(
        InProcessChannelBuilder.forName(name),
        shutdownTimeout = 5.seconds,
      )
    } yield CommandCompletionServiceGrpc.stub(channel)
  }

  "ApiCommandCompletionService" should {

    // With party-only auth, a request for all parties (empty `parties`) requires CanReadAsAnyParty.
    // A token carrying only ReadAs(alice) must be denied.
    "deny getCompletions with empty parties when caller lacks ReadAsAnyParty" in {
      val svc = new ApiCommandCompletionService(
        completionsService(),
        metrics,
        loggerFactory,
      )
      claimsRef.set(
        ClaimSet.Claims(
          claims = Seq[Claim](ClaimPublic, ClaimReadAsParty(party1)),
          participantId = None,
          userId = Some("someUser"),
          expiration = None,
          identityProviderId = None,
          resolvedFromUser = true,
        )
      )
      openChannel(svc).use { stub =>
        // Empty `parties` means "all parties", which is only allowed with CanReadAsAnyParty.
        val request = GetCompletionsRequest(
          parties = Seq.empty,
          beginExclusive = 0L,
        )
        new StreamConsumer[CompletionStreamResponse](observer =>
          stub.getCompletions(request, observer)
        ).all()
          .failed
          .map(inside(_) { case sre: StatusRuntimeException =>
            sre.getStatus.getCode shouldBe Status.Code.PERMISSION_DENIED
          })
      }
    }

    // A resolved-user token with readAs claims should produce a party filter carrying the full
    // set of readAs parties (regardless of the resolving user), pushed down to the index.
    "derive a party filter from a resolved-user token with readAs" in {
      val svc = new ApiCommandCompletionService(
        completionsService(byHashResponse = Some(someHashResponse)),
        metrics,
        loggerFactory,
      )
      claimsRef.set(
        ClaimSet.Claims(
          claims = Seq[Claim](ClaimPublic, ClaimReadAsParty(party1), ClaimReadAsParty(party2)),
          participantId = None,
          userId = Some("user1"),
          expiration = None,
          identityProviderId = None,
          resolvedFromUser = true,
        )
      )
      openChannel(svc).use { stub =>
        stub.getCompletionByHash(hashRequest).map { _ =>
          capturedByHashParties.get() shouldBe Some(
            Set(party1, party2)
          )
        }
      }
    }

    "allow getCompletions when caller can ReadAs the requested parties" in {
      val svc = new ApiCommandCompletionService(
        completionsService(),
        metrics,
        loggerFactory,
      )
      claimsRef.set(
        ClaimSet.Claims(
          claims = Seq[Claim](ClaimPublic, ClaimReadAsParty(party1)),
          participantId = None,
          userId = Some("myuser"),
          expiration = None,
          identityProviderId = None,
          resolvedFromUser = true,
        )
      )
      openChannel(svc).use { stub =>
        val request = GetCompletionsRequest(
          parties = Seq(party1),
          beginExclusive = 0L,
        )
        // Stream should open successfully (empty, but no PERMISSION_DENIED)
        new StreamConsumer[CompletionStreamResponse](observer =>
          stub.getCompletions(request, observer)
        ).all()
          .map { results =>
            results shouldBe empty
          }
      }
    }

    "deny getCompletionByHash when caller has no read-as claims" in {
      val svc = new ApiCommandCompletionService(
        completionsService(byHashResponse = Some(someHashResponse)),
        metrics,
        loggerFactory,
      )
      // Token with only ClaimPublic, no read-as or act-as
      claimsRef.set(
        ClaimSet.Claims(
          claims = Seq[Claim](ClaimPublic),
          participantId = None,
          userId = Some("user1"),
          expiration = None,
          identityProviderId = None,
          resolvedFromUser = true,
        )
      )
      openChannel(svc).use { stub =>
        stub
          .getCompletionByHash(hashRequest)
          .failed
          .map(inside(_) { case sre: StatusRuntimeException =>
            sre.getStatus.getCode shouldBe Status.Code.PERMISSION_DENIED
          })
      }
    }

    "return NOT_FOUND from getCompletionByHash when the index has nothing visible" in {
      val svc = new ApiCommandCompletionService(
        completionsService(byHashResponse = None),
        metrics,
        loggerFactory,
      )
      claimsRef.set(
        ClaimSet.Claims(
          claims = Seq[Claim](ClaimPublic, ClaimReadAsParty(party1)),
          participantId = None,
          userId = Some("user1"),
          expiration = None,
          identityProviderId = None,
          resolvedFromUser = true,
        )
      )
      openChannel(svc).use { stub =>
        stub
          .getCompletionByHash(hashRequest)
          .failed
          .map(inside(_) { case sre: StatusRuntimeException =>
            sre.getStatus.getCode shouldBe Status.Code.NOT_FOUND
          })
      }
    }

    "derive a party-only filter for an unresolved token, ignoring the userId" in {
      val svc = new ApiCommandCompletionService(
        completionsService(byHashResponse = Some(someHashResponse)),
        metrics,
        loggerFactory,
      )
      // Non-user token (resolvedFromUser=false) with an unrelated userId in claims.
      // The userId plays no role; only the readAs parties enter the filter.
      claimsRef.set(
        ClaimSet.Claims(
          claims = Seq[Claim](ClaimPublic, ClaimReadAsParty(party1)),
          participantId = None,
          userId = Some("completelyUnrelatedUser"),
          expiration = None,
          identityProviderId = None,
          resolvedFromUser = false,
        )
      )
      openChannel(svc).use { stub =>
        stub.getCompletionByHash(hashRequest).map { _ =>
          capturedByHashParties.get() shouldBe Some(
            Set(party1)
          )
        }
      }
    }

    "derive an empty parties set (no filter) for a CanReadAsAnyParty token" in {
      val svc = new ApiCommandCompletionService(
        completionsService(byHashResponse = Some(someHashResponse)),
        metrics,
        loggerFactory,
      )
      claimsRef.set(
        ClaimSet.Claims(
          claims = Seq[Claim](ClaimPublic, ClaimReadAsAnyParty),
          participantId = None,
          userId = Some("adminUser"),
          expiration = None,
          identityProviderId = None,
          resolvedFromUser = true,
        )
      )
      openChannel(svc).use { stub =>
        stub.getCompletionByHash(hashRequest).map { _ =>
          capturedByHashParties.get() shouldBe Some(Set.empty[Ref.Party])
        }
      }
    }
  }

}
