// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.StandardJWTTokenFormat
import com.daml.ledger.api.v2.admin as admin_proto
import com.daml.ledger.api.v2.admin.identity_provider_config_service.UpdateIdentityProviderConfigRequest
import com.daml.ledger.api.v2.admin.user_management_service as user_management_service_proto
import com.daml.ledger.api.v2.admin.user_management_service.{Right, UpdateUserRequest}
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc
import com.daml.ledger.api.v2.commands.Command as ApiCommand
import com.daml.ledger.api.v2.state_service.{GetLedgerEndRequest, StateServiceGrpc}
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse.Update
import com.daml.ledger.api.v2.update_service.{
  GetUpdatesRequest,
  GetUpdatesResponse,
  UpdateServiceGrpc,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.auth.AuthorizationChecksErrors.Unauthenticated
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitAndWaitDummyCommandHelpers
import com.digitalasset.canton.integration.util.UpdateFormatHelpers.getUpdateFormat
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.DelayUtil
import com.google.protobuf.field_mask.FieldMask
import io.grpc.stub.StreamObserver
import io.grpc.{Context, Status, StatusRuntimeException}

import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*

final class OngoingStreamAuthIT
    extends ServiceCallAuthTests
    with IdentityProviderConfigAuth
    with SubmitAndWaitDummyCommandHelpers
    with ErrorsAssertions {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "OngoingStreamAuthorizer"

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  private val testId = UUID.randomUUID().toString
  val partyAlice = "alice-party-1"
  val partyAlice2 = "alice-party-2"

  protected override def prerequisiteParties: List[String] = List(partyAlice, partyAlice2)

  serviceCallName should {
    "abort an ongoing stream after user state has changed" taggedAs securityAsset.setAttack(
      streamAttack(threat = "Continue privileged stream access after revocation of rights")
    ) in { implicit env =>
      import env.*
      val userIdAlice = testId + "-alice"
      val receivedTransactionsCount = new AtomicInteger(0)
      val transactionStreamAbortedPromise = Promise[Throwable]()

      def observeTransactionsStream(
          token: Option[String],
          party: PartyId,
      ): Unit = {
        val observer = new StreamObserver[GetUpdatesResponse] {
          override def onNext(value: GetUpdatesResponse): Unit = {
            val _ = receivedTransactionsCount.incrementAndGet()
          }

          override def onError(t: Throwable): Unit = {
            val _ = transactionStreamAbortedPromise.trySuccess(t)
          }

          override def onCompleted(): Unit = ()
        }
        val request = new GetUpdatesRequest(
          beginExclusive = participantBegin,
          endInclusive = None,
          updateFormat = Some(getUpdateFormat(Set(party))),
          descendingOrder = false,
        )
        val _ = stub(UpdateServiceGrpc.stub(channel), token)
          .getUpdates(request, observer)
      }

      val partyAliceId = env.participant1.parties.find(partyAlice)

      val canActAsAlice = Right(Right.Kind.CanActAs(Right.CanActAs(partyAliceId.toProtoPrimitive)))
      val f = for {
        (userAlice, aliceContext) <- createUserByAdmin(
          userId = userIdAlice,
          rights = Vector(canActAsAlice),
        )
        userId = userAlice.id
        submitAndWaitF = () =>
          submitAndWait(
            token = aliceContext.token,
            party = partyAliceId.toProtoPrimitive,
            userId = userId,
          )
        _ <- submitAndWaitF()
        _ = observeTransactionsStream(aliceContext.token, partyAliceId)
        _ <- submitAndWaitF()
        // Making a change to the user Alice
        _ <- grantUserRightsByAdmin(
          userId = userIdAlice,
          Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin())),
        )
        _ <- DelayUtil
          .delay((UserManagementCacheExpiryInSeconds + 1).second)
          .flatMap(_ =>
            Future(
              transactionStreamAbortedPromise.tryFailure(
                new AssertionError("Timed-out waiting while waiting for stream to abort")
              )
            )
          )
        t <- transactionStreamAbortedPromise.future
      } yield {
        t match {
          case sre: StatusRuntimeException =>
            assertError(
              actual = sre,
              expectedStatusCode = Status.Code.ABORTED,
              expectedMessage =
                "STALE_STREAM_AUTHORIZATION(2,0): Stale stream authorization. Retry quickly.",
              expectedDetails = List(
                ErrorDetails.ErrorInfoDetail(
                  "STALE_STREAM_AUTHORIZATION",
                  Map(
                    "participant" -> s"${participant1.name}",
                    "test" -> s"${this.getClass.getSimpleName}",
                    "category" -> "2",
                    "definite_answer" -> "false",
                  ),
                ),
                ErrorDetails.RetryInfoDetail(0.nanoseconds),
              ),
              verifyEmptyStackTrace = false,
            )
          case _ => fail("Unexpected error", t)
        }
        assert(receivedTransactionsCount.get() >= 2)
      }
      f.futureValue
    }

    "abort an ongoing stream after user has been deactivated" taggedAs securityAsset
      .setAttack(
        streamAttack(threat = "Continue privileged stream access after user deactivation")
      ) in { implicit env =>
      import env.*
      val userIdAlice = testId + "-alice-2"
      val receivedTransactionsCount = new AtomicInteger(0)
      val transactionStreamAbortedPromise = Promise[Throwable]()

      def observeTransactionsStream(
          token: Option[String],
          party: PartyId,
      ): Unit = {
        val observer = new StreamObserver[GetUpdatesResponse] {
          override def onNext(value: GetUpdatesResponse): Unit = {
            val _ = receivedTransactionsCount.incrementAndGet()
          }

          override def onError(t: Throwable): Unit = {
            val _ = transactionStreamAbortedPromise.trySuccess(t)
          }

          override def onCompleted(): Unit = ()
        }
        val request = new GetUpdatesRequest(
          beginExclusive = participantBegin,
          endInclusive = None,
          updateFormat = Some(getUpdateFormat(Set(party))),
          descendingOrder = false,
        )
        val _ = stub(UpdateServiceGrpc.stub(channel), token)
          .getUpdates(request, observer)
      }

      val partyAlice2Id = env.participant1.parties.find(partyAlice2)

      val canActAsAlice = Right(Right.Kind.CanActAs(Right.CanActAs(partyAlice2Id.toProtoPrimitive)))
      val f = for {
        (userAlice, aliceContext) <- createUserByAdmin(
          userId = userIdAlice,
          rights = Vector(canActAsAlice),
        )
        userId = userAlice.id
        submitAndWaitF = () =>
          submitAndWait(
            token = aliceContext.token,
            party = partyAlice2Id.toProtoPrimitive,
            userId = userId,
          )
        _ <- submitAndWaitF()
        _ = observeTransactionsStream(aliceContext.token, partyAlice2Id)
        _ <- submitAndWaitF()
        // Deactivating Alice user:
        _ <- deactivateUserByAdmin(userId = userIdAlice)
        _ <- DelayUtil
          .delay((UserManagementCacheExpiryInSeconds + 1).second)
          .flatMap(_ =>
            Future(
              transactionStreamAbortedPromise.tryFailure(
                new AssertionError("Timed-out waiting while waiting for stream to abort")
              )
            )
          )
        t <- transactionStreamAbortedPromise.future
      } yield {
        t match {
          case sre: StatusRuntimeException =>
            assertError(
              actual = sre,
              expectedStatusCode = Status.Code.ABORTED,
              expectedMessage =
                "STALE_STREAM_AUTHORIZATION(2,0): Stale stream authorization. Retry quickly.",
              expectedDetails = List(
                ErrorDetails.ErrorInfoDetail(
                  "STALE_STREAM_AUTHORIZATION",
                  Map(
                    "participant" -> s"${participant1.name}",
                    "test" -> s"${this.getClass.getSimpleName}",
                    "category" -> "2",
                    "definite_answer" -> "false",
                  ),
                ),
                ErrorDetails.RetryInfoDetail(0.nanoseconds),
              ),
              verifyEmptyStackTrace = false,
            )
          case _ => fail("Unexpected error", t)
        }
        assert(receivedTransactionsCount.get() >= 2)
      }
      f.futureValue
    }

    "abort an ongoing stream after the user's identity provider has been deactivated" taggedAs securityAsset
      .setAttack(
        streamAttack(threat =
          "Continue privileged stream access after deactivation of identity provider"
        )
      ) in { implicit env =>
      import env.*

      val alice3UserId = s"alice-3-user"
      val alice3PartyIdHint = s"alice-3-party"

      val identityProviderConfig = createConfig(
        canBeAnAdmin,
        idpId = Some("my-idp-id"),
        audience = Some("my-idp-audience"),
        issuer = Some("my-idp-issuer"),
      ).futureValue
      val alice3PartyId = allocateParty(
        canBeAnAdmin,
        alice3PartyIdHint,
        identityProviderIdOverride = Some(identityProviderConfig.identityProviderId),
      ).futureValue
      val alice3Token = Option(
        toHeaderRSA(
          keyId = key1.id,
          payload = audienceToken(
            alice3UserId,
            audience = List(identityProviderConfig.audience),
            issuer = Some(identityProviderConfig.issuer),
            expiresIn = Some(Duration.ofMinutes(5)),
          ),
          privateKey = key1.privateKey,
          enforceFormat = Some(StandardJWTTokenFormat.Audience),
        )
      )
      val aliceRight = Right(Right.Kind.CanActAs(Right.CanActAs(alice3PartyId)))
      val (_, alice3Context) = createUser(
        alice3UserId,
        alice3Token,
        identityProviderConfig.identityProviderId,
        Vector(aliceRight),
        primaryParty = "",
      ).futureValue
      val ledgerEnd = stub(StateServiceGrpc.stub(channel), canBeAnAdmin.token)
        .getLedgerEnd(GetLedgerEndRequest())
        .map(_.offset)
        .futureValue
      val (firstSeenF, secondSeenF) = observePartyUpdates(alice3Context, ledgerEnd, alice3PartyId)
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        submitObservedIou(
          alice3Context,
          alice3UserId,
          alice3PartyId,
          alice3PartyId,
        ).futureValue
      )
      firstSeenF.futureValue
      idpStub(canBeAnAdmin)
        .updateIdentityProviderConfig(
          UpdateIdentityProviderConfigRequest(
            identityProviderConfig = Some(identityProviderConfig.copy(isDeactivated = true)),
            updateMask = Some(FieldMask(Seq("is_deactivated"))),
          )
        )
        .futureValue

      // Fresh calls with the same IDP-issued token now fail, proving IDP deactivation took effect.
      inside(
        loggerFactory.assertLogs(
          stub(StateServiceGrpc.stub(channel), alice3Context.token)
            .getLedgerEnd(GetLedgerEndRequest())
            .failed
            .futureValue,
          _.warningMessage should fullyMatch regex raw"Authorization failed: the provided token was not verified by any configured authentication service: " +
            raw"\(1\) \[AuthServiceJWT\] Authorization error: Could not verify JWT token: The provided Algorithm doesn't match the one defined in the JWT's Header\.; " +
            raw"\(2\) \[IdentityProviderAwareAuthService\] Failed to authorize the token: Identity Provider \S+ is deactivated\.",
          _.shouldBeCantonError(
            Unauthenticated,
            _ shouldBe "The command is missing a (valid) JWT token",
          ),
        )
      ) { case ex: StatusRuntimeException =>
        ex.getStatus.getCode shouldBe Status.Code.UNAUTHENTICATED
        ex.getMessage should fullyMatch regex raw"UNAUTHENTICATED: An error occurred\. Please contact the operator and inquire about the request \S+ with tid \S+"
        ex.getCause shouldBe null
      }

      inside(secondSeenF.failed.futureValue) { case sre: StatusRuntimeException =>
        assertError(
          actual = sre,
          expectedStatusCode = Status.Code.ABORTED,
          expectedMessage =
            "STALE_STREAM_AUTHORIZATION(2,0): Stale stream authorization. Retry quickly.",
          expectedDetails = List(
            ErrorDetails.ErrorInfoDetail(
              "STALE_STREAM_AUTHORIZATION",
              Map(
                "participant" -> s"${participant1.name}",
                "test" -> s"${this.getClass.getSimpleName}",
                "category" -> "2",
                "definite_answer" -> "false",
              ),
            ),
            ErrorDetails.RetryInfoDetail(0.nanoseconds),
          ),
          verifyEmptyStackTrace = false,
        )
      }
    }
  }

  private def deactivateUserByAdmin(userId: String)(implicit ec: ExecutionContext): Future[Unit] =
    stub(
      user_management_service_proto.UserManagementServiceGrpc.stub(channel),
      canBeAnAdmin.token,
    )
      .updateUser(
        UpdateUserRequest(
          user = Some(
            user_management_service_proto.User(
              id = userId,
              primaryParty = "",
              isDeactivated = true,
              metadata = Some(admin_proto.object_meta.ObjectMeta.defaultInstance),
              identityProviderId = "",
              primaryPartyAuthentication = false,
            )
          ),
          updateMask = Some(
            FieldMask(
              paths = Seq("is_deactivated")
            )
          ),
        )
      )
      .map(_ => ())

  private def grantUserRightsByAdmin(
      userId: String,
      right: user_management_service_proto.Right,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val req = user_management_service_proto.GrantUserRightsRequest(
      userId = userId,
      rights = Seq(right),
      identityProviderId = "",
    )
    stub(
      user_management_service_proto.UserManagementServiceGrpc.stub(channel),
      canBeAnAdmin.token,
    )
      .grantUserRights(req)
      .map(_ => ())
  }

  private def observePartyUpdates(
      context: ServiceCallContext,
      beginExclusive: Long,
      party: String,
  ): (Future[GetUpdatesResponse], Future[GetUpdatesResponse]) = {
    val first = Promise[GetUpdatesResponse]()
    val second = Promise[GetUpdatesResponse]()
    val cancelContext = Context.ROOT.withCancellation()
    val request = GetUpdatesRequest(
      beginExclusive = beginExclusive,
      endInclusive = None,
      updateFormat = updateFormat(transactionsPartyO = Some(party)),
      descendingOrder = false,
    )

    val observer = new StreamObserver[GetUpdatesResponse] {
      private var seen = 0

      override def onNext(value: GetUpdatesResponse): Unit = value.update match {
        case _: Update.Transaction =>
          seen = seen + 1
          if (seen == 1) first.trySuccess(value)
          if (seen == 2) {
            second.trySuccess(value)
            cancelContext.cancel(null)
          }
        case _: Update => // skip
      }

      override def onError(t: Throwable): Unit = {
        first.tryFailure(t)
        second.tryFailure(t)
      }

      override def onCompleted(): Unit = ()
    }

    cancelContext.run(() =>
      stub(UpdateServiceGrpc.stub(channel), context.token).getUpdates(request, observer)
    )
    first.future -> second.future
  }

  private def submitObservedIou(
      context: ServiceCallContext,
      userId: String,
      submitterParty: String,
      observerParty: String,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val command = ApiCommand.fromJavaProto(
      new Iou(
        submitterParty,
        observerParty,
        new Amount(100.toBigDecimal, "USD"),
        List.empty[String].asJava,
      ).create.commands.asScala.toSeq.loneElement.toProtoCommand
    )
    val baseRequest = dummySubmitAndWaitRequest(userId, submitterParty, None)
    val request =
      baseRequest.copy(commands = baseRequest.commands.map(_.copy(commands = Seq(command))))

    stub(CommandServiceGrpc.stub(channel), context.token)
      .submitAndWait(request)
      .map(_ => ())
  }
}
