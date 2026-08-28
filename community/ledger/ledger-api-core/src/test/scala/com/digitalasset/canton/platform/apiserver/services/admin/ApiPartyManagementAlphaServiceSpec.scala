// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.v2.admin.party_management_alpha_service.{
  AuthorizePartyUpdateRequest,
  GeneratePartyTopologyUpdateRequest,
  GeneratePartyTopologyUpdateResponse,
  GetAddPartyStatusRequest,
  GetAddPartyStatusResponse,
  PartyReplicationStatus as LapiPartyReplicationStatus,
}
import com.daml.ledger.api.v2.state_service.ParticipantPermission as ProtoParticipantPermission
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, SuppressionRule}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TestTelemetrySetup
import com.digitalasset.canton.user.store.UserManagementStore
import com.digitalasset.canton.user.{IdentityProviderId, User, UserRight}
import com.digitalasset.canton.{BaseTest, HasExecutorService}
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.{ArgumentMatchers, ArgumentMatchersSugar, Mockito, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Unit test the gRPC API layer for the Party Management Alpha Service
  * (`ApiPartyManagementAlphaService`), focusing on request validation, error handling, and correct
  * delegation to the backend endpoints.
  */
class ApiPartyManagementAlphaServiceSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with ScalaFutures
    with ArgumentMatchersSugar
    with ErrorsAssertions
    with BaseTest
    with BeforeAndAfterEach
    with HasExecutorService {

  var testTelemetrySetup: TestTelemetrySetup = _

  override def beforeEach(): Unit =
    testTelemetrySetup = new TestTelemetrySetup()

  override def afterEach(): Unit =
    testTelemetrySetup.close()

  private implicit val ec: ExecutionContext = directExecutionContext

  val ApiPartyManagementAlphaServiceSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("ApiPartyManagementAlphaService") &&
      SuppressionRule.Level(Level.ERROR)

  // resolvedFromUser = true evaluates the user against standard user limits logic
  private def withUserClaims[T](f: => Future[T]): Future[T] = {
    val claims = com.digitalasset.canton.auth.ClaimSet.Claims(
      claims = Seq.empty,
      participantId = None,
      userId = None,
      expiration = None,
      identityProviderId = None,
      resolvedFromUser = true,
    )
    val context = io.grpc.Context.ROOT.withValue(
      com.digitalasset.canton.auth.AuthInterceptor.contextKeyClaimSet,
      claims,
    )
    val previous = context.attach()
    try f
    finally context.detach(previous)
  }

  private def createService(
      mockEndpoints: PartyReplicationEndpoints,
      mockStore: UserManagementStore,
      mockIdpExists: IdentityProviderExists,
      maxSelfAllocatedParties: Int = 100,
  ) = ApiPartyManagementAlphaService.createApiService(
    mockEndpoints,
    mockStore,
    mockIdpExists,
    NonNegativeInt.tryCreate(maxSelfAllocatedParties),
    new PendingPartyAllocations(),
    loggerFactory,
  )

  "ApiPartyManagementAlphaService" should {

    "validate GetAddPartyStatus request" when {
      "delegating valid requests to endpoints" in {
        val (mockEndpoints, mockStore, mockIdpExists) = mockedServices()

        val expectedResponse = GetAddPartyStatusResponse(
          Some(LapiPartyReplicationStatus(LapiPartyReplicationStatus.State.STATE_IN_PROGRESS, None))
        )

        when(mockEndpoints.getAddPartyStatus(any[GetAddPartyStatusRequest]))
          .thenReturn(Future.successful(expectedResponse))

        val service = createService(mockEndpoints, mockStore, mockIdpExists)
        val request = GetAddPartyStatusRequest(
          partyId = "alice::1220abcd",
          synchronizerId = "da::1220abcd",
          targetParticipantUid = "PAR::target::1220abcd",
        )

        withUserClaims {
          service.getAddPartyStatus(request).map { response =>
            response shouldBe expectedResponse
          }
        }
      }

      "propagating backend NOT_FOUND errors" in {
        val (mockEndpoints, mockStore, mockIdpExists) = mockedServices()

        when(mockEndpoints.getAddPartyStatus(any[GetAddPartyStatusRequest]))
          .thenReturn(
            Future.failed(
              new StatusRuntimeException(
                Status.NOT_FOUND.withDescription("Party replication status not found")
              )
            )
          )

        val service = createService(mockEndpoints, mockStore, mockIdpExists)
        val request =
          GetAddPartyStatusRequest("alice::1220abcd", "da::1220abcd", "PAR::target::1220abcd")

        withUserClaims {
          service.getAddPartyStatus(request).transform {
            case Failure(ex: StatusRuntimeException) =>
              ex.getStatus.getCode shouldBe Status.NOT_FOUND.getCode
              ex.getStatus.getDescription should include("Party replication status not found")
              Success(succeed)
            case other =>
              fail(s"Expected StatusRuntimeException, but got $other")
          }
        }
      }
    }

    "validate GeneratePartyTopologyUpdate request" when {
      "rejecting Submission permission" in {
        val (mockEndpoints, mockStore, mockIdpExists) = mockedServices()

        when(mockEndpoints.generatePartyTopologyUpdate(any[GeneratePartyTopologyUpdateRequest]))
          .thenReturn(
            Future.failed(
              new StatusRuntimeException(
                Status.INVALID_ARGUMENT.withDescription(
                  "External parties cannot be granted Submission permission"
                )
              )
            )
          )

        val service = createService(mockEndpoints, mockStore, mockIdpExists)

        val request = GeneratePartyTopologyUpdateRequest(
          partyId = "alice::1220...",
          synchronizerId = "da::1220...",
          targetParticipantUid = "PAR::participant2::1220...",
          participantPermission = ProtoParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
        )

        service.generatePartyTopologyUpdate(request).transform {
          case Failure(ex: StatusRuntimeException) =>
            ex.getStatus.getCode shouldBe Status.INVALID_ARGUMENT.getCode
            ex.getStatus.getDescription should include(
              "External parties cannot be granted Submission permission"
            )
            Success(succeed)
          case other =>
            fail(s"Expected StatusRuntimeException, but got $other")
        }
      }

      "delegating valid requests to endpoints" in {
        val (mockEndpoints, mockStore, mockIdpExists) = mockedServices()

        val mockHash = Hash.digest(
          HashPurpose.TopologyTransactionSignature,
          ByteString.copyFromUtf8("test"),
          HashAlgorithm.Sha256,
        )
        val mockTxBytes = ByteString.copyFromUtf8("mock-tx")

        when(
          mockEndpoints.generatePartyTopologyUpdate(any[GeneratePartyTopologyUpdateRequest])
        ).thenReturn(
          Future.successful(
            GeneratePartyTopologyUpdateResponse(
              transaction = mockTxBytes,
              hash = mockHash.unwrap,
            )
          )
        )

        val service = createService(mockEndpoints, mockStore, mockIdpExists)

        val request = GeneratePartyTopologyUpdateRequest(
          partyId = "alice::1220...",
          synchronizerId = "da::1220...",
          targetParticipantUid = "PAR::participant2::1220...",
          participantPermission = ProtoParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION,
        )

        service.generatePartyTopologyUpdate(request).map { response =>
          response.transaction shouldBe mockTxBytes
          response.hash shouldBe mockHash.unwrap
        }
      }
    }

    "validate AuthorizePartyUpdate request" when {
      "rejecting empty transactions" in {
        val (mockEndpoints, mockStore, mockIdpExists) = mockedServices()

        when(mockEndpoints.authorizePartyUpdate(any[AuthorizePartyUpdateRequest]))
          .thenReturn(
            Future.failed(
              new StatusRuntimeException(
                Status.INVALID_ARGUMENT.withDescription(
                  "party_to_participant_topology_transaction cannot be empty"
                )
              )
            )
          )

        val service = createService(mockEndpoints, mockStore, mockIdpExists)

        val request = AuthorizePartyUpdateRequest(
          synchronizerId = "da::1220...",
          transaction = ByteString.EMPTY,
          signatures = Seq.empty,
          userId = "",
          identityProviderId = "",
        )

        withUserClaims {
          service.authorizePartyUpdate(request).transform {
            case Failure(ex: StatusRuntimeException) =>
              ex.getStatus.getCode shouldBe Status.INVALID_ARGUMENT.getCode
              ex.getStatus.getDescription should include(
                "party_to_participant_topology_transaction cannot be empty"
              )
              Success(succeed)
            case other =>
              fail(s"Expected StatusRuntimeException, but got $other")
          }
        }
      }

      "propagating backend validation errors (e.g., missing signatures)" in {
        val (mockEndpoints, mockStore, mockIdpExists) = mockedServices()

        when(mockEndpoints.authorizePartyUpdate(any[AuthorizePartyUpdateRequest]))
          .thenReturn(
            Future.failed(
              new StatusRuntimeException(
                Status.INVALID_ARGUMENT.withDescription(
                  "At least one signature from the external party must be provided"
                )
              )
            )
          )

        val service = createService(mockEndpoints, mockStore, mockIdpExists)

        val request = AuthorizePartyUpdateRequest(
          synchronizerId = "da::1220...",
          transaction = ByteString.copyFromUtf8("mock-tx"),
          signatures = Seq.empty,
          userId = "",
          identityProviderId = "",
        )

        withUserClaims {
          service.authorizePartyUpdate(request).transform {
            case Failure(ex: StatusRuntimeException) =>
              ex.getStatus.getCode shouldBe Status.INVALID_ARGUMENT.getCode
              ex.getStatus.getDescription should include(
                "At least one signature from the external party must be provided"
              )
              Success(succeed)
            case other =>
              fail(s"Expected StatusRuntimeException, but got $other")
          }
        }
      }

      "rejects the request during pre-flight checks if the Identity Provider does not exist" in {
        val (mockEndpoints, mockStore, mockIdpExists) = mockedServices()

        val service = createService(mockEndpoints, mockStore, mockIdpExists)

        val dummySignature = com.daml.ledger.api.v2.crypto.Signature(
          format = com.daml.ledger.api.v2.crypto.SignatureFormat.SIGNATURE_FORMAT_RAW,
          signature = ByteString.copyFromUtf8("sig"),
          signedBy = "1220...",
          signingAlgorithmSpec =
            com.daml.ledger.api.v2.crypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519,
        )

        val requestWithInvalidIdp = AuthorizePartyUpdateRequest(
          synchronizerId = "da::1220...",
          transaction = ByteString.copyFromUtf8("mock-tx"),
          signatures = Seq(dummySignature),
          userId = "valid-user",
          identityProviderId = "missing-idp", // Mock is set up to return false for this ID
        )

        withUserClaims {
          service.authorizePartyUpdate(requestWithInvalidIdp).transform {
            case Failure(ex: StatusRuntimeException) =>
              ex.getStatus.getCode shouldBe Status.NOT_FOUND.getCode
              ex.getStatus.getDescription should include("missing-idp")
              Success(succeed)
            case other =>
              fail(s"Expected NOT_FOUND StatusRuntimeException, but got $other")
          }
        }
      }

      "completes successfully and provisions IAM rights when valid user and IDP are provided on target participant" in {
        val (mockEndpoints, mockStore, mockIdpExists) = mockedServices()

        val mockPartyId = PartyId.tryFromProtoPrimitive("alice::1220abcd")
        when(mockEndpoints.authorizePartyUpdate(any[AuthorizePartyUpdateRequest]))
          .thenReturn(Future.successful((mockPartyId, true)))

        val service = createService(mockEndpoints, mockStore, mockIdpExists)

        val dummySignature = com.daml.ledger.api.v2.crypto.Signature(
          format = com.daml.ledger.api.v2.crypto.SignatureFormat.SIGNATURE_FORMAT_RAW,
          signature = ByteString.copyFromUtf8("sig"),
          signedBy = "1220...",
          signingAlgorithmSpec =
            com.daml.ledger.api.v2.crypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519,
        )

        val request = AuthorizePartyUpdateRequest(
          synchronizerId = "da::1220...",
          transaction = ByteString.copyFromUtf8("mock-tx"),
          signatures = Seq(dummySignature),
          userId = "valid-user",
          identityProviderId = "", // Falls back to Default IDP which exists
        )

        withUserClaims {
          service.authorizePartyUpdate(request).map { response =>
            response shouldBe com.daml.ledger.api.v2.admin.party_management_alpha_service
              .AuthorizePartyUpdateResponse()

            verify(mockStore, Mockito.times(1)).grantRights(
              any[Ref.UserId],
              any[Set[UserRight]],
              any[IdentityProviderId],
            )(any[LoggingContextWithTrace])
            succeed
          }
        }
      }

      "completes successfully but skips IAM provisioning when called on a non-target participant" in {
        val (mockEndpoints, mockStore, mockIdpExists) = mockedServices()

        val mockPartyId = PartyId.tryFromProtoPrimitive("alice::1220abcd")
        when(mockEndpoints.authorizePartyUpdate(any[AuthorizePartyUpdateRequest]))
          .thenReturn(Future.successful((mockPartyId, false)))

        val service = createService(mockEndpoints, mockStore, mockIdpExists)

        val request = AuthorizePartyUpdateRequest(
          synchronizerId = "da::1220...",
          transaction = ByteString.copyFromUtf8("mock-tx"),
          signatures = Seq.empty,
          userId = "valid-user",
          identityProviderId = "",
        )

        withUserClaims {
          service.authorizePartyUpdate(request).map { response =>
            response shouldBe com.daml.ledger.api.v2.admin.party_management_alpha_service
              .AuthorizePartyUpdateResponse()

            verify(mockStore, Mockito.never()).grantRights(
              any[Ref.UserId],
              any[Set[UserRight]],
              any[IdentityProviderId],
            )(any[LoggingContextWithTrace])
            succeed
          }
        }
      }

      "completes successfully and skips IAM provisioning when no user ID is provided" in {
        val (mockEndpoints, mockStore, mockIdpExists) = mockedServices()

        val mockPartyId = PartyId.tryFromProtoPrimitive("alice::1220abcd")
        // Even if it is the target participant, empty userId should skip IAM provisioning
        when(mockEndpoints.authorizePartyUpdate(any[AuthorizePartyUpdateRequest]))
          .thenReturn(Future.successful((mockPartyId, true)))

        val service = createService(mockEndpoints, mockStore, mockIdpExists)

        val request = AuthorizePartyUpdateRequest(
          synchronizerId = "da::1220...",
          transaction = ByteString.copyFromUtf8("mock-tx"),
          signatures = Seq.empty,
          userId = "", // Empty User ID
          identityProviderId = "",
        )

        withUserClaims {
          service.authorizePartyUpdate(request).map { response =>
            response shouldBe com.daml.ledger.api.v2.admin.party_management_alpha_service
              .AuthorizePartyUpdateResponse()

            // Verify that IAM provisioning was skipped because userId was empty
            verify(mockStore, Mockito.never()).grantRights(
              any[Ref.UserId],
              any[Set[UserRight]],
              any[IdentityProviderId],
            )(any[LoggingContextWithTrace])
            succeed
          }
        }
      }

      "fails when user quota is exhausted (maxSelfAllocatedParties = 0)" in {
        val (mockEndpoints, mockStore, mockIdpExists) = mockedServices()

        val service =
          createService(mockEndpoints, mockStore, mockIdpExists, maxSelfAllocatedParties = 0)

        val dummySignature = com.daml.ledger.api.v2.crypto.Signature(
          format = com.daml.ledger.api.v2.crypto.SignatureFormat.SIGNATURE_FORMAT_RAW,
          signature = ByteString.copyFromUtf8("sig"),
          signedBy = "1220...",
          signingAlgorithmSpec =
            com.daml.ledger.api.v2.crypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519,
        )

        val request = AuthorizePartyUpdateRequest(
          synchronizerId = "da::1220...",
          transaction = ByteString.copyFromUtf8("mock-tx"),
          signatures = Seq(dummySignature),
          userId = "valid-user",
          identityProviderId = "",
        )

        loggerFactory.assertLogs(
          withUserClaims {
            service.authorizePartyUpdate(request).transform {
              case Failure(ex: StatusRuntimeException) =>
                ex.getStatus.getCode shouldBe Status.PERMISSION_DENIED.getCode
                Success(succeed)
              case other =>
                fail(s"Expected PERMISSION_DENIED StatusRuntimeException, but got $other")
            }
          },
          _.warningMessage should include("User quota of party allocations exhausted"),
        )
      }
    }
  }

  private def mockedServices(): (
      PartyReplicationEndpoints,
      UserManagementStore,
      IdentityProviderExists,
  ) = {
    val mockEndpoints = mock[PartyReplicationEndpoints]

    when(mockEndpoints.getAddPartyStatus(any[GetAddPartyStatusRequest]))
      .thenReturn(Future.failed(new RuntimeException("Not mocked for success")))

    // Generically stub the backend endpoints to return errors by default.
    // This prevents SmartNullPointerExceptions if validation fails to short-circuit,
    // while allowing specific tests to override these stubs with successful responses.
    when(mockEndpoints.generatePartyTopologyUpdate(any[GeneratePartyTopologyUpdateRequest]))
      .thenReturn(Future.failed(new RuntimeException("Not mocked for success")))

    when(mockEndpoints.authorizePartyUpdate(any[AuthorizePartyUpdateRequest]))
      .thenReturn(Future.failed(new RuntimeException("Not mocked for success")))

    val mockIdentityProviderExists = mock[IdentityProviderExists]

    // Mock for the default IDP (returns true)
    when(
      mockIdentityProviderExists.apply(ArgumentMatchers.eq(IdentityProviderId.Default))(
        any[LoggingContextWithTrace]
      )
    ).thenReturn(Future.successful(true))

    // Mock for the "missing-idp" (returns false)
    when(
      mockIdentityProviderExists.apply(
        ArgumentMatchers.eq(IdentityProviderId.Id(Ref.LedgerString.assertFromString("missing-idp")))
      )(any[LoggingContextWithTrace])
    ).thenReturn(Future.successful(false))

    val mockUserManagementStore = mock[UserManagementStore]

    Mockito
      .when(
        mockUserManagementStore.grantRights(
          any[Ref.UserId],
          any[Set[UserRight]],
          any[IdentityProviderId],
        )(any[LoggingContextWithTrace])
      )
      .thenAnswer { _ =>
        Future.successful(Right(()))
      }

    // Mock getUserInfo so user quota checks don't fail preemptively with a UserNotFound
    Mockito
      .when(
        mockUserManagementStore.getUserInfo(any[Ref.UserId], any[IdentityProviderId])(
          any[LoggingContextWithTrace]
        )
      )
      .thenAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
        val userId = invocation.getArgument[Ref.UserId](0)
        val idpId = invocation.getArgument[IdentityProviderId](1)
        Future.successful(
          Right(
            UserManagementStore.UserInfo(
              user = User(userId, None, identityProviderId = idpId),
              // We add a dummy pre-existing right so `resultingRightsCount > max` properly triggers when max = 0
              rights =
                Set(UserRight.CanActAs(Ref.Party.assertFromString("pre-existing-party::1220abcd"))),
            )
          )
        )
      }

    (mockEndpoints, mockUserManagementStore, mockIdentityProviderExists)
  }
}
