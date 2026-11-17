// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta as ProtoObjectMeta
import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.platform.v1.page_tokens.ListUsersPageTokenPayload
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.concurrent.DirectExecutionContext.directExecutionContext
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.api.sizeconfig.v2.admin.{
  CreateUserRequestSizeConfig,
  GrantUserRightsRequestSizeConfig,
  ObjectMetaSizeConfig,
  RevokeUserRightsRequestSizeConfig,
}
import com.digitalasset.canton.ledger.api.validation.LedgerApiSizeLimitsConfig
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.localstore.api.PartyRecordStore
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.user.store.{IdentityProviderConfigStore, UserManagementStore}
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask
import io.grpc.StatusRuntimeException
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.event.Level

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.concurrent.Future

class ApiUserManagementServiceSpec
    extends AnyFlatSpec
    with BaseTest
    with EitherValues
    with ErrorsAssertions
    with MockitoSugar {

  it should "test users page token encoding and decoding" in {
    val id2 = Ref.UserId.assertFromString("user2")
    val actualNextPageToken = ApiUserManagementService.encodeNextPageToken(Some(id2))
    actualNextPageToken shouldBe "CgV1c2VyMg=="
    ApiUserManagementService.decodeUserIdFromPageToken(actualNextPageToken) shouldBe Right(
      Some(id2)
    )
  }

  it should "test users empty page token encoding and decoding" in {
    val actualNextPageToken = ApiUserManagementService.encodeNextPageToken(None)
    actualNextPageToken shouldBe ("")
    ApiUserManagementService.decodeUserIdFromPageToken(actualNextPageToken) shouldBe Right(None)
  }

  it should "return invalid argument error when token is not a base64" in {
    val actualNextPageToken =
      ApiUserManagementService.decodeUserIdFromPageToken("not-a-base64-string!!")
    val error = actualNextPageToken.left.value
    assertError(
      actual = error,
      expected = RequestValidationErrors.InvalidArgument
        .Reject("Invalid page token")
        .asGrpcError,
    )
  }

  it should "return invalid argument error when token is base64 but not a valid protobuf" in {
    val notValidProtoBufBytes = "not a valid proto buf".getBytes()
    val badPageToken = new String(
      Base64.getEncoder.encode(notValidProtoBufBytes),
      StandardCharsets.UTF_8,
    )

    val actualNextPageToken =
      ApiUserManagementService.decodeUserIdFromPageToken(badPageToken)
    val error = actualNextPageToken.left.value
    assertError(
      actual = error,
      expected = RequestValidationErrors.InvalidArgument
        .Reject("Invalid page token")
        .asGrpcError,
    )
  }

  it should "return invalid argument error when token is valid base64 encoded protobuf but does not contain a valid user id string" in {
    val notValidUserId = "not a valid user id"
    Ref.UserId.fromString(notValidUserId).isLeft shouldBe true
    val payload = ListUsersPageTokenPayload(
      userIdLowerBoundExcl = notValidUserId
    )
    val badPageToken = new String(
      Base64.getEncoder.encode(payload.toByteArray),
      StandardCharsets.UTF_8,
    )

    val actualNextPageToken =
      ApiUserManagementService.decodeUserIdFromPageToken(badPageToken)
    val error = actualNextPageToken.left.value
    assertError(
      actual = error,
      expected = RequestValidationErrors.InvalidArgument
        .Reject("Invalid page token")
        .asGrpcError,
    )
  }

  private val maxElements = 1
  private val apiServiceWithLimits = {
    implicit val ec: DirectExecutionContext = directExecutionContext
    val defaultSizeLimitsConfig = LedgerApiSizeLimitsConfig()
    implicit val sizeLimitsConfig: LedgerApiSizeLimitsConfig = defaultSizeLimitsConfig.copy(
      adminLapiSizeConfig = defaultSizeLimitsConfig.adminLapiSizeConfig.copy(
        createUserRequest = CreateUserRequestSizeConfig(
          rightsSizeLimit = PositiveInt.tryCreate(maxElements)
        ),
        grantUserRightsRequest = GrantUserRightsRequestSizeConfig(
          rightsSizeLimit = PositiveInt.tryCreate(maxElements)
        ),
        revokeUserRightsRequest = RevokeUserRightsRequestSizeConfig(
          rightsSizeLimit = PositiveInt.tryCreate(maxElements)
        ),
        objectMeta = ObjectMetaSizeConfig(
          annotationsSizeLimit = PositiveInt.tryCreate(maxElements)
        ),
      )
    )
    new ApiUserManagementService(
      userManagementStore = mock[UserManagementStore],
      identityProviderExists = new IdentityProviderExists(mock[IdentityProviderConfigStore]),
      partyRecordExist = new PartyRecordsExist(mock[PartyRecordStore]),
      maxUsersPageSize = 100,
      submissionIdGenerator = SubmissionIdGenerator.Random,
      loggerFactory = loggerFactory,
    )
  }

  // no claims in the context, so resolving the authenticated user logs an error
  private def mustFailWith[T](result: => Future[T]) =
    loggerFactory.suppress(
      SuppressionRule.LoggerNameContains("ApiUserManagementService") &&
        SuppressionRule.Level(Level.ERROR)
    ) {
      val error = result.failed.futureValue
      inside(error) { case e: StatusRuntimeException =>
        assertMatchesErrorCode(e, RequestValidationErrors.FieldCardinalityLimitExceeded)
      }
    }

  private val rights = Seq(
    proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs("party1"))),
    proto.Right(proto.Right.Kind.CanReadAs(proto.Right.CanReadAs("party2"))),
  )
  private val metadata = Some(ProtoObjectMeta("", Map("key1" -> "value1", "key2" -> "value2")))
  private def user(metadata: Option[ProtoObjectMeta]) = proto.User(
    id = "user1",
    primaryParty = "",
    isDeactivated = false,
    metadata = metadata,
    identityProviderId = "",
    primaryPartyAuthentication = false,
  )

  it should "reject a user creation exceeding the rights limit" in {
    mustFailWith(
      apiServiceWithLimits.createUser(
        proto.CreateUserRequest(user = Some(user(None)), rights = rights)
      )
    )
  }

  it should "reject a user update exceeding the annotations limit" in {
    mustFailWith(
      apiServiceWithLimits.updateUser(
        proto.UpdateUserRequest(
          user = Some(user(metadata)),
          updateMask = Some(FieldMask(Seq("metadata"))),
        )
      )
    )
  }

  it should "reject a rights grant exceeding the rights limit" in {
    mustFailWith(
      apiServiceWithLimits.grantUserRights(
        proto.GrantUserRightsRequest(userId = "user1", rights = rights, identityProviderId = "")
      )
    )
  }

  it should "reject a rights revocation exceeding the rights limit" in {
    mustFailWith(
      apiServiceWithLimits.revokeUserRights(
        proto.RevokeUserRightsRequest(userId = "user1", rights = rights, identityProviderId = "")
      )
    )
  }
}
