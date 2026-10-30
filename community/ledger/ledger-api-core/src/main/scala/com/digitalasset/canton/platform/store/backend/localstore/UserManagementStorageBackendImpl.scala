// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.localstore

import anorm.SqlParser.{bool, int, long, str}
import anorm.{RowParser, SqlParser, SqlStringInterpolation, ~}
import com.daml.ledger.api.v2.admin.user_management_service.Right
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.backend.common.{ComposableQuery, QueryStrategy}
import com.digitalasset.canton.platform.{LedgerString, Party, UserId}
import com.digitalasset.canton.tea.AccountId
import com.digitalasset.canton.user.UserRight.{
  CanActAs,
  CanActAsAnyParty,
  CanDebitTrafficFrom,
  CanDebitTrafficFromAnyAccount,
  CanExecuteAs,
  CanExecuteAsAnyParty,
  CanReadAs,
  CanReadAsAnyParty,
  IdentityProviderAdmin,
  ParticipantAdmin,
}
import com.digitalasset.canton.user.{IdentityProviderId, UserRight}

import java.sql.Connection
import scala.util.Try

object UserManagementStorageBackendImpl extends UserManagementStorageBackend {

  private val ParticipantUserParser
      : RowParser[(Int, String, Option[String], Option[String], Boolean, Boolean, Long, Long)] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.bigDecimalColumnToBoolean
    int("internal_id") ~
      str("user_id") ~
      str("primary_party").? ~
      str("identity_provider_id").? ~
      bool("is_deactivated") ~
      bool("primary_party_authentication") ~
      long("resource_version") ~
      long("created_at") map {
        case internalId ~ userId ~ primaryParty ~ identityProviderId ~ isDeactivated ~ primaryPartyAuthentication ~ resourceVersion ~ createdAt =>
          (
            internalId,
            userId,
            primaryParty,
            identityProviderId,
            isDeactivated,
            primaryPartyAuthentication,
            resourceVersion,
            createdAt,
          )
      }
  }

  private val UserRightParser: RowParser[(Int, Option[String], Option[String], Long)] =
    int("user_right") ~ str("for_party").? ~ str("account_id").? ~ long("granted_at") map {
      case userRight ~ forParty ~ accountId ~ grantedAt =>
        (userRight, forParty, accountId, grantedAt)
    }

  private val IntParser0: RowParser[Int] =
    int("dummy") map { i => i }

  override def createUser(
      user: UserManagementStorageBackend.DbUserPayload
  )(connection: Connection): Int = {
    val id = user.id: String
    val primaryParty = user.primaryPartyO: Option[String]
    val identityProviderId = user.identityProviderId.map(_.value): Option[String]
    val isDeactivated = user.isDeactivated
    val primaryPartyAuthentication = user.primaryPartyAuthentication
    val resourceVersion = user.resourceVersion
    val createdAt = user.createdAt
    val internalId: Try[Int] =
      SQL"""
         INSERT INTO lapi_users (user_id, primary_party, identity_provider_id, is_deactivated, primary_party_authentication, resource_version, created_at)
         VALUES ($id, $primaryParty, $identityProviderId, $isDeactivated, $primaryPartyAuthentication, $resourceVersion, $createdAt)
       """.executeInsert1("internal_id")(SqlParser.scalar[Int].single)(connection)
    internalId.fold(throw _, identity)
  }

  override def addUserAnnotation(internalId: Int, key: String, value: String, updatedAt: Long)(
      connection: Connection
  ): Unit =
    ParticipantMetadataBackend.addAnnotation("lapi_user_annotations")(
      internalId,
      key,
      value,
      updatedAt,
    )(connection)

  override def deleteUserAnnotations(internalId: Int)(connection: Connection): Unit =
    ParticipantMetadataBackend.deleteAnnotations("lapi_user_annotations")(internalId)(
      connection
    )

  override def getUserAnnotations(internalId: Int)(connection: Connection): Map[String, String] =
    ParticipantMetadataBackend.getAnnotations("lapi_user_annotations")(internalId)(
      connection
    )

  override def compareAndIncreaseResourceVersion(
      internalId: Int,
      expectedResourceVersion: Long,
  )(connection: Connection): Boolean =
    ParticipantMetadataBackend.compareAndIncreaseResourceVersion("lapi_users")(
      internalId,
      expectedResourceVersion,
    )(connection)

  override def increaseResourceVersion(internalId: Int)(
      connection: Connection
  ): Boolean =
    ParticipantMetadataBackend.increaseResourceVersion("lapi_users")(internalId)(connection)

  override def getUser(
      id: UserId
  )(connection: Connection): Option[UserManagementStorageBackend.DbUserWithId] =
    SQL"""
       SELECT internal_id, user_id, primary_party, is_deactivated, identity_provider_id, primary_party_authentication, resource_version, created_at
       FROM lapi_users
       WHERE user_id = ${id: String}
       """
      .as(ParticipantUserParser.singleOpt)(connection)
      .map {
        case (
              internalId,
              userId,
              primaryPartyRaw,
              identityProviderId,
              isDeactivated,
              primaryPartyAuthentication,
              resourceVersion,
              createdAt,
            ) =>
          UserManagementStorageBackend.DbUserWithId(
            internalId = internalId,
            payload = UserManagementStorageBackend.DbUserPayload(
              id = UserId.assertFromString(userId),
              primaryPartyO = dbStringToPartyString(primaryPartyRaw),
              identityProviderId = dbStringToIdentityProviderId(identityProviderId),
              isDeactivated = isDeactivated,
              primaryPartyAuthentication = primaryPartyAuthentication,
              resourceVersion = resourceVersion,
              createdAt = createdAt,
            ),
          )
      }

  override def getUsersOrderedById(
      fromExcl: Option[UserId],
      maxResults: Int,
      identityProviderId: IdentityProviderId,
  )(
      connection: Connection
  ): Vector[UserManagementStorageBackend.DbUserWithId] = {
    import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val userIdWhereClause = fromExcl match {
      case None => Nil
      case Some(id: String) => List(cSQL"user_id > $id")
    }
    val identityProviderIdWhereClause = identityProviderId match {
      case IdentityProviderId.Default =>
        List(cSQL"identity_provider_id is NULL")
      case IdentityProviderId.Id(value) =>
        List(cSQL"identity_provider_id=${value: String}")
    }
    val whereClause = {
      val clauses = userIdWhereClause ++ identityProviderIdWhereClause
      if (clauses.nonEmpty) {
        cSQL"WHERE ${clauses.mkComposite("", " AND ", "")}"
      } else
        cSQL""
    }
    SQL"""SELECT internal_id, user_id, primary_party, identity_provider_id, is_deactivated, primary_party_authentication, resource_version, created_at
          FROM lapi_users
          $whereClause
          ORDER BY user_id
          ${QueryStrategy.limitClause(Some(maxResults))}"""
      .asVectorOf(ParticipantUserParser)(connection)
      .map {
        case (
              internalId,
              userId,
              primaryPartyRaw,
              identityProviderId,
              isDeactivated,
              primaryPartyAuthentication,
              resourceVersion,
              createdAt,
            ) =>
          UserManagementStorageBackend.DbUserWithId(
            internalId = internalId,
            payload = UserManagementStorageBackend.DbUserPayload(
              id = UserId.assertFromString(userId),
              primaryPartyO = dbStringToPartyString(primaryPartyRaw),
              identityProviderId = dbStringToIdentityProviderId(identityProviderId),
              isDeactivated = isDeactivated,
              primaryPartyAuthentication = primaryPartyAuthentication,
              resourceVersion = resourceVersion,
              createdAt = createdAt,
            ),
          )
      }
  }

  override def deleteUser(userId: UserId)(connection: Connection): Boolean = {
    val updatedRowsCount =
      SQL"""
         DELETE FROM lapi_users WHERE user_id = ${userId: String}
         """.executeUpdate()(connection)
    updatedRowsCount == 1
  }

  override def userRightExists(internalId: Int, right: UserRight)(
      connection: Connection
  ): Boolean = {
    val (userRight: Int, forParty: Option[Party], accountId: Option[String]) =
      fromUserRight(right)

    import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val res: Seq[?] =
      SQL"""
         SELECT 1 AS dummy
         FROM lapi_user_rights ur
         WHERE ur.user_internal_id = $internalId
               AND
               ur.user_right = $userRight
               AND
               ur.for_party ${isNullOrEqualPredicate(forParty)}
               AND
               ur.account_id ${isNullOrEqualPredicate(accountId)}"""
        .asVectorOf(IntParser0)(connection)
    assert(res.sizeIs <= 1)
    res.sizeIs == 1
  }

  override def addUserRight(internalId: Int, right: UserRight, grantedAt: Long)(
      connection: Connection
  ): Unit = {
    val (userRight: Int, forParty: Option[Party], accountId: Option[String]) =
      fromUserRight(right)
    val _ =
      SQL"""
         INSERT INTO lapi_user_rights (user_internal_id, user_right, for_party, account_id, granted_at)
         VALUES (
            $internalId,
            $userRight,
            ${forParty: Option[String]},
            $accountId,
            $grantedAt
         )
         """.executeUpdate()(connection)
  }

  override def getUserRights(
      internalId: Int
  )(connection: Connection): Set[UserManagementStorageBackend.DbUserRight] = {
    val rec =
      SQL"""
         SELECT ur.user_right, ur.for_party, ur.account_id, ur.granted_at
         FROM lapi_user_rights ur
         WHERE ur.user_internal_id = $internalId
         """.asVectorOf(UserRightParser)(connection)
    rec.map { case (userRight, forPartyRaw, accountIdRaw, grantedAt) =>
      UserManagementStorageBackend.DbUserRight(
        makeUserRight(
          value = userRight,
          partyRaw = forPartyRaw,
          accountIdRaw = accountIdRaw,
        ),
        grantedAt = grantedAt,
      )
    }.toSet
  }

  override def deleteUserRight(internalId: Int, right: UserRight)(
      connection: Connection
  ): Boolean = {
    val (userRight: Int, forParty: Option[Party], accountId: Option[String]) =
      fromUserRight(right)

    import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val updatedRowCount: Int =
      SQL"""
           DELETE FROM lapi_user_rights ur
           WHERE
            ur.user_internal_id = $internalId
            AND
            ur.user_right = $userRight
            AND
            ur.for_party ${isNullOrEqualPredicate(forParty)}
            AND
            ur.account_id ${isNullOrEqualPredicate(accountId)}
           """.executeUpdate()(connection)
    updatedRowCount == 1
  }

  override def countUserRights(internalId: Int)(connection: Connection): Int =
    SQL"SELECT count(*) AS user_rights_count from lapi_user_rights WHERE user_internal_id = $internalId"
      .as(SqlParser.int("user_rights_count").single)(connection)

  private def makeUserRight(
      value: Int,
      partyRaw: Option[String],
      accountIdRaw: Option[String],
  ): UserRight = {
    val partyO = dbStringToPartyString(partyRaw)
    (value, partyO, accountIdRaw) match {
      case (Right.PARTICIPANT_ADMIN_FIELD_NUMBER, None, None) => ParticipantAdmin
      case (Right.CAN_ACT_AS_FIELD_NUMBER, Some(party), None) => CanActAs(party)
      case (Right.CAN_READ_AS_FIELD_NUMBER, Some(party), None) => CanReadAs(party)
      case (Right.CAN_EXECUTE_AS_FIELD_NUMBER, Some(party), None) => CanExecuteAs(party)
      case (Right.IDENTITY_PROVIDER_ADMIN_FIELD_NUMBER, None, None) => IdentityProviderAdmin
      case (Right.CAN_READ_AS_ANY_PARTY_FIELD_NUMBER, None, None) => CanReadAsAnyParty
      case (Right.CAN_EXECUTE_AS_ANY_PARTY_FIELD_NUMBER, None, None) => CanExecuteAsAnyParty
      case (Right.CAN_ACT_AS_ANY_PARTY_FIELD_NUMBER, None, None) => CanActAsAnyParty
      case (Right.CAN_DEBIT_TRAFFIC_FROM_FIELD_NUMBER, None, Some(accountId)) =>
        CanDebitTrafficFrom(AccountId.tryCreate(accountId))
      case (Right.CAN_DEBIT_TRAFFIC_FROM_ANY_ACCOUNT_FIELD_NUMBER, None, None) =>
        CanDebitTrafficFromAnyAccount
      case _ =>
        throw new RuntimeException(
          s"Could not convert ${(value, partyO, accountIdRaw)} to a user right."
        )
    }
  }

  private def fromUserRight(right: UserRight): (Int, Option[Party], Option[String]) =
    right match {
      case ParticipantAdmin => (Right.PARTICIPANT_ADMIN_FIELD_NUMBER, None, None)
      case IdentityProviderAdmin => (Right.IDENTITY_PROVIDER_ADMIN_FIELD_NUMBER, None, None)
      case CanActAs(party) => (Right.CAN_ACT_AS_FIELD_NUMBER, Some(party), None)
      case CanReadAs(party) => (Right.CAN_READ_AS_FIELD_NUMBER, Some(party), None)
      case CanReadAsAnyParty => (Right.CAN_READ_AS_ANY_PARTY_FIELD_NUMBER, None, None)
      case CanExecuteAs(party) => (Right.CAN_EXECUTE_AS_FIELD_NUMBER, Some(party), None)
      case CanExecuteAsAnyParty => (Right.CAN_EXECUTE_AS_ANY_PARTY_FIELD_NUMBER, None, None)
      case CanActAsAnyParty => (Right.CAN_ACT_AS_ANY_PARTY_FIELD_NUMBER, None, None)
      case CanDebitTrafficFrom(accountId) =>
        (Right.CAN_DEBIT_TRAFFIC_FROM_FIELD_NUMBER, None, Some(accountId.unwrap))
      case CanDebitTrafficFromAnyAccount =>
        (Right.CAN_DEBIT_TRAFFIC_FROM_ANY_ACCOUNT_FIELD_NUMBER, None, None)
      case _ =>
        throw new RuntimeException(s"Could not recognize user right: $right.")
    }

  private def dbStringToPartyString(raw: Option[String]): Option[Party] =
    raw.map(Party.assertFromString)

  private def dbStringToIdentityProviderId(
      raw: Option[String]
  ): Option[IdentityProviderId.Id] =
    raw.map(LedgerString.assertFromString).map(IdentityProviderId.Id.apply)

  private def isNullOrEqualPredicate(value: Option[String]): ComposableQuery.CompositeSql = {
    import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    value.fold(cSQL"IS NULL") { v =>
      cSQL"= $v"
    }
  }

  override def updateUserPrimaryParty(internalId: Int, primaryPartyO: Option[Party])(
      connection: Connection
  ): Boolean = {
    val rowsUpdated = SQL"""
         UPDATE lapi_users
         SET primary_party  = ${primaryPartyO: Option[String]}
         WHERE
             internal_id = $internalId
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def updateUserIsDeactivated(internalId: Int, isDeactivated: Boolean)(
      connection: Connection
  ): Boolean = {
    val rowsUpdated = SQL"""
         UPDATE lapi_users
         SET is_deactivated  = $isDeactivated
         WHERE
             internal_id = $internalId
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def updateUserIdp(internalId: Int, identityProviderId: Option[IdentityProviderId.Id])(
      connection: Connection
  ): Boolean = {
    val idpId = identityProviderId.map(_.value): Option[String]
    val rowsUpdated =
      SQL"""
         UPDATE lapi_users
         SET identity_provider_id = $idpId
         WHERE
             internal_id = $internalId
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def updateUserPrimaryPartyAuthentication(
      internalId: Int,
      primaryPartyAuthentication: Boolean,
  )(
      connection: Connection
  ): Boolean = {
    val rowsUpdated = SQL"""
         UPDATE lapi_users
         SET primary_party_authentication = $primaryPartyAuthentication
         WHERE
             internal_id = $internalId
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

}
