// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.AuthServiceJWTCodec.AuthServiceJWTPayloadCodec
import com.daml.jwt.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  Error,
  JwtFromBearerHeader,
  JwtVerifierBase,
  StandardJWTPayload,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.parser

import scala.concurrent.Future
import scala.util.Try

sealed trait AccessLevel extends Product with Serializable

object AccessLevel {
  case object Admin extends AccessLevel
  case object Wildcard extends AccessLevel
}

final case class AuthorizedUser(
    userId: String,
    allowedServices: Seq[String],
)

/** An AuthService that reads a JWT token from a `Authorization: Bearer` HTTP header. The token is
  * expected to use the format as defined in [[com.daml.jwt.AuthServiceJWTPayload]]:
  */
abstract class AuthServiceJWTBase(
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
    generalImplicitDecoder: AuthServiceJWTPayloadCodec,
) extends AuthService
    with NamedLogging {

  override def decodeToken(
      authToken: Option[String],
      serviceName: String,
  )(implicit traceContext: TraceContext): Future[ClaimSet] =
    Future.successful {
      authToken match {
        case None => ClaimSet.Unauthenticated
        case Some(header) => parseHeader(header, serviceName)
      }
    }

  private[this] def parseHeader(header: String, serviceName: String)(implicit
      traceContext: TraceContext
  ): ClaimSet =
    parseJWTPayload(header).fold(
      error => {
        logger.warn("Authorization error: " + error.message)
        ClaimSet.Unauthenticated
      },
      payloadToClaims(serviceName),
    )

  private[this] def parsePayload(jwtPayload: String): Either[Error, AuthServiceJWTPayload] = {
    val parsed = targetAudience match {
      case Some(_) => Try(parseAudienceBasedPayload(jwtPayload))
      case None if targetScope.isDefined => Try(parseScopeBasedPayload(jwtPayload))
      case _ => Try(parseAuthServicePayload(jwtPayload))
    }

    parsed.toEither.left
      .map(t => Error(Symbol("parsePayload"), "Could not parse JWT token: " + t.getMessage))
      .flatMap(checkAudienceAndScope)
  }

  private def checkAudienceAndScope(
      payload: AuthServiceJWTPayload
  ): Either[Error, AuthServiceJWTPayload] =
    (payload, targetAudience, targetScope) match {
      case (payload: StandardJWTPayload, Some(audience), _) =>
        if (payload.audiences.contains(audience))
          Right(payload)
        else
          Left(Error(Symbol("checkAudienceAndScope"), "Audience doesn't match the target value"))
      case (payload: StandardJWTPayload, None, Some(scope)) =>
        if (payload.scope.toList.flatMap(_.split(' ')).contains(scope))
          Right(payload)
        else
          Left(Error(Symbol("checkAudienceAndScope"), "Scope doesn't match the target value"))
      case (payload, None, None) =>
        Right(payload)
      case _ =>
        Left(Error(Symbol("checkAudienceAndScope"), "Could not check the audience"))
    }

  private[this] def parseAuthServicePayload(jwtPayload: String): AuthServiceJWTPayload = {
    import generalImplicitDecoder.*
    parser.decode(jwtPayload).fold(throw _, identity)
  }

  private[this] def parseAudienceBasedPayload(
      jwtPayload: String
  ): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.AudienceBasedTokenJsonImplicits.*
    parser.decode(jwtPayload).fold(throw _, identity)
  }

  private[this] def parseScopeBasedPayload(
      jwtPayload: String
  ): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.ScopeBasedTokenJsonImplicits.*
    parser.decode(jwtPayload).fold(throw _, identity)
  }

  private[this] def parseJWTPayload(header: String): Either[Error, AuthServiceJWTPayload] =
    for {
      token <- JwtFromBearerHeader(header)
      decoded <- verifier
        .verify(com.daml.jwt.Jwt(token))
        .left
        .map(e => Error(Symbol("parseJWTPayload"), "Could not verify JWT token: " + e.message))
      parsed <- parsePayload(decoded.payload)
    } yield parsed

  protected[this] def payloadToClaims(serviceName: String): AuthServiceJWTPayload => ClaimSet
}

class AuthServiceJWT(
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
    val loggerFactory: NamedLoggerFactory,
    warnOnJwtScopeUsage: Boolean,
) extends AuthServiceJWTBase(
      verifier,
      targetAudience,
      targetScope,
      AuthServiceJWTCodec.jsonImplicits(warnOnJwtScopeUsage),
    ) {
  protected[this] def payloadToClaims(serviceName: String): AuthServiceJWTPayload => ClaimSet = {
    case payload: StandardJWTPayload =>
      ClaimSet.AuthenticatedUser(
        identityProviderId = None,
        participantId = payload.participantId,
        userId = payload.userId,
        expiration = payload.exp,
      )
  }
}

class UserConfigAuthService private[auth] (
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
    users: Seq[AuthorizedUser],
    val loggerFactory: NamedLoggerFactory,
    warnOnJwtScopeUsage: Boolean,
) extends AuthServiceJWTBase(
      verifier,
      targetAudience,
      targetScope,
      AuthServiceJWTCodec.jsonImplicits(warnOnJwtScopeUsage),
    ) {
  protected[this] def payloadToClaims(serviceName: String): AuthServiceJWTPayload => ClaimSet = {
    case payload: StandardJWTPayload =>
      users
        .find(_.userId == payload.userId)
        .flatMap(_.allowedServices.find(_ == serviceName))
        .fold[ClaimSet](ClaimSet.Unauthenticated)(_ => ClaimSet.Claims.Admin)
  }
}

class AuthServicePrivilegedJWT private[auth] (
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
    accessLevel: AccessLevel,
    val loggerFactory: NamedLoggerFactory,
    warnOnJwtScopeUsage: Boolean,
) extends AuthServiceJWTBase(
      verifier = verifier,
      targetAudience = targetAudience,
      targetScope = targetScope,
      AuthServiceJWTCodec.jsonImplicits(warnOnJwtScopeUsage),
    ) {

  private def claims = accessLevel match {
    case AccessLevel.Admin => ClaimSet.Claims.Admin.claims
    case AccessLevel.Wildcard => ClaimSet.Claims.Wildcard.claims
  }
  protected[this] def payloadToClaims(serviceName: String): AuthServiceJWTPayload => ClaimSet = {
    case payload: StandardJWTPayload =>
      ClaimSet.Claims(
        claims = claims,
        identityProviderId = None,
        participantId = payload.participantId,
        userId = Option.when(payload.userId.nonEmpty)(payload.userId),
        expiration = payload.exp,
        resolvedFromUser = false,
      )
  }
}

object AuthServiceJWT {
  def apply(
      verifier: JwtVerifierBase,
      targetAudience: Option[String],
      targetScope: Option[String],
      privileged: Boolean,
      accessLevel: AccessLevel,
      loggerFactory: NamedLoggerFactory,
      users: Seq[AuthorizedUser],
      warnOnJwtScopeUsage: Boolean,
  ): AuthServiceJWTBase = {
    val logger = loggerFactory.getTracedLogger(getClass)
    import com.digitalasset.canton.tracing.TraceContext
    implicit val traceContext: TraceContext = TraceContext.empty

    // TODO (i32650):  Add Canton version to the method signature. For versions 3.5 and below keep the logic as-is.
    //  Once audience-based tokens are enforced in version 3.7,
    //  - At startup: enforce the safe default or an explicitly configured value.
    //  - Do not allow scope-only configs.
    //  - Remove warning for when both audience and scope are defined as scope will be an additional optional field.

    if (targetScope.isDefined && targetAudience.isEmpty) {
      logger.warn(
        "Scope-based tokens have been configured (targetScope is set without a targetAudience). " +
          "This feature is deprecated in Canton 3.5 and will be removed in version 3.7. " +
          "Please migrate to audience-based tokens."
      )
    }
    if (targetScope.isDefined && targetAudience.isDefined) {
      logger.warn(
        "Ambiguous configuration: both targetScope and targetAudience have been specified. Canton versions 3.5 and below support " +
          "one of these, but not both. " + "Please configure exactly one."
      )
    }
    (privileged, targetScope, targetAudience, users) match {
      case (_, _, _, authorizedUsers) if authorizedUsers.nonEmpty =>
        new UserConfigAuthService(
          verifier,
          targetAudience,
          targetScope,
          authorizedUsers,
          loggerFactory,
          warnOnJwtScopeUsage,
        )
      case (true, Some(scope), _, _) =>
        new AuthServicePrivilegedJWT(
          verifier,
          None,
          Some(scope),
          accessLevel,
          loggerFactory,
          warnOnJwtScopeUsage,
        )
      case (true, None, Some(audience), _) =>
        new AuthServicePrivilegedJWT(
          verifier,
          Some(audience),
          None,
          accessLevel,
          loggerFactory,
          warnOnJwtScopeUsage,
        )
      case (true, None, None, _) =>
        throw new IllegalArgumentException(
          "Both targetScope and targetAudience are missing in the definition of a privileged JWT AuthService."
        )
      case (false, _, _, _) =>
        new AuthServiceJWT(
          verifier,
          targetAudience,
          targetScope,
          loggerFactory,
          warnOnJwtScopeUsage,
        )
    }
  }
}
