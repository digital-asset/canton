// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  Error,
  JwtFromBearerHeader,
  JwtVerifierBase,
  PartyJWTPayload,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import com.digitalasset.canton.config.{Confidential, DeprecatedFeaturesConfig}
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
    userId: Confidential[String],
    allowedServices: Seq[String],
)

/** An AuthService that reads a JWT token from a `Authorization: Bearer` HTTP header. The token is
  * expected to use the format as defined in [[com.daml.jwt.AuthServiceJWTPayload]].
  *
  * @param enableScopeOnlyJwt
  *   whether scope-only tokens (a scope but no audience) are accepted, see
  *   [[com.digitalasset.canton.config.DeprecatedFeaturesConfig.enableScopeOnlyJwt]]
  */
abstract class AuthServiceJWTBase(
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
    enableScopeOnlyJwt: Boolean,
) extends AuthService
    with NamedLogging {

  override def decodeToken(
      authToken: Option[String],
      serviceName: String,
  )(implicit traceContext: TraceContext): Future[AuthService.Result] =
    Future.successful {
      authToken match {
        case None => AuthService.Result(ClaimSet.Unauthenticated)
        case Some(header) => parseHeader(header, serviceName)
      }
    }

  private[this] def parseHeader(header: String, serviceName: String): AuthService.Result =
    parseJWTPayload(header).fold(
      error =>
        AuthService.Result(ClaimSet.Unauthenticated, Some("Authorization error: " + error.message)),
      payload => AuthService.Result(payloadToClaims(serviceName)(payload)),
    )

  private[this] def parsePayload(jwtPayload: String): Either[Error, AuthServiceJWTPayload] = {
    val parsed = targetAudience match {
      case Some(_) => Try(parseAudienceBasedPayload(jwtPayload))
      case None if targetScope.isDefined => Try(parseScopeBasedPayload(jwtPayload))
      case _ => Try(parseAuthServicePayload(jwtPayload))
    }

    parsed.toEither.left
      .map(t => Error(Symbol("parsePayload"), "Could not parse JWT token: " + t.getMessage))
      // precedes the target check so that a disabled scope-only token is reported as such; with a
      // target audience the parser never yields the scope-only format, and a target scope with the
      // flag off is refused by the config validation, so in practice this is the no-target case
      // TODO (i#33090): remove together with the scope-only token format
      .flatMap(AuthServiceJWT.rejectScopeOnlyJwtUnlessEnabled(_, enableScopeOnlyJwt))
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
    import AuthServiceJWTCodec.JsonImplicits.*
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
    enableScopeOnlyJwt: Boolean,
) extends AuthServiceJWTBase(
      verifier,
      targetAudience,
      targetScope,
      enableScopeOnlyJwt,
    ) {
  protected[this] def payloadToClaims(serviceName: String): AuthServiceJWTPayload => ClaimSet = {
    case payload: StandardJWTPayload =>
      ClaimSet.AuthenticatedUser(
        identityProviderId = None,
        participantId = payload.participantId,
        userId = payload.userId,
        expiration = payload.exp,
        restriction = payload.scope.fold(Seq[Claim]())(Claim.parseJwtScope(_)),
      )
    case payload: PartyJWTPayload => ClaimSet.Unauthenticated
  }
}

class UserConfigAuthService private[auth] (
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
    users: Seq[AuthorizedUser],
    val loggerFactory: NamedLoggerFactory,
    enableScopeOnlyJwt: Boolean,
) extends AuthServiceJWTBase(
      verifier,
      targetAudience,
      targetScope,
      enableScopeOnlyJwt,
    ) {
  protected[this] def payloadToClaims(serviceName: String): AuthServiceJWTPayload => ClaimSet = {
    case payload: PartyJWTPayload => ClaimSet.Unauthenticated
    case payload: StandardJWTPayload => {
      users
        .find(_.userId.unwrap == payload.userId)
        .flatMap(_.allowedServices.find(_ == serviceName))
        .fold[ClaimSet](ClaimSet.Unauthenticated)(_ => ClaimSet.Claims.Admin)
    }
  }
}

class AuthServicePrivilegedJWT private[auth] (
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
    accessLevel: AccessLevel,
    val loggerFactory: NamedLoggerFactory,
    enableScopeOnlyJwt: Boolean,
) extends AuthServiceJWTBase(
      verifier = verifier,
      targetAudience = targetAudience,
      targetScope = targetScope,
      enableScopeOnlyJwt = enableScopeOnlyJwt,
    ) {

  private def claims = accessLevel match {
    case AccessLevel.Admin => ClaimSet.Claims.Admin.claims
    case AccessLevel.Wildcard => ClaimSet.Claims.Wildcard.claims
  }
  protected[this] def payloadToClaims(serviceName: String): AuthServiceJWTPayload => ClaimSet = {
    case payload: PartyJWTPayload => ClaimSet.Unauthenticated
    case payload: StandardJWTPayload =>
      ClaimSet.Claims(
        claims = claims,
        identityProviderId = None,
        participantId = payload.participantId,
        userId = Option.when(payload.userId.nonEmpty)(payload.userId),
        expiration = payload.exp,
        resolvedFromUser = false,
        appliedRestriction = List.empty,
      )
  }
}

object AuthServiceJWT {

  /** Rejects a scope-only token unless `enableScopeOnlyJwt` is set and passes every other payload
    * through.
    */
  // TODO (i#33090): remove together with the scope-only token format
  def rejectScopeOnlyJwtUnlessEnabled(
      payload: AuthServiceJWTPayload,
      enableScopeOnlyJwt: Boolean,
  ): Either[Error, AuthServiceJWTPayload] =
    payload match {
      case payload: StandardJWTPayload
          if payload.format == StandardJWTTokenFormat.Scope && !enableScopeOnlyJwt =>
        Left(
          Error(
            Symbol("scopeOnlyJwtDisabled"),
            DeprecatedFeaturesConfig.ScopeOnlyJwtDisabledMessage,
          )
        )
      case _ => Right(payload)
    }

  def apply(
      verifier: JwtVerifierBase,
      targetAudience: Option[String],
      targetScope: Option[String],
      privileged: Boolean,
      accessLevel: AccessLevel,
      loggerFactory: NamedLoggerFactory,
      users: Seq[AuthorizedUser],
      enableScopeOnlyJwt: Boolean,
  ): AuthServiceJWTBase = {
    val logger = loggerFactory.getTracedLogger(getClass)
    implicit val traceContext: TraceContext = TraceContext.empty

    val authService = (privileged, targetScope, targetAudience, users) match {
      case (_, _, _, authorizedUsers) if authorizedUsers.nonEmpty =>
        new UserConfigAuthService(
          verifier,
          targetAudience,
          targetScope,
          authorizedUsers,
          loggerFactory,
          enableScopeOnlyJwt,
        )
      case (true, Some(scope), _, _) =>
        new AuthServicePrivilegedJWT(
          verifier,
          None,
          Some(scope),
          accessLevel,
          loggerFactory,
          enableScopeOnlyJwt,
        )
      case (true, None, Some(audience), _) =>
        new AuthServicePrivilegedJWT(
          verifier,
          Some(audience),
          None,
          accessLevel,
          loggerFactory,
          enableScopeOnlyJwt,
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
          enableScopeOnlyJwt,
        )
    }

    // TODO (i#33090): Once the scope-only token format is removed, reject a target scope without a
    //  target audience and drop these warnings; a target scope then only restricts audience-based
    //  tokens further, so configuring both is no longer ambiguous.
    (targetScope, targetAudience) match {
      case (Some(_), Some(_)) =>
        logger.warn(
          "Ambiguous configuration: both targetScope and targetAudience have been specified. " +
            "Canton versions 3.5 and below support one of these, but not both. Please configure exactly one."
        )
      // The config validation rejects a target scope without a target audience unless the flag is set
      case (_, None) if enableScopeOnlyJwt =>
        logger.warn(
          s"Scope-only JWTs are enabled (${DeprecatedFeaturesConfig.EnableScopeOnlyJwtKey} = true) and " +
            "no targetAudience is configured, so the deprecated scope-only tokens are accepted. " +
            "This feature will be removed in a future version. Please migrate to audience-based tokens."
        )
      case _ => ()
    }

    authService
  }
}
