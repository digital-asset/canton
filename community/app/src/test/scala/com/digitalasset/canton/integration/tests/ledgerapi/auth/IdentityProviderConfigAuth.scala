// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import cats.syntax.functor.*
import com.daml.ledger.api.v2.admin.identity_provider_config_service.{
  CreateIdentityProviderConfigRequest,
  DeleteIdentityProviderConfigRequest,
  GetIdentityProviderConfigRequest,
  IdentityProviderConfig,
  IdentityProviderConfigServiceGrpc,
}
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocateExternalPartyRequest,
  AllocatePartyResponse,
  GenerateExternalPartyTopologyRequest,
  GenerateExternalPartyTopologyResponse,
  PartyManagementServiceGrpc,
  UpdatePartyIdentityProviderIdRequest,
}
import com.daml.ledger.api.v2.admin.user_management_service.{
  UpdateUserIdentityProviderIdRequest,
  User,
  UserManagementServiceGrpc,
}
import com.daml.ledger.api.v2.admin.{
  party_management_service as pproto,
  user_management_service as uproto,
}
import com.daml.ledger.api.v2.crypto as lapicrypto
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseJWKSServer
import com.google.protobuf.ByteString

import java.security.{KeyPairGenerator, Signature}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait IdentityProviderConfigAuth extends KeyPairs {
  this: ServiceCallAuthTests =>

  val jwksServer = new UseJWKSServer(jwks, altJwks)
  registerPlugin(jwksServer)
  lazy val jwksUrl: String = jwksServer.endpoint
  lazy val altJwksUrl: String = jwksServer.altEndpoint

  def idpStub(
      context: ServiceCallContext
  ): IdentityProviderConfigServiceGrpc.IdentityProviderConfigServiceStub =
    stub(IdentityProviderConfigServiceGrpc.stub(channel), context.token)

  def createConfig(
      context: ServiceCallContext,
      idpId: Option[String] = None,
      audience: Option[String] = None,
      jwksUrlOverride: Option[String] = None,
      issuer: Option[String] = None,
  )(implicit ec: ExecutionContext): Future[IdentityProviderConfig] = {
    val suffix = UUID.randomUUID().toString
    val config =
      IdentityProviderConfig(
        identityProviderId = idpId.getOrElse("idp-id-" + suffix),
        isDeactivated = false,
        issuer = issuer.getOrElse("issuer-" + suffix),
        // token must be signed with one of the private keys corresponding to the jwks provided by this url
        jwksUrl = jwksUrlOverride.getOrElse(jwksUrl),
        audience = audience.getOrElse(""),
      )
    createConfig(context, config)
  }

  private def createConfig(
      context: ServiceCallContext,
      config: IdentityProviderConfig,
  )(implicit ec: ExecutionContext): Future[IdentityProviderConfig] =
    idpStub(context)
      .createIdentityProviderConfig(CreateIdentityProviderConfigRequest(Some(config)))
      .map(_.identityProviderConfig)
      .flatMap {
        case Some(idp) => Future.successful(idp)
        case None => Future.failed(new RuntimeException("Failed to create IDP Config"))
      }

  def deleteConfig(
      context: ServiceCallContext,
      identityProviderId: String,
  )(implicit ec: ExecutionContext): Future[Unit] =
    idpStub(context)
      .deleteIdentityProviderConfig(
        DeleteIdentityProviderConfigRequest(identityProviderId = identityProviderId)
      )
      .void

  def getConfig(
      context: ServiceCallContext,
      identityProviderId: String,
  )(implicit ec: ExecutionContext): Future[IdentityProviderConfig] =
    idpStub(context)
      .getIdentityProviderConfig(GetIdentityProviderConfigRequest(identityProviderId))
      .map(_.identityProviderConfig)
      .flatMap {
        case Some(idp) => Future.successful(idp)
        case None => Future.failed(new RuntimeException("Failed to get IDP Config"))
      }

  def parkParties(
      context: ServiceCallContext,
      parties: Seq[String],
      identityProviderId: String,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val pmsvc = stub(PartyManagementServiceGrpc.stub(channel), context.token)
    Future
      .sequence(
        parties.map(party =>
          pmsvc.updatePartyIdentityProviderId(
            UpdatePartyIdentityProviderIdRequest(
              party = party,
              sourceIdentityProviderId = identityProviderId,
              targetIdentityProviderId = "",
            )
          )
        )
      )
      .void
  }

  def parkUsers(
      context: ServiceCallContext,
      users: Seq[String],
      identityProviderId: String,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val umsvc = stub(UserManagementServiceGrpc.stub(channel), context.token)
    Future
      .sequence(
        users.map(user =>
          umsvc.updateUserIdentityProviderId(
            UpdateUserIdentityProviderIdRequest(
              userId = user,
              sourceIdentityProviderId = identityProviderId,
              targetIdentityProviderId = "",
            )
          )
        )
      )
      .void
  }

  protected def createIDPBundle(context: ServiceCallContext, suffix: String)(implicit
      ec: ExecutionContext
  ): Future[(User, ServiceCallContext, IdentityProviderConfig)] =
    for {
      idpConfig <- createConfig(context)
      (user, idpAdminContext) <- createUserByAdminRSA(
        userId = "idp-admin-" + suffix,
        identityProviderId = idpConfig.identityProviderId,
        tokenIssuer = Some(idpConfig.issuer),
        rights = idpAdminRights,
        privateKey = key1.privateKey,
        keyId = key1.id,
      )
    } yield (user, idpAdminContext, idpConfig)

  protected def createUser(
      userId: String,
      serviceCallContext: ServiceCallContext,
      rights: Vector[uproto.Right] = Vector.empty,
  ): Future[uproto.CreateUserResponse] = {
    val user = uproto.User(
      id = userId,
      primaryParty = "",
      isDeactivated = false,
      metadata = Some(ObjectMeta.defaultInstance),
      identityProviderId = serviceCallContext.identityProviderId,
    )
    val req = uproto.CreateUserRequest(Some(user), rights)
    stub(uproto.UserManagementServiceGrpc.stub(channel), serviceCallContext.token)
      .createUser(req)
  }

  protected def allocateParty(
      serviceCallContext: ServiceCallContext,
      party: String,
      userId: String = "",
      identityProviderIdOverride: Option[String] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): Future[String] =
    allocatePartyWithDetails(serviceCallContext, party, userId, identityProviderIdOverride)
      .map(_.partyDetails.value.party)(env.executionContext)

  protected def allocatePartyWithDetails(
      serviceCallContext: ServiceCallContext,
      party: String,
      userId: String = "",
      identityProviderIdOverride: Option[String] = None,
  ): Future[AllocatePartyResponse] =
    stub(pproto.PartyManagementServiceGrpc.stub(channel), serviceCallContext.token)
      .allocateParty(
        pproto.AllocatePartyRequest(
          partyIdHint = party,
          localMetadata = None,
          identityProviderId =
            identityProviderIdOverride.getOrElse(serviceCallContext.identityProviderId),
          synchronizerId = "",
          userId = userId,
        )
      )

  protected def allocateExternalParty(
      serviceCallContext: ServiceCallContext,
      partyHint: String,
      userId: String = "",
      identityProviderIdOverride: Option[String] = None,
  )(implicit env: TestConsoleEnvironment): Future[String] = {
    val keyGen = KeyPairGenerator.getInstance("Ed25519")
    val keyPair = keyGen.generateKeyPair()
    val pb = keyPair.getPublic
    val ledger = stub(pproto.PartyManagementServiceGrpc.stub(channel), serviceCallContext.token)
    val identityProviderId =
      identityProviderIdOverride.getOrElse(serviceCallContext.identityProviderId)
    val synchronizerId = env.synchronizer1Id.logical.toProtoPrimitive
    implicit val ec: ExecutionContext = env.executionContext

    def signTopology(response: GenerateExternalPartyTopologyResponse): ByteString = {
      val signing = Signature.getInstance("Ed25519")
      signing.initSign(keyPair.getPrivate)
      signing.update(response.multiHash.toByteArray)
      ByteString.copyFrom(signing.sign())
    }

    for {
      generated <- ledger.generateExternalPartyTopology(
        GenerateExternalPartyTopologyRequest(
          synchronizer = synchronizerId,
          partyHint = partyHint,
          publicKey = Some(
            lapicrypto.SigningPublicKey(
              format =
                lapicrypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO,
              keyData = ByteString.copyFrom(pb.getEncoded),
              keySpec = lapicrypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519,
            )
          ),
          localParticipantObservationOnly = false,
          otherConfirmingParticipantUids = Seq(),
          confirmationThreshold = 1,
          observingParticipantUids = Seq(),
        )
      )
      signature = signTopology(generated)
      resp <- ledger
        .allocateExternalParty(
          AllocateExternalPartyRequest(
            synchronizer = synchronizerId,
            onboardingTransactions = generated.topologyTransactions.map(x =>
              AllocateExternalPartyRequest
                .SignedTransaction(transaction = x, signatures = Seq.empty)
            ),
            multiHashSignatures = Seq(
              lapicrypto.Signature(
                format = lapicrypto.SignatureFormat.SIGNATURE_FORMAT_RAW,
                signature = signature,
                signedBy = generated.publicKeyFingerprint,
                signingAlgorithmSpec =
                  lapicrypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519,
              )
            ),
            waitForAllocation = Some(true),
            identityProviderId = identityProviderId,
            userId = userId,
          )
        )
        .map(_.partyId)
    } yield resp
  }

  protected def idpAdminRights: Vector[uproto.Right] = Vector(
    uproto.Right(
      uproto.Right.Kind.IdentityProviderAdmin(uproto.Right.IdentityProviderAdmin())
    )
  )

}
