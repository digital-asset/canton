// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import java.time.Instant
import cats.syntax.either._
import cats.syntax.traverse._
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.admin.api.client.data._
import com.digitalasset.canton.crypto.{CertificateId, Fingerprint, KeyPurpose}
import com.digitalasset.canton.topology.{DomainId, _}
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.topology.admin.grpc.BaseQuery
import com.digitalasset.canton.topology.admin.v0
import com.digitalasset.canton.topology.admin.v0.TopologyAggregationServiceGrpc.TopologyAggregationServiceStub
import com.digitalasset.canton.topology.admin.v0.TopologyManagerReadServiceGrpc.TopologyManagerReadServiceStub
import com.digitalasset.canton.topology.admin.v0.TopologyManagerWriteServiceGrpc.TopologyManagerWriteServiceStub
import com.digitalasset.canton.topology.admin.v0.InitializationServiceGrpc.InitializationServiceStub
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.protocol.{DynamicDomainParameters, v0 => idProto}
import com.digitalasset.canton.topology.admin.v0.AuthorizationSuccess
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp
import io.grpc.ManagedChannel

import scala.concurrent.Future

object TopologyAdminCommands {

  object Aggregation {

    abstract class BaseCommand[Req, Res, Result] extends GrpcAdminCommand[Req, Res, Result] {
      override type Svc = TopologyAggregationServiceStub
      override def createService(channel: ManagedChannel): TopologyAggregationServiceStub =
        v0.TopologyAggregationServiceGrpc.stub(channel)
    }

    case class ListParties(
        filterDomain: String,
        filterParty: String,
        filterParticipant: String,
        asOf: Option[Instant],
        limit: Int,
    ) extends BaseCommand[v0.ListPartiesRequest, v0.ListPartiesResponse, Seq[ListPartiesResult]] {

      override def createRequest(): Either[String, v0.ListPartiesRequest] =
        Right(
          v0.ListPartiesRequest(
            filterDomain = filterDomain,
            filterParty = filterParty,
            filterParticipant = filterParticipant,
            asOf = asOf.map(ts => Timestamp(ts.getEpochSecond)),
            limit = limit,
          )
        )

      override def submitRequest(
          service: TopologyAggregationServiceStub,
          request: v0.ListPartiesRequest,
      ): Future[v0.ListPartiesResponse] =
        service.listParties(request)

      override def handleResponse(
          response: v0.ListPartiesResponse
      ): Either[String, Seq[ListPartiesResult]] =
        response.results.traverse(ListPartiesResult.fromProtoV0).leftMap(_.toString)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    case class ListKeyOwners(
        filterDomain: String,
        filterKeyOwnerType: Option[KeyOwnerCode],
        filterKeyOwnerUid: String,
        asOf: Option[Instant],
        limit: Int,
    ) extends BaseCommand[v0.ListKeyOwnersRequest, v0.ListKeyOwnersResponse, Seq[
          ListKeyOwnersResult
        ]] {

      override def createRequest(): Either[String, v0.ListKeyOwnersRequest] =
        Right(
          v0.ListKeyOwnersRequest(
            filterDomain = filterDomain,
            filterKeyOwnerType = filterKeyOwnerType.map(_.toProtoPrimitive).getOrElse(""),
            filterKeyOwnerUid = filterKeyOwnerUid,
            asOf = asOf.map(ts => Timestamp(ts.getEpochSecond)),
            limit = limit,
          )
        )

      override def submitRequest(
          service: TopologyAggregationServiceStub,
          request: v0.ListKeyOwnersRequest,
      ): Future[v0.ListKeyOwnersResponse] =
        service.listKeyOwners(request)

      override def handleResponse(
          response: v0.ListKeyOwnersResponse
      ): Either[String, Seq[ListKeyOwnersResult]] =
        response.results.traverse(ListKeyOwnersResult.fromProtoV0).leftMap(_.toString)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }
  }

  object Write {

    abstract class BaseWriteCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = TopologyManagerWriteServiceStub
      override def createService(channel: ManagedChannel): TopologyManagerWriteServiceStub =
        v0.TopologyManagerWriteServiceGrpc.stub(channel)

    }

    abstract class BaseCommand[Req]
        extends BaseWriteCommand[Req, v0.AuthorizationSuccess, ByteString] {

      protected def authData(
          ops: TopologyChangeOp,
          signedBy: Option[Fingerprint],
          replaceExisting: Boolean,
          force: Boolean,
      ) =
        Some(
          v0.AuthorizationData(
            ops.toProto,
            signedBy.map(_.unwrap).getOrElse(""),
            replaceExisting = replaceExisting,
            forceChange = force,
          )
        )

      override def handleResponse(response: v0.AuthorizationSuccess): Either[String, ByteString] =
        Right(response.serialized)

    }

    case class AuthorizeNamespaceDelegation(
        ops: TopologyChangeOp,
        signedBy: Option[Fingerprint],
        namespace: Fingerprint,
        authorizedKey: Fingerprint,
        isRootDelegation: Boolean,
    ) extends BaseCommand[v0.NamespaceDelegationAuthorization] {

      override def createRequest(): Either[String, v0.NamespaceDelegationAuthorization] =
        Right(
          v0.NamespaceDelegationAuthorization(
            authData(ops, signedBy, replaceExisting = false, force = false),
            namespace.toProtoPrimitive,
            authorizedKey.toProtoPrimitive,
            isRootDelegation,
          )
        )

      override def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: v0.NamespaceDelegationAuthorization,
      ): Future[v0.AuthorizationSuccess] =
        service.authorizeNamespaceDelegation(request)

    }

    case class AuthorizeIdentifierDelegation(
        ops: TopologyChangeOp,
        signedBy: Option[Fingerprint],
        identifier: UniqueIdentifier,
        authorizedKey: Fingerprint,
    ) extends BaseCommand[v0.IdentifierDelegationAuthorization] {

      override def createRequest(): Either[String, v0.IdentifierDelegationAuthorization] =
        Right(
          v0.IdentifierDelegationAuthorization(
            authData(ops, signedBy, replaceExisting = false, force = false),
            identifier.toProtoPrimitive,
            authorizedKey.toProtoPrimitive,
          )
        )

      override def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: v0.IdentifierDelegationAuthorization,
      ): Future[v0.AuthorizationSuccess] =
        service.authorizeIdentifierDelegation(request)

    }

    case class AuthorizeOwnerToKeyMapping(
        ops: TopologyChangeOp,
        signedBy: Option[Fingerprint],
        keyOwner: KeyOwner,
        fingerprintOfKey: Fingerprint,
        purpose: KeyPurpose,
        force: Boolean,
    ) extends BaseCommand[v0.OwnerToKeyMappingAuthorization] {

      override def createRequest(): Either[String, v0.OwnerToKeyMappingAuthorization] = Right(
        v0.OwnerToKeyMappingAuthorization(
          authData(ops, signedBy, replaceExisting = false, force = force),
          keyOwner.toProtoPrimitive,
          fingerprintOfKey.toProtoPrimitive,
          purpose.toProtoEnum,
        )
      )

      override def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: v0.OwnerToKeyMappingAuthorization,
      ): Future[v0.AuthorizationSuccess] =
        service.authorizeOwnerToKeyMapping(request)

    }

    case class AuthorizePartyToParticipant(
        ops: TopologyChangeOp,
        signedBy: Option[Fingerprint],
        side: RequestSide,
        party: PartyId,
        participant: ParticipantId,
        permission: ParticipantPermission,
        replaceExisting: Boolean,
    ) extends BaseCommand[v0.PartyToParticipantAuthorization] {

      override def createRequest(): Either[String, v0.PartyToParticipantAuthorization] =
        Right(
          v0.PartyToParticipantAuthorization(
            authData(ops, signedBy, replaceExisting = replaceExisting, force = false),
            side.toProtoEnum,
            party.uid.toProtoPrimitive,
            participant.toProtoPrimitive,
            permission.toProtoEnum,
          )
        )

      override def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: v0.PartyToParticipantAuthorization,
      ): Future[v0.AuthorizationSuccess] =
        service.authorizePartyToParticipant(request)

    }

    case class AuthorizeParticipantDomainState(
        ops: TopologyChangeOp,
        signedBy: Option[Fingerprint],
        side: RequestSide,
        domain: DomainId,
        participant: ParticipantId,
        permission: ParticipantPermission,
        trustLevel: TrustLevel,
        replaceExisting: Boolean,
    ) extends BaseCommand[v0.ParticipantDomainStateAuthorization] {

      override def createRequest(): Either[String, v0.ParticipantDomainStateAuthorization] =
        Right(
          v0.ParticipantDomainStateAuthorization(
            authData(ops, signedBy, replaceExisting = replaceExisting, force = false),
            side.toProtoEnum,
            domain.unwrap.toProtoPrimitive,
            participant.toProtoPrimitive,
            permission.toProtoEnum,
            trustLevel.toProtoEnum,
          )
        )

      override def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: v0.ParticipantDomainStateAuthorization,
      ): Future[v0.AuthorizationSuccess] =
        service.authorizeParticipantDomainState(request)

    }

    case class AuthorizeMediatorDomainState(
        ops: TopologyChangeOp,
        signedBy: Option[Fingerprint],
        side: RequestSide,
        domain: DomainId,
        mediator: MediatorId,
        replaceExisting: Boolean,
    ) extends BaseCommand[v0.MediatorDomainStateAuthorization] {

      override def createRequest(): Either[String, v0.MediatorDomainStateAuthorization] =
        Right(
          v0.MediatorDomainStateAuthorization(
            authData(ops, signedBy, replaceExisting = replaceExisting, force = false),
            side.toProtoEnum,
            domain.unwrap.toProtoPrimitive,
            mediator.uid.toProtoPrimitive,
          )
        )

      override def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: v0.MediatorDomainStateAuthorization,
      ): Future[v0.AuthorizationSuccess] =
        service.authorizeMediatorDomainState(request)

    }

    case class AuthorizeSignedLegalIdentityClaim(
        ops: TopologyChangeOp,
        signedBy: Option[Fingerprint],
        claim: SignedLegalIdentityClaim,
    ) extends BaseCommand[v0.SignedLegalIdentityClaimAuthorization] {

      override def createRequest(): Either[String, v0.SignedLegalIdentityClaimAuthorization] =
        Right(
          v0.SignedLegalIdentityClaimAuthorization(
            authData(ops, signedBy, replaceExisting = false, force = false),
            claim = Some(claim.toProtoV0),
          )
        )

      override def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: v0.SignedLegalIdentityClaimAuthorization,
      ): Future[v0.AuthorizationSuccess] =
        service.authorizeSignedLegalIdentityClaim(request)

    }

    case class AuthorizeVettedPackages(
        ops: TopologyChangeOp,
        signedBy: Option[Fingerprint],
        participant: ParticipantId,
        packageIds: Seq[PackageId],
        force: Boolean,
    ) extends BaseCommand[v0.VettedPackagesAuthorization] {

      override def createRequest(): Either[String, v0.VettedPackagesAuthorization] =
        Right(
          v0.VettedPackagesAuthorization(
            authData(ops, signedBy, replaceExisting = false, force = force),
            participant.uid.toProtoPrimitive,
            packageIds = packageIds,
          )
        )

      override def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: v0.VettedPackagesAuthorization,
      ): Future[v0.AuthorizationSuccess] =
        service.authorizeVettedPackages(request)

    }

    case class AuthorizeDomainParametersChange(
        signedBy: Option[Fingerprint],
        domainId: DomainId,
        newParameters: DynamicDomainParameters,
    ) extends BaseCommand[v0.DomainParametersChangeAuthorization] {
      override def createRequest(): Either[String, v0.DomainParametersChangeAuthorization] =
        v0.DomainParametersChangeAuthorization(
          authorization =
            authData(TopologyChangeOp.Replace, signedBy, replaceExisting = false, force = false),
          domain = domainId.toProtoPrimitive,
          parameters = Option(newParameters.toProtoV0),
        ).asRight

      override def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: v0.DomainParametersChangeAuthorization,
      ): Future[AuthorizationSuccess] = service.authorizeDomainParametersChange(request)
    }

    case class AddSignedTopologyTransaction(bytes: ByteString)
        extends BaseWriteCommand[v0.SignedTopologyTransactionAddition, v0.AdditionSuccess, Unit] {

      override def createRequest(): Either[String, v0.SignedTopologyTransactionAddition] =
        Right(v0.SignedTopologyTransactionAddition(serialized = bytes))

      override def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: v0.SignedTopologyTransactionAddition,
      ): Future[v0.AdditionSuccess] =
        service.addSignedTopologyTransaction(request)

      override def handleResponse(response: v0.AdditionSuccess): Either[String, Unit] =
        Right(())
    }

    sealed trait BaseClaimCommand
        extends BaseWriteCommand[
          v0.SignedLegalIdentityClaimGeneration,
          idProto.SignedLegalIdentityClaim,
          SignedLegalIdentityClaim,
        ] {

      override def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: v0.SignedLegalIdentityClaimGeneration,
      ): Future[idProto.SignedLegalIdentityClaim] =
        service.generateSignedLegalIdentityClaim(request)
      override def handleResponse(
          response: idProto.SignedLegalIdentityClaim
      ): Either[String, SignedLegalIdentityClaim] =
        SignedLegalIdentityClaim.fromProtoV0(response).leftMap(_.toString)

    }

    case class GenerateSignedLegalIdentityClaim(claim: LegalIdentityClaim)
        extends BaseClaimCommand {

      override def createRequest(): Either[String, v0.SignedLegalIdentityClaimGeneration] =
        Right(
          v0.SignedLegalIdentityClaimGeneration(request =
            v0.SignedLegalIdentityClaimGeneration.Request
              .LegalIdentityClaim(value = claim.getCryptographicEvidence)
          )
        )

    }

    case class GenerateX509IdentityClaim(uid: UniqueIdentifier, certificateId: CertificateId)
        extends BaseClaimCommand {

      override def createRequest(): Either[String, v0.SignedLegalIdentityClaimGeneration] =
        Right(
          v0.SignedLegalIdentityClaimGeneration(
            request = v0.SignedLegalIdentityClaimGeneration.Request.Certificate(
              v0.SignedLegalIdentityClaimGeneration
                .X509CertificateClaim(uid.toProtoPrimitive, certificateId.unwrap)
            )
          )
        )
    }

  }

  object Read {

    abstract class BaseCommand[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = TopologyManagerReadServiceStub
      override def createService(channel: ManagedChannel): TopologyManagerReadServiceStub =
        v0.TopologyManagerReadServiceGrpc.stub(channel)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    case class ListPartyToParticipant(
        query: BaseQuery,
        filterParty: String,
        filterParticipant: String,
        filterRequestSide: Option[RequestSide],
        filterPermission: Option[ParticipantPermission],
    ) extends BaseCommand[v0.ListPartyToParticipantRequest, v0.ListPartyToParticipantResult, Seq[
          ListPartyToParticipantResult
        ]] {

      override def createRequest(): Either[String, v0.ListPartyToParticipantRequest] =
        Right(
          new v0.ListPartyToParticipantRequest(
            baseQuery = Some(query.toProtoV0),
            filterParty,
            filterParticipant,
            filterRequestSide
              .map(_.toProtoEnum)
              .map(new v0.ListPartyToParticipantRequest.FilterRequestSide(_)),
            filterPermission
              .map(_.toProtoEnum)
              .map(new v0.ListPartyToParticipantRequest.FilterPermission(_)),
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v0.ListPartyToParticipantRequest,
      ): Future[v0.ListPartyToParticipantResult] =
        service.listPartyToParticipant(request)

      override def handleResponse(
          response: v0.ListPartyToParticipantResult
      ): Either[String, Seq[ListPartyToParticipantResult]] =
        response.results.traverse(ListPartyToParticipantResult.fromProtoV0).leftMap(_.toString)

    }

    case class ListOwnerToKeyMapping(
        query: BaseQuery,
        filterKeyOwnerType: Option[KeyOwnerCode],
        filterKeyOwnerUid: String,
        filterKeyPurpose: Option[KeyPurpose],
    ) extends BaseCommand[v0.ListOwnerToKeyMappingRequest, v0.ListOwnerToKeyMappingResult, Seq[
          ListOwnerToKeyMappingResult
        ]] {

      override def createRequest(): Either[String, v0.ListOwnerToKeyMappingRequest] =
        Right(
          new v0.ListOwnerToKeyMappingRequest(
            baseQuery = Some(query.toProtoV0),
            filterKeyOwnerType = filterKeyOwnerType.map(_.toProtoPrimitive).getOrElse(""),
            filterKeyOwnerUid = filterKeyOwnerUid,
            filterKeyPurpose
              .map(_.toProtoEnum)
              .map(new admin.v0.ListOwnerToKeyMappingRequest.FilterKeyPurpose(_)),
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v0.ListOwnerToKeyMappingRequest,
      ): Future[v0.ListOwnerToKeyMappingResult] =
        service.listOwnerToKeyMapping(request)

      override def handleResponse(
          response: v0.ListOwnerToKeyMappingResult
      ): Either[String, Seq[ListOwnerToKeyMappingResult]] =
        response.results.traverse(ListOwnerToKeyMappingResult.fromProtoV0).leftMap(_.toString)
    }

    case class ListNamespaceDelegation(query: BaseQuery, filterNamespace: String)
        extends BaseCommand[
          v0.ListNamespaceDelegationRequest,
          v0.ListNamespaceDelegationResult,
          Seq[ListNamespaceDelegationResult],
        ] {

      override def createRequest(): Either[String, v0.ListNamespaceDelegationRequest] =
        Right(
          new v0.ListNamespaceDelegationRequest(
            baseQuery = Some(query.toProtoV0),
            filterNamespace = filterNamespace,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v0.ListNamespaceDelegationRequest,
      ): Future[v0.ListNamespaceDelegationResult] =
        service.listNamespaceDelegation(request)

      override def handleResponse(
          response: v0.ListNamespaceDelegationResult
      ): Either[String, Seq[ListNamespaceDelegationResult]] =
        response.results.traverse(ListNamespaceDelegationResult.fromProtoV0).leftMap(_.toString)
    }

    case class ListIdentifierDelegation(query: BaseQuery, filterUid: String)
        extends BaseCommand[
          v0.ListIdentifierDelegationRequest,
          v0.ListIdentifierDelegationResult,
          Seq[ListIdentifierDelegationResult],
        ] {

      override def createRequest(): Either[String, v0.ListIdentifierDelegationRequest] =
        Right(
          new v0.ListIdentifierDelegationRequest(
            baseQuery = Some(query.toProtoV0),
            filterUid = filterUid,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v0.ListIdentifierDelegationRequest,
      ): Future[v0.ListIdentifierDelegationResult] =
        service.listIdentifierDelegation(request)

      override def handleResponse(
          response: v0.ListIdentifierDelegationResult
      ): Either[String, Seq[ListIdentifierDelegationResult]] =
        response.results.traverse(ListIdentifierDelegationResult.fromProtoV0).leftMap(_.toString)
    }

    case class ListSignedLegalIdentityClaim(query: BaseQuery, filterUid: String)
        extends BaseCommand[
          v0.ListSignedLegalIdentityClaimRequest,
          v0.ListSignedLegalIdentityClaimResult,
          Seq[ListSignedLegalIdentityClaimResult],
        ] {

      override def createRequest(): Either[String, v0.ListSignedLegalIdentityClaimRequest] =
        Right(
          new v0.ListSignedLegalIdentityClaimRequest(
            baseQuery = Some(query.toProtoV0),
            filterUid = filterUid,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v0.ListSignedLegalIdentityClaimRequest,
      ): Future[v0.ListSignedLegalIdentityClaimResult] =
        service.listSignedLegalIdentityClaim(request)

      override def handleResponse(
          response: v0.ListSignedLegalIdentityClaimResult
      ): Either[String, Seq[ListSignedLegalIdentityClaimResult]] =
        response.results
          .traverse(ListSignedLegalIdentityClaimResult.fromProtoV0)
          .leftMap(_.toString)
    }

    case class ListVettedPackages(query: BaseQuery, filterParticipant: String)
        extends BaseCommand[v0.ListVettedPackagesRequest, v0.ListVettedPackagesResult, Seq[
          ListVettedPackagesResult
        ]] {

      override def createRequest(): Either[String, v0.ListVettedPackagesRequest] =
        Right(
          new v0.ListVettedPackagesRequest(
            baseQuery = Some(query.toProtoV0),
            filterParticipant,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v0.ListVettedPackagesRequest,
      ): Future[v0.ListVettedPackagesResult] =
        service.listVettedPackages(request)

      override def handleResponse(
          response: v0.ListVettedPackagesResult
      ): Either[String, Seq[ListVettedPackagesResult]] =
        response.results.traverse(ListVettedPackagesResult.fromProtoV0).leftMap(_.toString)
    }

    case class ListDomainParametersChanges(query: BaseQuery)
        extends BaseCommand[
          v0.ListDomainParametersChangesRequest,
          v0.ListDomainParametersChangesResult,
          Seq[ListDomainParametersChangeResult],
        ] {
      override def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v0.ListDomainParametersChangesRequest,
      ): Future[v0.ListDomainParametersChangesResult] = service.listDomainParametersChanges(request)

      override def createRequest(): Either[String, v0.ListDomainParametersChangesRequest] = Right(
        v0.ListDomainParametersChangesRequest(Some(query.toProtoV0))
      )

      override def handleResponse(
          response: v0.ListDomainParametersChangesResult
      ): Either[String, Seq[ListDomainParametersChangeResult]] =
        response.results.traverse(ListDomainParametersChangeResult.fromProtoV0).leftMap(_.toString)
    }

    case class ListStores()
        extends BaseCommand[v0.ListAvailableStoresRequest, v0.ListAvailableStoresResult, Seq[
          String
        ]] {

      override def createRequest(): Either[String, v0.ListAvailableStoresRequest] =
        Right(v0.ListAvailableStoresRequest())

      override def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v0.ListAvailableStoresRequest,
      ): Future[v0.ListAvailableStoresResult] =
        service.listAvailableStores(request)

      override def handleResponse(
          response: v0.ListAvailableStoresResult
      ): Either[String, Seq[String]] =
        Right(response.storeIds)
    }

    case class ListParticipantDomainState(
        query: BaseQuery,
        filterDomain: String,
        filterParticipant: String,
    ) extends BaseCommand[
          v0.ListParticipantDomainStateRequest,
          v0.ListParticipantDomainStateResult,
          Seq[ListParticipantDomainStateResult],
        ] {

      override def createRequest(): Either[String, v0.ListParticipantDomainStateRequest] =
        Right(
          new v0.ListParticipantDomainStateRequest(
            baseQuery = Some(query.toProtoV0),
            filterDomain = filterDomain,
            filterParticipant = filterParticipant,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v0.ListParticipantDomainStateRequest,
      ): Future[v0.ListParticipantDomainStateResult] =
        service.listParticipantDomainState(request)

      override def handleResponse(
          response: v0.ListParticipantDomainStateResult
      ): Either[String, Seq[ListParticipantDomainStateResult]] =
        response.results.traverse(ListParticipantDomainStateResult.fromProtoV0).leftMap(_.toString)
    }

    case class ListMediatorDomainState(
        query: BaseQuery,
        filterDomain: String,
        filterMediator: String,
    ) extends BaseCommand[
          v0.ListMediatorDomainStateRequest,
          v0.ListMediatorDomainStateResult,
          Seq[ListMediatorDomainStateResult],
        ] {

      override def createRequest(): Either[String, v0.ListMediatorDomainStateRequest] =
        Right(
          new v0.ListMediatorDomainStateRequest(
            baseQuery = Some(query.toProtoV0),
            filterDomain = filterDomain,
            filterMediator = filterMediator,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v0.ListMediatorDomainStateRequest,
      ): Future[v0.ListMediatorDomainStateResult] =
        service.listMediatorDomainState(request)

      override def handleResponse(
          response: v0.ListMediatorDomainStateResult
      ): Either[String, Seq[ListMediatorDomainStateResult]] =
        response.results.traverse(ListMediatorDomainStateResult.fromProtoV0).leftMap(_.toString)
    }

    case class ListAll(query: BaseQuery)
        extends BaseCommand[
          v0.ListAllRequest,
          v0.ListAllResponse,
          StoredTopologyTransactions[
            TopologyChangeOp
          ],
        ] {
      override def createRequest(): Either[String, v0.ListAllRequest] =
        Right(new v0.ListAllRequest(Some(query.toProtoV0)))

      override def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v0.ListAllRequest,
      ): Future[v0.ListAllResponse] = service.listAll(request)

      override def handleResponse(
          response: v0.ListAllResponse
      ): Either[String, StoredTopologyTransactions[TopologyChangeOp]] =
        response.result
          .fold[Either[String, StoredTopologyTransactions[TopologyChangeOp]]](
            Right(StoredTopologyTransactions.empty)
          ) { collection =>
            StoredTopologyTransactions.fromProtoV0(collection).leftMap(_.toString)
          }
    }
  }

  object Init {

    abstract class BaseInitializationService[Req, Resp, Res]
        extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = InitializationServiceStub
      override def createService(channel: ManagedChannel): InitializationServiceStub =
        v0.InitializationServiceGrpc.stub(channel)

    }

    final case class InitId(identifier: String, fingerprint: String)
        extends BaseInitializationService[v0.InitIdRequest, v0.InitIdResponse, UniqueIdentifier] {

      override def createRequest(): Either[String, v0.InitIdRequest] =
        Right(v0.InitIdRequest(identifier, fingerprint, instance = ""))

      override def submitRequest(
          service: InitializationServiceStub,
          request: v0.InitIdRequest,
      ): Future[v0.InitIdResponse] =
        service.initId(request)

      override def handleResponse(response: v0.InitIdResponse): Either[String, UniqueIdentifier] =
        UniqueIdentifier.fromProtoPrimitive_(response.uniqueIdentifier)
    }

    final case class GetId()
        extends BaseInitializationService[Empty, v0.GetIdResponse, Option[UniqueIdentifier]] {
      override def createRequest(): Either[String, Empty] =
        Right(Empty())

      override def submitRequest(
          service: InitializationServiceStub,
          request: Empty,
      ): Future[v0.GetIdResponse] =
        service.getId(request)

      override def handleResponse(
          response: v0.GetIdResponse
      ): Either[String, Option[UniqueIdentifier]] = {
        if (response.uniqueIdentifier.nonEmpty)
          UniqueIdentifier.fromProtoPrimitive_(response.uniqueIdentifier).map(Some(_))
        else
          Left(
            s"Node ${response.instance} is not initialized and therefore does not have an Id assigned yet."
          )
      }
    }
  }
}
