// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.implicits.{catsSyntaxParallelTraverse1, toBifunctorOps, toTraverseOps}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.daml.lf.crypto.Hash as LfHash
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, NodeId, VersionedTransaction}
import com.digitalasset.daml.lf.value.Value.ContractId

import java.util.UUID
import scala.collection.immutable.{SortedMap, SortedSet}
import scala.concurrent.ExecutionContext

object InteractiveSubmission {
  implicit val contractIdOrdering: Ordering[LfContractId] = Ordering.by(_.coid)

  sealed trait HashError {
    def message: String
  }
  final case class HashingFailed(message: String) extends HashError
  final case class UnsupportedHashingSchemeVersion(
      version: HashingSchemeVersion,
      currentProtocolVersion: ProtocolVersion,
      minProtocolVersion: Option[ProtocolVersion],
      supportedSchemesOnCurrentPV: Set[HashingSchemeVersion],
  ) extends HashError {
    override def message: String =
      s"Hashing scheme version $version is not supported on protocol version $currentProtocolVersion." +
        s" Minimum protocol version for hashing version $version: ${minProtocolVersion.map(_.toString).getOrElse("Unsupported")}." +
        s" Supported hashing version on protocol version $currentProtocolVersion: ${supportedSchemesOnCurrentPV
            .mkString(", ")}"
  }

  object TransactionMetadataForHashing {
    def apply(
        actAs: SortedSet[Ref.Party],
        commandId: Ref.CommandId,
        transactionUUID: UUID,
        mediatorGroup: Int,
        domainId: DomainId,
        ledgerEffectiveTime: Option[Time.Timestamp],
        submissionTime: Time.Timestamp,
        disclosedContracts: Map[ContractId, SerializableContract],
    ): TransactionMetadataForHashing = {

      val asFatContracts = disclosedContracts
        .map { case (contractId, serializedNode) =>
          // Salt is not hashed in V1, so it's not relevant for now, but the hashing function takes a FatContractInstance
          // so we extract it and pass it in still
          val salt = serializedNode.contractSalt
            .map(_.toProtoV30.salt)
            .map(Bytes.fromByteString)
            .getOrElse(Bytes.Empty)
          contractId -> FatContractInstance.fromCreateNode(
            serializedNode.toLf,
            serializedNode.ledgerCreateTime.toLf,
            salt,
          )
        }

      TransactionMetadataForHashing(
        actAs,
        commandId,
        transactionUUID,
        mediatorGroup,
        domainId,
        ledgerEffectiveTime,
        submissionTime,
        SortedMap.from(asFatContracts),
      )
    }
  }

  final case class TransactionMetadataForHashing(
      actAs: SortedSet[Ref.Party],
      commandId: Ref.CommandId,
      transactionUUID: UUID,
      mediatorGroup: Int,
      domainId: DomainId,
      ledgerEffectiveTime: Option[Time.Timestamp],
      submissionTime: Time.Timestamp,
      disclosedContracts: SortedMap[ContractId, FatContractInstance],
  )

  private def computeHashV1(
      transaction: VersionedTransaction,
      metadata: TransactionMetadataForHashing,
      nodeSeeds: Map[NodeId, LfHash],
  ): Either[HashError, Hash] = {
    def catchHashingErrors[T](f: => T): Either[HashError, T] =
      scala.util
        .Try(f)
        .toEither
        .leftMap {
          case nodeHashErr: LfHash.NodeHashingError => nodeHashErr.msg
          case hashErr: LfHash.HashingError => hashErr.msg
          case err => err.getMessage
        }
        .leftMap(HashingFailed.apply)

    val v1Metadata = LfHash.TransactionMetadataBuilderV1.Metadata(
      metadata.actAs,
      metadata.commandId,
      metadata.transactionUUID,
      metadata.mediatorGroup,
      metadata.domainId.toProtoPrimitive,
      metadata.ledgerEffectiveTime,
      metadata.submissionTime,
      metadata.disclosedContracts,
    )

    for {
      transactionHash <- catchHashingErrors(LfHash.hashTransactionV1(transaction, nodeSeeds))
      metadataHash <- catchHashingErrors(LfHash.hashTransactionMetadataV1(v1Metadata))
    } yield {
      Hash
        .digest(
          HashPurpose.PreparedSubmission,
          transactionHash.bytes.toByteString.concat(metadataHash.bytes.toByteString),
          HashAlgorithm.Sha256,
        )
    }
  }

  def computeVersionedHash(
      hashVersion: HashingSchemeVersion,
      transaction: VersionedTransaction,
      metadata: TransactionMetadataForHashing,
      nodeSeeds: Map[NodeId, LfHash],
      protocolVersion: ProtocolVersion,
  ): Either[HashError, Hash] = {
    val supportedVersions =
      HashingSchemeVersion.getHashingSchemeVersionsForProtocolVersion(protocolVersion)
    if (!supportedVersions.contains(hashVersion)) {
      Left(
        UnsupportedHashingSchemeVersion(
          hashVersion,
          protocolVersion,
          HashingSchemeVersion.minProtocolVersionForHSV(hashVersion),
          supportedVersions,
        )
      )
    } else {
      hashVersion match {
        case HashingSchemeVersion.V1 => computeHashV1(transaction, metadata, nodeSeeds)
      }
    }
  }

  def verifySignatures(
      hash: Hash,
      signatures: Map[PartyId, Seq[Signature]],
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Set[LfPartyId]] =
    signatures.toList
      .parTraverse { case (party, signatures) =>
        for {
          authInfo <- EitherT(
            cryptoSnapshot.ipsSnapshot
              .partyAuthorization(party)
              .map(
                _.toRight(s"Could not find party signing keys for $party.")
              )
          )

          validSignatures <- EitherT.fromEither[FutureUnlessShutdown](signatures.traverse {
            signature =>
              authInfo.signingKeys
                .find(_.fingerprint == signature.signedBy)
                .toRight(s"Signing key ${signature.signedBy} is not a valid key for $party")
                .flatMap(key =>
                  cryptoSnapshot.pureCrypto
                    .verifySignature(hash, key, signature)
                    .map(_ => key.fingerprint)
                    .leftMap(_.toString)
                )
          })
          validSignaturesSet = validSignatures.toSet
          _ <- EitherT.cond[FutureUnlessShutdown](
            validSignaturesSet.size == validSignatures.size,
            (),
            s"The following signatures were provided one or more times for $party, all signatures must be unique: ${validSignatures
                .diff(validSignaturesSet.toList)}",
          )
          _ <- EitherT.cond[FutureUnlessShutdown](
            validSignaturesSet.size >= authInfo.threshold.unwrap,
            (),
            s"Received ${validSignatures.size} signatures, but expected ${authInfo.threshold} for $party",
          )
        } yield party.toLf
      }
      .map(_.toSet)

}
