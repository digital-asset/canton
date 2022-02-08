// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import java.util.UUID
import cats.data.EitherT
import com.daml.ledger.participant.state.v2.ChangeId
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator.{
  SeedData,
  SeedForTransaction,
  SeedForTransfer,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferOutRequest
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult
import com.digitalasset.canton._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

/** Creates seeds and UUIDs for requests.
  */
class SeedGenerator(val hmacOps: HmacPrivateOps, hashOps: HashOps)(implicit ec: ExecutionContext) {

  /** Yields a hash from the method parameters and [[crypto.HmacPrivateOps.hmac]].
    * It is assumed that the hash uniquely identifies the method parameters.
    *
    * If two instances of this class have different `hmacOps`, they will create different hashes, even if
    * the method is called with the same parameters.
    *
    * Moreover, if the secret key used by `hmacOps` is unknown, the result of this method cannot be predicted.
    */
  def generateSeedForTransaction(
      changeId: ChangeId,
      originDomain: DomainId,
      let: CantonTimestamp,
      transactionUuid: UUID,
  ): EitherT[Future, SaltError, Salt] = {
    val seedData = SeedForTransaction(changeId, originDomain, let, transactionUuid)
    createSalt(seedData)
  }

  def generateSeedForTransferOut(
      request: TransferOutRequest,
      transferOutUuid: UUID,
  ): EitherT[Future, SaltError, Salt] = {
    val seedData = SeedForTransfer(
      request.contractId,
      request.originDomain,
      request.targetDomain,
      hashOps.digest(
        HashPurpose.TransferViewTreeMessageSeed,
        request.targetTimeProof.getCryptographicEvidence,
      ),
      transferOutUuid,
    )
    createSalt(seedData)
  }

  def generateSeedForTransferIn(
      contractId: LfContractId,
      transferOutResultEvent: DeliveredTransferOutResult,
      targetDomain: DomainId,
      transferInUuid: UUID,
  ): EitherT[Future, SaltError, Salt] = {
    val seedData = SeedForTransfer(
      contractId,
      transferOutResultEvent.unwrap.domainId,
      targetDomain,
      hashOps.digest(
        HashPurpose.TransferViewTreeMessageSeed,
        transferOutResultEvent.result.content.getCryptographicEvidence,
      ),
      transferInUuid,
    )
    createSalt(seedData)
  }

  def generateUuid(): UUID = UUID.randomUUID()

  private def createSalt(seedData: SeedData): EitherT[Future, SaltError, Salt] =
    Salt.generate(seedData.toDeterministicByteString, hmacOps)
}

object SeedGenerator {

  trait SeedData {
    def toDeterministicByteString: ByteString
  }

  @VisibleForTesting
  case class SeedForTransaction(
      changeId: ChangeId,
      domainId: DomainId,
      let: CantonTimestamp,
      transactionUuid: UUID,
  ) extends SeedData {
    override def toDeterministicByteString: ByteString = {
      DeterministicEncoding
        .encodeBytes(changeId.hash.bytes.toByteString)
        .concat(DeterministicEncoding.encodeString(domainId.toProtoPrimitive))
        .concat(DeterministicEncoding.encodeInstant(let.toInstant))
        .concat(DeterministicEncoding.encodeString(transactionUuid.toString))
    }
  }

  case class SeedForTransfer(
      contractId: LfContractId,
      originDomain: DomainId,
      targetDomain: DomainId,
      messageHash: Hash,
      requestUuid: UUID,
  ) extends SeedData {
    override def toDeterministicByteString: ByteString =
      DeterministicEncoding
        .encodeString(contractId.toString)
        .concat(DeterministicEncoding.encodeString(originDomain.toProtoPrimitive))
        .concat(DeterministicEncoding.encodeString(targetDomain.toProtoPrimitive))
        .concat(DeterministicEncoding.encodeBytes(messageHash.getCryptographicEvidence))
        .concat(DeterministicEncoding.encodeString(requestUuid.toString))
  }

}
