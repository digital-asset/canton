// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.{Hash, HashOps, HashPurpose, HmacOps, Salt}
import com.digitalasset.canton.data.ViewPosition
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.digitalasset.canton.topology.SynchronizerId

import java.util.UUID

/** Generates [[ContractSalt]]s and [[Unicum]]s for contract IDs such that the [[Unicum]] is a
  * cryptographic commitment to the following:
  *   - The [[com.digitalasset.canton.topology.SynchronizerId]] of the transaction that creates the
  *     contract
  *   - The [[com.digitalasset.canton.topology.MediatorId]] of the mediator that handles the
  *     transaction request
  *   - The `UUID` of the transaction that creates the contract
  *   - The [[com.digitalasset.canton.data.ViewPosition]] of the view whose core creates the
  *     contract
  *   - The index of the create node within the view
  *   - The ledger time when the contract was created
  *   - The template ID and the template arguments of the contract, including the agreement text
  *
  * The commitment is implemented as a blinded hash with the view action salt as the blinding
  * factor.
  *
  * The above data is split into two groups:
  *   - [[com.digitalasset.canton.topology.SynchronizerId]],
  *     [[com.digitalasset.canton.topology.MediatorId]], the `UUID`, the
  *     [[com.digitalasset.canton.data.ViewPosition]], and the index contribute to the blinded hash
  *     of the [[ContractSalt]].
  *   - The ledger time and the template arguments
  *
  * The [[Unicum]] is then the cryptographic hash of the [[ContractSalt]] and the second group.
  *
  * The [[ContractSalt]] contains all the information that ensures uniqueness of contract IDs in
  * Canton. The second group contains the information that is relevant for using the contract in
  * transactions. The commitment to the information in the second group can be opened by revealing
  * the [[ContractSalt]]. Since the [[ContractSalt]] is a blinded hash, such an opening does not
  * reveal information about the data in the first group.
  *
  * =Properties=
  *
  * ==Global Uniqueness==
  *
  * '''If a transaction is added to the virtual synchronizer ledger for a given synchronizer, then
  * the [[Unicum]] is globally unique unless a hash collision occurs.'''
  *
  * Contracts with the same [[Unicum]] must run over the same synchronizer, have the same
  * transaction UUID, and are handled by the same mediator. The definition of the virtual
  * synchronizer ledger ensures that transaction UUIDs are unique within the ledger effective time
  * tolerance and within the mediator handling the request, and that the sequencing time deviates
  * from the ledger time by at most this tolerance. So two contracts with the same [[Unicum]] must
  * be generated by the same transaction. However, the [[com.digitalasset.canton.data.ViewPosition]]
  * and the create index uniquely identify the node in the transaction that creates the contract.
  *
  * We include both the [[com.digitalasset.canton.topology.SynchronizerId]] and the
  * [[com.digitalasset.canton.topology.MediatorId]] in the [[ContractSalt]] because we cannot
  * exclude that mediators on different synchronizers happen to have the same identifier and there
  * may be mupltiple mediators on a synchronizer.
  *
  * ==No Information Leak of Template Arguments==
  *
  * '''If the submitter is honest and chooses a random transaction seed, the [[Unicum]] does not
  * leak information about template arguments.'''
  *
  * The transaction seed's randomness propagates to the action seed through the seed derivation
  * scheme. Since the honest submitter does not leak the transaction seed and shows the action seed
  * only to the witnesses of the view, the [[ContractSalt]] looks random to non-witnesses of the
  * view. Accordingly, the [[ContractSalt]] blinds the template arguments.
  *
  * ==Authentication of Contract Details==
  *
  * '''The [[Unicum]] authenticates the contract details (ledger time and template arguments) if the
  * hash function is preimage resistant.'''
  *
  * By checking the hash of the [[ContractSalt]] and the contract details against the [[Unicum]],
  * everyone can verify that they fit together. As the hash function is preimage resistant, it is
  * therefore computationally infeasible for a participant to find a different [[ContractSalt]] such
  * that different contract details lead to the same hash.
  *
  * ==No Information Leak of Contract Creation==
  *
  * '''Participants learning about the contract only through divulgence or disclosure do not learn
  * in which transaction the contract was created unless the submitter or witnesses of the creation
  * leak this information.'''
  *
  * By the honesty assumption, the action seed is a random value to those participants. Accordingly,
  * since the [[ContractSalt]] contains all the information that ties the contract to a particular
  * transaction, the participants cannot say which transaction with the same ledger time created the
  * contract.
  *
  * ==No Information Leak of Contract Details==
  *
  * '''The [[Unicum]] does not leak the contract details when a contract ID is shown to a third
  * party if the submitter and all witnesses and divulgees are honest.'''
  *
  * By the honesty assumption, the action seed is a random value to the third party, and so is the
  * [[ContractSalt]]. This entropy hides the contract details to the third party.
  */
class UnicumGenerator(cryptoOps: HashOps with HmacOps) {

  /** Creates the [[ContractSalt]] and [[Unicum]] for a create node.
    *
    * @param synchronizerId
    *   the synchronizer on which this transaction is sequenced
    * @param mediator
    *   the mediator that is responsible for handling the request that creates the contract
    * @param transactionUuid
    *   the UUID of the transaction
    * @param viewPosition
    *   the position of the view whose core creates the contract
    * @param viewParticipantDataSalt
    *   the salt of the [[com.digitalasset.canton.data.ViewParticipantData]] of the view whose core
    *   creates the contract
    * @param createIndex
    *   the index of the node creating the contract (starting at 0). Only create nodes and only
    *   nodes that belong to the core of the view with salt `viewActionSalt` have an index.
    * @param ledgerCreateTime
    *   the ledger time at which the contract is created
    * @param metadata
    *   contract metadata
    * @param suffixedContractInstance
    *   the serializable raw contract instance of the contract where contract IDs have already been
    *   suffixed.
    *
    * @see
    *   UnicumGenerator for the construction details and the security properties
    */
  def generateSaltAndUnicum(
      synchronizerId: SynchronizerId,
      mediator: MediatorGroupRecipient,
      transactionUuid: UUID,
      viewPosition: ViewPosition,
      viewParticipantDataSalt: Salt,
      createIndex: Int,
      ledgerCreateTime: LedgerCreateTime,
      metadata: ContractMetadata,
      suffixedContractInstance: SerializableRawContractInstance,
  ): (ContractSalt, Unicum) = {
    val contractSalt =
      ContractSalt.create(cryptoOps)(
        transactionUuid,
        synchronizerId,
        mediator,
        viewParticipantDataSalt,
        createIndex,
        viewPosition,
      )
    val unicumHash = computeUnicumV3Hash(
      ledgerCreateTime = ledgerCreateTime,
      metadata,
      suffixedContractInstance = suffixedContractInstance,
      contractSalt = contractSalt.unwrap,
    )

    contractSalt -> Unicum(unicumHash)
  }

  /** Re-computes a contract's [[Unicum]] based on the provided salt. Used for authenticating
    * contracts.
    *
    * @param contractSalt
    *   the [[ContractSalt]] computed when the original contract id was generated.
    * @param ledgerCreateTime
    *   the ledger time at which the contract is created
    * @param metadata
    *   contract metadata
    * @param suffixedContractInstance
    *   the serializable raw contract instance of the contract where contract IDs have already been
    *   suffixed.
    * @return
    *   the unicum if successful or a failure if the contract salt size is mismatching the
    *   predefined size.
    */
  def recomputeUnicum(
      contractSalt: Salt,
      ledgerCreateTime: LedgerCreateTime,
      metadata: ContractMetadata,
      suffixedContractInstance: SerializableRawContractInstance,
  ): Either[String, Unicum] = {
    val contractSaltSize = contractSalt.size
    Either.cond(
      contractSaltSize.toLong == cryptoOps.defaultHmacAlgorithm.hashAlgorithm.length,
      Unicum(
        computeUnicumV3Hash(
          ledgerCreateTime,
          metadata,
          suffixedContractInstance,
          contractSalt,
        )
      ),
      s"Invalid contract salt size ($contractSaltSize)",
    )
  }

  private def computeUnicumV3Hash(
      ledgerCreateTime: LedgerCreateTime,
      metadata: ContractMetadata,
      suffixedContractInstance: SerializableRawContractInstance,
      contractSalt: Salt,
  ): Hash = {
    val nonSignatoryStakeholders = metadata.stakeholders -- metadata.signatories

    val hash = cryptoOps
      .build(HashPurpose.Unicum)
      // The salt's length is determined by the hash algorithm and the contract ID version determines the hash algorithm,
      // so salts have fixed length.
      .addWithoutLengthPrefix(contractSalt.forHashing)
      .addWithoutLengthPrefix(DeterministicEncoding.encodeInstant(ledgerCreateTime.toInstant))
      .add(
        DeterministicEncoding.encodeSeqWith(metadata.signatories.toSeq.sorted)(
          DeterministicEncoding.encodeParty
        )
      )
      .add(
        DeterministicEncoding.encodeSeqWith(nonSignatoryStakeholders.toSeq.sorted)(
          DeterministicEncoding.encodeParty
        )
      )
      // When present, the contract key has a fixed length, so we do not need a length prefix
      .addWithoutLengthPrefix(
        DeterministicEncoding
          .encodeOptionWith(metadata.maybeKeyWithMaintainers.map(_.globalKey.hash))(
            _.bytes.toByteString
          )
      )
      .add(
        DeterministicEncoding.encodeOptionWith(
          metadata.maybeKeyWithMaintainers.map(_.maintainers)
        ) { maintainers =>
          DeterministicEncoding.encodeSeqWith(maintainers.toSeq.sorted)(
            DeterministicEncoding.encodeParty
          )
        }
      )
      // The hash of the contract instance has a fixed length, so we do not need a length prefix
      .addWithoutLengthPrefix(suffixedContractInstance.contractHash.bytes.toByteString)
      .finish()

    hash
  }
}
