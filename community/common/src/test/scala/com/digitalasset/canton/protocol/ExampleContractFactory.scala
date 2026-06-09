// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{Salt, TestHash, TestSalt}
import com.digitalasset.canton.util.{LfTransactionBuilder, TestContractHasher}
import com.digitalasset.canton.{LfPackageId, LfPartyId, protocol}
import com.digitalasset.daml.lf.crypto.SValueHash.assertHashContractKey
import com.digitalasset.daml.lf.data.Ref.PackageName
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.transaction.*
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{ContractId, ValueInt64}
import org.scalatest.EitherValues
import org.scalatest.Inside.inside

import java.util.concurrent.atomic.AtomicLong

object ExampleContractFactory extends EitherValues {

  private val atomic = new AtomicLong(10)
  private def nextInt64(): Long = atomic.getAndIncrement()
  private def nextInt(): Int = (nextInt64() % Int.MaxValue).toInt

  private val unicumGenerator = new UnicumGenerator(new SymbolicPureCrypto())

  def lfHash(index: Int = nextInt()): LfHash =
    LfHash.assertFromBytes(
      Bytes.assertFromString(f"$index%04x".padTo(LfHash.underlyingHashLength * 2, '0'))
    )

  val signatory: LfPartyId = LfPartyId.assertFromString("signatory::default")
  val observer: LfPartyId = LfPartyId.assertFromString("observer::default")
  val extra: LfPartyId = LfPartyId.assertFromString("extra::default")

  val packageId: LfPackageId = LfTransactionBuilder.defaultPackageId
  val templateId: LfTemplateId = LfTransactionBuilder.defaultTemplateId
  val packageName: PackageName = LfTransactionBuilder.defaultPackageName

  def build[Time <: CreationTime](
      templateId: Ref.Identifier = templateId,
      packageName: Ref.PackageName = packageName,
      argument: Value = ValueInt64(nextInt64()),
      createdAt: Time = CreationTime.CreatedAt(Time.Timestamp.now()),
      salt: Salt = TestSalt.generateSalt(nextInt()),
      signatories: Set[Ref.Party] = Set(signatory),
      stakeholders: Set[Ref.Party] = Set(signatory, observer, extra),
      keyOpt: Option[GlobalKeyWithMaintainers] = None,
      cantonContractIdVersion: CantonContractIdV1Version = CantonContractIdVersion.maxV1,
      overrideContractId: Option[ContractId] = None,
  ): GenContractInstance { type InstCreatedAtTime <: Time } = {

    val discriminator = lfHash()

    // Template ID must be common across contract and key
    val contractTemplateId = keyOpt.map(_.globalKey.templateId).getOrElse(templateId)

    val create = Node.Create(
      coid = LfContractId.V1(discriminator),
      templateId = contractTemplateId,
      packageName = packageName,
      arg = argument,
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = keyOpt,
      version = if (keyOpt.isDefined) SerializationVersion.V2 else SerializationVersion.V1,
    )
    fromCreateInternal[Time](create, createdAt, salt, cantonContractIdVersion, overrideContractId)
  }

  def fromCreate(
      create: protocol.LfNodeCreate,
      createdAt: CreationTime.CreatedAt = CreationTime.CreatedAt(Time.Timestamp.now()),
      cantonContractIdVersion: CantonContractIdV1Version = CantonContractIdVersion.maxV1,
  ): GenContractInstance { type InstCreatedAtTime <: CreationTime.CreatedAt } =
    fromCreateInternal(
      create,
      createdAt = createdAt,
      cantonContractIdVersion = cantonContractIdVersion,
    )

  private def fromCreateInternal[Time <: CreationTime](
      create: protocol.LfNodeCreate,
      createdAt: Time,
      salt: Salt = TestSalt.generateSalt(nextInt()),
      cantonContractIdVersion: CantonContractIdV1Version,
      overrideContractId: Option[ContractId] = None,
  ): GenContractInstance { type InstCreatedAtTime <: Time } = {

    val unsuffixed = FatContractInstance.fromCreateNode(
      create,
      createdAt,
      ContractAuthenticationDataV1(salt)(cantonContractIdVersion).toLfBytes,
    )

    val contractHash =
      TestContractHasher.Sync.hash(create, cantonContractIdVersion.contractHashingMethod)

    val unicum = unicumGenerator
      .recomputeUnicum(unsuffixed, cantonContractIdVersion, contractHash)
      .value

    val discriminator = inside(create.coid) { case ContractId.V1(discriminator, _) =>
      discriminator
    }

    val contractId =
      overrideContractId.getOrElse(cantonContractIdVersion.fromDiscriminator(discriminator, unicum))

    val inst = FatContractInstance.fromCreateNode(
      create.copy(coid = contractId),
      createdAt,
      ContractAuthenticationDataV1(salt)(cantonContractIdVersion).toLfBytes,
    )

    ContractInstance.create(inst).value
  }

  def buildContractId(
      index: Int = nextInt(),
      cantonContractIdVersion: CantonContractIdV1Version = CantonContractIdVersion.maxV1,
  ): ContractId =
    cantonContractIdVersion.fromDiscriminator(lfHash(index), Unicum(TestHash.digest(index)))

  def buildKeyWithMaintainers(
      templateId: Ref.Identifier = templateId,
      packageName: Ref.PackageName = packageName,
      maintainers: Set[Ref.Party] = Set(signatory),
      value: Long = nextInt64(),
  ): GlobalKeyWithMaintainers =
    GlobalKeyWithMaintainers.assertBuild(
      templateId = templateId,
      value = Value.ValueInt64(value),
      valueHash =
        assertHashContractKey(packageName, templateId.qualifiedName, SValue.SInt64(value)),
      maintainers = maintainers,
      packageName = packageName,
    )

  def modify[Time <: CreationTime](
      base: GenContractInstance { type InstCreatedAtTime <: Time },
      contractId: Option[ContractId] = None,
      metadata: Option[ContractMetadata] = None,
      arg: Option[Value] = None,
      templateId: Option[LfTemplateId] = None,
      packageName: Option[PackageName] = None,
      authenticationData: Option[Bytes] = None,
      createdAt: Option[Time] = None,
  ): GenContractInstance { type InstCreatedAtTime <: Time } = {
    val create = base.toLf
    val inst = FatContractInstance.fromCreateNode(
      base.toLf.copy(
        coid = contractId.getOrElse(create.coid),
        templateId = templateId.getOrElse(create.templateId),
        arg = arg.getOrElse(create.arg),
        signatories = metadata.map(_.signatories).getOrElse(create.signatories),
        stakeholders = metadata.map(_.stakeholders).getOrElse(create.stakeholders),
        keyOpt = metadata.map(_.maybeKeyWithMaintainers).getOrElse(create.keyOpt),
        packageName = packageName.getOrElse(create.packageName),
      ),
      createTime = createdAt.getOrElse(base.inst.createdAt),
      authenticationData = authenticationData.getOrElse(base.inst.authenticationData),
    )
    ContractInstance.create(inst).value
  }

}
