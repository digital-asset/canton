// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml
package lf
package transaction

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.transaction.ContractInstanceCoderSpec.{
  addUnknownField,
  adjustStakeholders,
  bytesGen,
  makePartyFresh,
  normalizeCreate,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.google.protobuf
import com.google.protobuf.{ByteString, Message}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.jdk.CollectionConverters.*

import collection.immutable.TreeSet

final class ContractInstanceCoderSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with EitherAssertions
    with ScalaCheckPropertyChecks {

  // TODO https://github.com/digital-asset/daml/issues/18457
  // Tests that messages with unknown field are rejected

  import com.digitalasset.daml.lf.value.test.ValueGenerators.*

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000, sizeRange = 10)

  "do Versioned" in {
    forAll(Gen.oneOf(SerializationVersion.All), bytesGen, minSuccessful(5)) { (version, bytes) =>
      val encoded = ContractInstanceCoder.encodeVersioned(version, bytes)
      val Right(decoded) = ContractInstanceCoder.decodeVersioned(encoded)
      decoded shouldBe Versioned(version, bytes)
    }
  }

  "reject versioned message with trailing data" in {
    forAll(
      Gen.oneOf(SerializationVersion.All),
      bytesGen,
      bytesGen
        .filterNot(_.isEmpty)
        // 0x12,0x00 is protobuf's format for an additional field with length 0,
        // we filter that out here since decode will happily ignore it, causing
        // the test to fail.
        // This is best effort, we could filter all possible additional fields
        // but that would take time. In the meantime we wait to see if we get
        // another flake.
        .filterNot(bs => "(\u0012\u0000)+".r.matches(bs.toStringUtf8)),
      minSuccessful(5),
    ) { (version, bytes1, bytes2) =>
      val encoded = ContractInstanceCoder.encodeVersioned(version, bytes1)
      ContractInstanceCoder.decodeVersioned(encoded concat bytes2) shouldBe a[Left[?, ?]]
    }
  }

  "reject Versioned message with unknown fields" in {
    forAll(
      Gen.oneOf(SerializationVersion.All),
      bytesGen,
      Arbitrary.arbInt.arbitrary,
      bytesGen.filterNot(_.isEmpty),
      minSuccessful(5),
    ) { (version, payload, i, extraData) =>
      val encoded = ContractInstanceCoder.encodeVersioned(version, payload)
      val proto = TransactionOuterClass.Versioned.parseFrom(encoded)
      val reencoded = addUnknownField(proto.toBuilder, i, extraData).toByteString
      assert(reencoded != encoded)
      inside(ContractInstanceCoder.decodeVersioned(reencoded)) {
        case Left(DecodeError(errorMessage)) =>
          errorMessage should include("unknown field")
      }
    }
  }

  "do FatContractInstance" in {
    forAll(
      malformedCreateNodeGen(),
      timestampGen,
      bytesGen,
      minSuccessful(5),
    ) { (create, time, salt) =>
      val normalizedCreate = adjustStakeholders(normalizeCreate(create))
      val instance = FatContractInstance.fromCreateNode(
        normalizedCreate,
        CreationTime.CreatedAt(time),
        data.Bytes.fromByteString(salt),
      )
      val Right(encoded) = ContractInstanceCoder.encodeFatContractInstance(instance)
      val Right(decoded) = ContractInstanceCoder.decodeFatContractInstance(encoded)

      decoded shouldBe instance
    }
  }

  def hackProto(
      instance: FatContractInstance,
      f: TransactionOuterClass.FatContractInstance.Builder => Message,
  ): ByteString = {
    val Right(encoded) = ContractInstanceCoder.encodeFatContractInstance(instance)
    val Right(Versioned(v, bytes)) = ContractInstanceCoder.decodeVersioned(encoded)
    val builder = TransactionOuterClass.FatContractInstance.parseFrom(bytes).toBuilder
    ContractInstanceCoder.encodeVersioned(v, f(builder).toByteString)
  }

  "reject FatContractInstance with unknown fields" in {
    forAll(
      malformedCreateNodeGen(),
      timestampGen,
      bytesGen,
      Arbitrary.arbInt.arbitrary,
      bytesGen.filterNot(_.isEmpty),
      minSuccessful(5),
    ) { (create, time, salt, i, extraBytes) =>
      val normalizedCreate = adjustStakeholders(normalizeCreate(create))
      val instance = FatContractInstance.fromCreateNode(
        normalizedCreate,
        CreationTime.CreatedAt(time),
        data.Bytes.fromByteString(salt),
      )
      val bytes = hackProto(instance, addUnknownField(_, i, extraBytes))
      inside(ContractInstanceCoder.decodeFatContractInstance(bytes)) {
        case Left(DecodeError(errorMessage)) =>
          errorMessage should include("unknown field")
      }
    }
  }

  "reject FatContractInstance with key but empty maintainers" in {
    forAll(
      malformedCreateNodeGen(SerializationVersion.V2),
      timestampGen,
      bytesGen,
      minSuccessful(2),
    ) { (create, time, salt) =>
      forAll(
        keyWithMaintainersGen(create.templateId, create.packageName),
        minSuccessful(2),
      ) { key =>
        val normalizedCreate = adjustStakeholders(normalizeCreate(create))
        val instance = FatContractInstance.fromCreateNode(
          normalizedCreate,
          CreationTime.CreatedAt(time),
          data.Bytes.fromByteString(salt),
        )
        val Right(protoKey) =
          ContractInstanceCoder.internal.encodeKeyWithMaintainers(create.version, key)
        val bytes = hackProto(
          instance,
          _.setContractKeyWithMaintainers(protoKey.toBuilder.clearMaintainers()).build(),
        )
        inside(ContractInstanceCoder.decodeFatContractInstance(bytes)) {
          case Left(DecodeError(errorMessage)) =>
            errorMessage should include("key without maintainers")
        }
      }
    }
  }

  "reject FatContractInstance with empty signatories" in {
    forAll(
      malformedCreateNodeGen(),
      timestampGen,
      bytesGen,
      minSuccessful(3),
    ) { (create, time, salt) =>
      val normalizedCreate = adjustStakeholders(normalizeCreate(create))
      val instance = FatContractInstance.fromCreateNode(
        normalizedCreate,
        CreationTime.CreatedAt(time),
        data.Bytes.fromByteString(salt),
      )
      val bytes =
        hackProto(
          instance,
          _.clearContractKeyWithMaintainers().clearNonMaintainerSignatories().build(),
        )
      inside(ContractInstanceCoder.decodeFatContractInstance(bytes)) {
        case Left(DecodeError(errorMessage)) =>
          errorMessage should include(
            "maintainers or non_maintainer_signatories should be non empty"
          )
      }
    }
  }

  def hackKeyProto(
      version: SerializationVersion,
      key: GlobalKeyWithMaintainers,
      f: TransactionOuterClass.KeyWithMaintainers.Builder => TransactionOuterClass.KeyWithMaintainers.Builder,
  ): TransactionOuterClass.KeyWithMaintainers = {
    val Right(encoded) = ContractInstanceCoder.internal.encodeKeyWithMaintainers(version, key)
    f(encoded.toBuilder).build()
  }

  "reject FatContractInstance with nonMaintainerSignatories containing maintainers" in {
    forAll(
      party,
      malformedCreateNodeGen(SerializationVersion.V2),
      timestampGen,
      bytesGen,
      minSuccessful(2),
    ) { (party, create, time, salt) =>
      forAll(
        keyWithMaintainersGen(create.templateId, create.packageName),
        minSuccessful(2),
      ) { key =>
        val normalizedCreate = adjustStakeholders(normalizeCreate(create))
        val instance = FatContractInstance.fromCreateNode(
          normalizedCreate,
          CreationTime.CreatedAt(time),
          data.Bytes.fromByteString(salt),
        )
        val nonMaintainerSignatories = instance.nonMaintainerSignatories + party
        val maintainers = TreeSet.from(key.maintainers + party)
        val protoKey = hackKeyProto(
          create.version,
          key,
          { builder =>
            builder.clearMaintainers()
            maintainers.foreach(builder.addMaintainers)
            builder
          },
        )

        val bytes = hackProto(
          instance,
          { builder =>
            builder.clearNonMaintainerSignatories()
            nonMaintainerSignatories.foreach(builder.addNonMaintainerSignatories)
            builder.setContractKeyWithMaintainers(protoKey)
            builder.build()
          },
        )
        inside(ContractInstanceCoder.decodeFatContractInstance(bytes)) {
          case Left(DecodeError(errorMessage)) =>
            errorMessage should include("is declared as maintainer and nonMaintainerSignatory")
        }
      }
    }
  }

  "reject FatContractInstance with nonSignatoryStakeholders containing maintainers" in {
    forAll(
      party,
      malformedCreateNodeGen(SerializationVersion.V2),
      timestampGen,
      bytesGen,
      minSuccessful(2),
    ) { (party, create, time, salt) =>
      forAll(
        keyWithMaintainersGen(create.templateId, create.packageName),
        minSuccessful(2),
      ) { key =>
        val normalizedCreate = adjustStakeholders(normalizeCreate(create))
        val instance = FatContractInstance.fromCreateNode(
          normalizedCreate,
          CreationTime.CreatedAt(time),
          data.Bytes.fromByteString(salt),
        )
        val maintainers = TreeSet.from(key.maintainers + party)
        val nonMaintainerSignatories =
          instance.nonMaintainerSignatories -- key.maintainers - party
        val nonSignatoryStakeholders = instance.nonSignatoryStakeholders + party
        val protoKey = hackKeyProto(
          create.version,
          key,
          { builder =>
            builder.clearMaintainers()
            maintainers.foreach(builder.addMaintainers)
            builder
          },
        )

        val bytes = hackProto(
          instance,
          { builder =>
            builder.setContractKeyWithMaintainers(protoKey)
            builder.clearNonMaintainerSignatories()
            nonMaintainerSignatories.foreach(builder.addNonMaintainerSignatories)
            builder.clearNonSignatoryStakeholders()
            nonSignatoryStakeholders.foreach(builder.addNonSignatoryStakeholders)
            builder.build()
          },
        )
        inside(ContractInstanceCoder.decodeFatContractInstance(bytes)) {
          case Left(DecodeError(errorMessage)) =>
            errorMessage should include("is declared as signatory and nonSignatoryStakeholder")
        }
      }
    }

  }

  "reject FatContractInstance with nonSignatoryStakeholders containing nonMaintainerSignatories" in {
    forAll(
      party,
      malformedCreateNodeGen(),
      timestampGen,
      bytesGen,
      minSuccessful(4),
    ) { (party, create, time, salt) =>
      val normalizedCreate = adjustStakeholders(normalizeCreate(create))
      val instance = FatContractInstance.fromCreateNode(
        normalizedCreate,
        CreationTime.CreatedAt(time),
        data.Bytes.fromByteString(salt),
      )
      val party_ = makePartyFresh(party, create)

      val nonMaintainerSignatories = instance.nonMaintainerSignatories + party_
      val nonSignatoryStakeholders = instance.nonSignatoryStakeholders + party_

      val bytes = hackProto(
        instance,
        { builder =>
          builder.clearNonMaintainerSignatories()
          nonMaintainerSignatories.foreach(builder.addNonMaintainerSignatories)
          builder.clearNonSignatoryStakeholders()
          nonSignatoryStakeholders.foreach(builder.addNonSignatoryStakeholders)
          builder.build()
        },
      )
      inside(ContractInstanceCoder.decodeFatContractInstance(bytes)) {
        case Left(DecodeError(errorMessage)) =>
          errorMessage should include("is declared as signatory and nonSignatoryStakeholder")

      }
    }
  }

  // The decoder must never build a FatContractInstanceImpl that violates one of the requirements
  // checked in its body. Whatever the (parsable) proto is, decoding returns a DecodeError or a
  // valid instance, but never throws.
  "decodeFatContractInstance" should {
    import scala.util.{Failure, Success, Try}

    val alice = Party.assertFromString("alice")
    val pkgName = Ref.PackageName.assertFromString("pkg-name")
    val templateId = Ref.Identifier.assertFromString("-dummyPkg-:Module:Template")

    def bytesOf(bs: Int*) =
      data.Bytes.fromByteString(ByteString.copyFrom(bs.map(_.toByte).toArray))
    val local12 = bytesOf(Seq.fill(Value.ContractId.V2.localSize)(1)*)
    val v1Hash = crypto.Hash.hashPrivateKey("cid")

    val localCid: Value.ContractId = Value.ContractId.V2.assertBuild(local12, data.Bytes.Empty)
    val relativeCid: Value.ContractId =
      Value.ContractId.V2.assertBuild(local12, bytesOf(0x01, 0x02))
    val absoluteCid: Value.ContractId =
      Value.ContractId.V2.assertBuild(local12, bytesOf(0x81, 0x02))
    val v1LocalCid: Value.ContractId = Value.ContractId.V1(v1Hash)
    val v1AbsoluteCid: Value.ContractId = Value.ContractId.V1.assertBuild(v1Hash, bytesOf(0x01))

    val relativeCids = Seq(relativeCid)
    val localOrAbsoluteCids = Seq(localCid, absoluteCid, v1LocalCid, v1AbsoluteCid)
    val allCids = relativeCids ++ localOrAbsoluteCids

    val baseKey = GlobalKeyWithMaintainers(
      templateId = templateId,
      value = Value.ValueText("key"),
      crypto.Hash.hashPrivateKey("key-hash"),
      maintainers = Set(alice),
      packageName = pkgName,
    )

    val baseInstance: FatContractInstance = FatContractInstance.fromCreateNode(
      Node.Create(
        coid = relativeCid,
        packageName = pkgName,
        templateId = templateId,
        arg = Value.ValueText("arg"),
        signatories = Set(alice),
        stakeholders = Set(alice),
        keyOpt = None,
        version = SerializationVersion.V2,
      ),
      CreationTime.CreatedAt(data.Time.Timestamp.Epoch),
      data.Bytes.Empty,
    )

    // Raw strings are used for parties, to be able to produce unordered lists and duplicates
    def decode(
        cid: Value.ContractId,
        createdAt: CreationTime,
        nonMaintainerSignatories: Seq[String] = Seq("alice"),
        nonSignatoryStakeholders: Seq[String] = Seq.empty,
        keyMaintainers: Option[Seq[String]] = None,
    ) = {
      val protoKey = keyMaintainers.map { ms =>
        hackKeyProto(
          SerializationVersion.V2,
          baseKey,
          { b =>
            b.clearMaintainers()
            ms.foreach((m: String) => b.addMaintainers(m))
            b
          },
        )
      }
      val bytes = hackProto(
        baseInstance,
        { b =>
          b.setContractId(cid.toBytes.toByteString)
          b.setCreatedAt(CreationTime.encode(createdAt))
          b.clearContractKeyWithMaintainers()
          protoKey.foreach(k => b.setContractKeyWithMaintainers(k))
          b.clearNonMaintainerSignatories()
          nonMaintainerSignatories.foreach((p: String) => b.addNonMaintainerSignatories(p))
          b.clearNonSignatoryStakeholders()
          nonSignatoryStakeholders.foreach((p: String) => b.addNonSignatoryStakeholders(p))
          b.build()
        },
      )
      ContractInstanceCoder.decodeFatContractInstance(bytes)
    }

    val timestamp = CreationTime.CreatedAt(data.Time.Timestamp.Epoch)

    // The whole input space explored by the tests below, decoded once.
    val partyLists: Seq[Seq[String]] = Seq(
      Seq.empty,
      Seq("alice"),
      Seq("bob"),
      Seq("alice", "bob"), // strictly ordered
      Seq("bob", "alice"), // not ordered
      Seq("alice", "alice"), // duplicate
    )
    val keyMaintainerOptions: Seq[Option[Seq[String]]] = None +: partyLists.map(Some(_))
    val createdAts = Seq[CreationTime](CreationTime.Now, timestamp)

    case class Decoding(
        cid: Value.ContractId,
        createdAt: CreationTime,
        keyMaintainers: Option[Seq[String]],
        nonMaintainerSignatories: Seq[String],
        nonSignatoryStakeholders: Seq[String],
    ) {
      override def toString: String =
        s"cid=${cid.coid}, createdAt=$createdAt, keyMaintainers=$keyMaintainers, " +
          s"nonMaintainerSignatories=$nonMaintainerSignatories, " +
          s"nonSignatoryStakeholders=$nonSignatoryStakeholders"
    }

    lazy val results: Seq[(Decoding, Try[Either[DecodeError, FatContractInstance]])] =
      for {
        cid <- allCids
        createdAt <- createdAts
        keyMaintainers <- keyMaintainerOptions
        nonMaintainerSignatories <- partyLists
        nonSignatoryStakeholders <- partyLists
      } yield {
        val input =
          Decoding(
            cid,
            createdAt,
            keyMaintainers,
            nonMaintainerSignatories,
            nonSignatoryStakeholders,
          )
        input -> Try(
          decode(cid, createdAt, nonMaintainerSignatories, nonSignatoryStakeholders, keyMaintainers)
        )
      }

    // A violated requirement shows up as an exception thrown by the FatContractInstanceImpl
    // constructor, so every requirement test starts by checking that nothing threw.
    def assertNoException(): Unit =
      results.foreach { case (input, result) =>
        result match {
          case Failure(e) => fail(s"decoding threw an exception for $input: $e", e)
          case Success(_) => ()
        }
      }

    def decoded: Seq[(Decoding, FatContractInstance)] =
      results.collect { case (input, Success(Right(instance))) => input -> instance }

    // Sanity check: the tests below are only meaningful if the success path is exercised.
    "decode some of the explored inputs successfully" in {
      decoded should not be empty
    }

    // Requirement: maintainers should be a TreeSet
    "satisfy the requirement 'maintainers should be a TreeSet'" in {
      assertNoException()
      decoded.foreach { case (input, instance) =>
        withClue(input) {
          instance.maintainers shouldBe a[TreeSet[?]]
        }
      }
    }

    // Requirement: maintainers should be a subset of signatories
    "satisfy the requirement 'maintainers should be a subset of signatories'" in {
      assertNoException()
      decoded.foreach { case (input, instance) =>
        withClue(input) {
          instance.maintainers.subsetOf(instance.signatories) shouldBe true
        }
      }
      // A key whose maintainers are not declared as signatories is accepted, as the
      // maintainers are added to the signatories.
      inside(decode(relativeCid, timestamp, Seq.empty, Seq.empty, Some(Seq("bob")))) {
        case Right(instance) =>
          instance.signatories shouldBe TreeSet(Party.assertFromString("bob"))
      }
    }

    // Requirement: signatories should be non empty
    "satisfy the requirement 'signatories should be non empty'" in {
      assertNoException()
      decoded.foreach { case (input, instance) =>
        withClue(input) {
          instance.signatories should not be empty
        }
      }
      inside(decode(relativeCid, timestamp, Seq.empty, Seq.empty, None)) {
        case Left(DecodeError(errorMessage)) =>
          errorMessage should include(
            "maintainers or non_maintainer_signatories should be non empty"
          )
      }
    }

    // Requirement: signatories should be a subset of stakeholders
    "satisfy the requirement 'signatories should be a subset of stakeholders'" in {
      assertNoException()
      decoded.foreach { case (input, instance) =>
        withClue(input) {
          instance.signatories.subsetOf(instance.stakeholders) shouldBe true
        }
      }
      // Stakeholders that are not signatories are accepted and kept
      inside(decode(relativeCid, timestamp, Seq("alice"), Seq("bob"), None)) {
        case Right(instance) =>
          instance.stakeholders shouldBe
            TreeSet(Party.assertFromString("alice"), Party.assertFromString("bob"))
      }
    }

    // Requirement: creation time 'now' is not allowed for local and absolute contract ids
    "satisfy the requirement 'creation time now is not allowed for local and absolute contract ids'" in {
      assertNoException()
      decoded.foreach { case (input, instance) =>
        withClue(input) {
          if (instance.createdAt == CreationTime.Now) {
            instance.contractId.isAbsolute shouldBe false
            instance.contractId.isLocal shouldBe false
          }
        }
      }
      localOrAbsoluteCids.foreach { cid =>
        withClue(cid.coid) {
          inside(decode(cid, CreationTime.Now)) { case Left(DecodeError(errorMessage)) =>
            errorMessage should include("'now' is not allowed for local and absolute")
          }
        }
      }
      // ... but 'now' is accepted for relative contract ids ...
      inside(decode(relativeCid, CreationTime.Now)) { case Right(instance) =>
        instance.createdAt shouldBe CreationTime.Now
        instance.contractId shouldBe relativeCid
      }
      // ... and a concrete creation time is accepted for any contract id
      allCids.foreach(cid => decode(cid, timestamp) shouldBe a[Right[?, ?]])
    }

    // Requirement: template ID of the contract key must match the template ID of the contract
    "satisfy the requirement 'template id of the contract key must match the template id of the contract'" in {
      assertNoException()
      decoded.foreach { case (input, instance) =>
        withClue(input) {
          instance.contractKeyWithMaintainers.foreach(
            _.globalKey.templateId shouldBe instance.templateId
          )
        }
      }
      // alice is the key maintainer, so she must not also be a non maintainer signatory
      inside(
        decode(
          relativeCid,
          timestamp,
          nonMaintainerSignatories = Seq.empty,
          keyMaintainers = Some(Seq("alice")),
        )
      ) { case Right(instance) =>
        instance.contractKeyWithMaintainers.map(_.globalKey.templateId) shouldBe
          Some(instance.templateId)
      }
    }
  }

  "toOrderPartySet" should {
    import com.google.protobuf.LazyStringArrayList
    import scala.util.Random.shuffle

    def toProto(strings: Seq[String]) = {
      val l = new LazyStringArrayList()
      strings.foreach(s => l.add(ByteString.copyFromUtf8(s)))
      l
    }

    "accept strictly order list of parties" in {
      forAll(Gen.listOf(party)) { parties =>
        val sortedParties = parties.sorted.distinct
        val proto = toProto(sortedParties)
        inside(ContractInstanceCoder.internal.toPartyTreeSet(proto)) {
          case Right(decoded: TreeSet[Party]) =>
            decoded shouldBe TreeSet.from(sortedParties)
        }
      }
    }

    "reject non sorted list of parties" in {
      forAll(party, Gen.nonEmptyListOf(party)) { (party0, parties0) =>
        val party = Iterator
          .iterate(party0)(p => Party.assertFromString("_" + p))
          .filterNot(parties0.contains)
          .next()
        val parties = party :: parties0
        val sortedParties = parties.sorted
        val nonSortedParties =
          Iterator.iterate(parties)(shuffle(_)).filterNot(_ == sortedParties).next()
        val proto = toProto(nonSortedParties)
        ContractInstanceCoder.internal.toPartyTreeSet(proto) shouldBe a[Left[?, ?]]
      }
    }

    "reject non list with duplicate" in {
      forAll(party, Gen.listOf(party)) { (party, parties) =>
        val partiesWithDuplicate = (party :: party :: parties).sorted
        val proto = toProto(partiesWithDuplicate)
        ContractInstanceCoder.internal.toPartyTreeSet(proto) shouldBe a[Left[?, ?]]
      }
    }
  }

}

private object ContractInstanceCoderSpec {

  private val bytesGen: Gen[ByteString] =
    Gen
      .listOf(Arbitrary.arbByte.arbitrary)
      .map(x => ByteString.copyFrom(x.toArray))

  private def adjustStakeholders(create: Node.Create) = {
    val maintainers = create.keyOpt.fold(Set.empty[Party])(_.maintainers)
    val signatories = create.signatories | maintainers
    val stakeholders = create.stakeholders | signatories
    create.copy(
      signatories = signatories,
      stakeholders = stakeholders,
    )
  }

  private def makePartyFresh(party: Party, create: Node.Create): Party = {
    val contractParties = create.stakeholders ++ create.keyOpt.fold(Set.empty[Party])(_.maintainers)
    Iterator.iterate(party)(p => Party.assertFromString(p + "_")).filterNot(contractParties).next()
  }

  private def normalizeCreate(
      create: Node.Create
  ): Node.Create = {
    val maintainers = create.keyOpt.fold(Set.empty[Party])(_.maintainers)
    val signatories0 = create.signatories ++ maintainers
    val signatories =
      if (signatories0.isEmpty) Set(Party.assertFromString("alice")) else signatories0
    val stakeholders = signatories ++ create.stakeholders
    create.copy(
      signatories = signatories0,
      stakeholders = stakeholders,
      arg = normalize(create.arg, create.version),
      keyOpt = create.keyOpt.map(normalizeKey(_, create.version)),
    )
  }

  private def normalizeKey(
      key: GlobalKeyWithMaintainers,
      version: SerializationVersion,
  ) =
    key.copy(globalKey =
      GlobalKey(
        key.globalKey.templateId,
        key.globalKey.packageName,
        normalize(key.value, version),
        key.globalKey.hash,
      )
    )

  private def normalize(
      value0: Value,
      version: SerializationVersion,
  ): Value = Util.assertNormalizeValue(value0, version)

  private def addUnknownField(
      builder: Message.Builder,
      i: Int,
      content: ByteString,
  ): Message = {
    require(!content.isEmpty)
    def norm(i: Int) = (i % 536870911).abs + 1 // valid proto field index are 1 to 536870911
    val knownFieldIndex = builder.getDescriptorForType.getFields.asScala.map(_.getNumber).toSet
    val j = Iterator.iterate(norm(i))(i => norm(i + 1)).filterNot(knownFieldIndex).next()
    val field = protobuf.UnknownFieldSet.Field.newBuilder().addLengthDelimited(content).build()
    val extraFields = protobuf.UnknownFieldSet.newBuilder().addField(j, field).build()
    builder.setUnknownFields(extraFields).build()
  }

}
