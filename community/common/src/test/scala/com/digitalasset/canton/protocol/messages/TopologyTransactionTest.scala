// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{
  Fingerprint,
  Signature,
  SigningKeyUsage,
  SigningKeysWithThreshold,
  SigningPublicKey,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{TestSynchronizerParameters, TopologyLimits, v30}
import com.digitalasset.canton.serialization.HasCryptographicEvidenceTest
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
  CanSignSpecificMappings,
}
import com.digitalasset.canton.topology.transaction.KeyMapping.MaxKeys
import com.digitalasset.canton.topology.transaction.SynchronizerTrustCertificate.ParticipantTopologyFeatureFlag
import com.digitalasset.canton.topology.transaction.SynchronizerTrustCertificate.ParticipantTopologyFeatureFlag.EnableMultiSynchronizer
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.validation.ProtoUnvalidatedSeq
import com.digitalasset.canton.version.v1.UntypedVersionedMessage
import com.digitalasset.canton.version.{ProtoVersion, ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  LfPackageId,
  ProtoDeserializationError,
}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

final class TopologyTransactionTest
    extends AnyWordSpec
    with BaseTest
    with HasCryptographicEvidenceTest
    with FailOnShutdown
    with HasExecutionContext {

  private val uid = DefaultTestIdentities.uid
  private val uid2 = UniqueIdentifier.tryFromProtoPrimitive("da1::default1")
  private val sequencerId = DefaultTestIdentities.daSequencerId
  private val synchronizerId = DefaultTestIdentities.physicalSynchronizerId
  private val crypto =
    TestingTopology(sequencerGroup =
      SequencerGroup(
        active = NonEmpty(Seq, SequencerId(synchronizerId.uid)),
        passive = Seq.empty,
        threshold = PositiveInt.one,
      )
    ).build(loggerFactory).forOwnerAndSynchronizer(sequencerId, synchronizerId)
  // TODO(#25072): Create keys with a single usage and change the tests accordingly
  private val publicKey =
    crypto.crypto.privateCrypto
      .generateSigningKey(usage = SigningKeyUsage.All)
      .valueOrFail("create public key")
      .futureValueUS
  private val defaultDynamicSynchronizerParameters = TestSynchronizerParameters.defaultDynamic

  private def mk[T <: TopologyMapping](
      mapping: T
  ): TopologyTransaction[TopologyChangeOp.Replace, T] =
    TopologyTransaction.tryCreate(
      TopologyChangeOp.Replace,
      PositiveInt.one,
      mapping,
      testedProtocolVersion,
    )

  private val deserialize: ByteString => TopologyTransaction[TopologyChangeOp, TopologyMapping] =
    bytes =>
      TopologyTransaction.fromByteString(
        testedProtocolVersionValidation,
        testedTopologyTransactionContext,
        bytes,
      ) match {
        case Left(err) => throw new TestFailedException(err.toString, 0)
        case Right(msg) => msg
      }

  private def runTest(
      t1: TopologyTransaction[TopologyChangeOp, TopologyMapping],
      t2: TopologyTransaction[TopologyChangeOp, TopologyMapping],
  ): Unit = {
    behave like hasCryptographicEvidenceSerialization(t1, t2)
    behave like hasCryptographicEvidenceDeserialization(t1, t1.getCryptographicEvidence)(
      deserialize
    )
  }

  "synchronizer topology transactions" when {

    "namespace mappings" should {
      val nsd =
        mk(NamespaceDelegation.tryCreate(uid.namespace, publicKey, CanSignAllMappings))
      val nsd2 =
        mk(
          NamespaceDelegation.tryCreate(
            uid2.namespace,
            publicKey,
            CanSignAllButNamespaceDelegations,
          )
        )

      runTest(nsd, nsd2)
    }

    def mkProtoTransaction(nsd: v30.NamespaceDelegation) = v30.TopologyTransaction(
      operation = v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE,
      serial = 1,
      mapping = Some(
        v30.TopologyMapping(v30.TopologyMapping.Mapping.NamespaceDelegation(nsd))
      ),
    )

    "read legacy namespace delegations" in {
      // Test case: is_root_delegation=true, restriction=empty <=> CanSignAllMappings
      val rootDelegationProto = v30.NamespaceDelegation(
        uid.namespace.toProtoPrimitive,
        Some(publicKey.toProtoV30.valueOrFail("serializing public key")),
        isRootDelegation = true,
        restriction = v30.NamespaceDelegation.Restriction.Empty,
      )
      val rootFromScala = NamespaceDelegation
        .create(
          uid.namespace,
          publicKey,
          CanSignAllMappings,
        )
        .value
      // we don't need to check that inverse direction of serialization, because topology transactions are memoized,
      // therefore we only need to be able to serialize to the new format
      NamespaceDelegation
        .fromProtoV30(testedProtocolVersionValidation, rootDelegationProto)
        .value shouldBe rootFromScala

      val protoTx = mkProtoTransaction(rootDelegationProto)
      val scalaTxFromBytes = TopologyTransaction
        .fromTrustedByteString(testedTopologyTransactionContext)(
          UntypedVersionedMessage(
            UntypedVersionedMessage.Wrapper.Data(protoTx.toByteString),
            1,
          ).toByteString
        )
        .value
      scalaTxFromBytes.toByteString

      // Test case: is_root_delegation=false, restriction=empty <=> CanSignAllButNamespaceDelegations
      val nonRootDelegationProto = v30.NamespaceDelegation(
        uid.namespace.toProtoPrimitive,
        Some(publicKey.toProtoV30.valueOrFail("serializing public key")),
        isRootDelegation = false,
        restriction = v30.NamespaceDelegation.Restriction.Empty,
      )
      val nonRootFromScala = NamespaceDelegation
        .create(
          uid.namespace,
          publicKey,
          CanSignAllButNamespaceDelegations,
        )
        .value
      // we don't need to check that inverse direction of serialization, because topology transactions are memoized,
      // therefore we only need to be able to serialize to the new format
      NamespaceDelegation
        .fromProtoV30(testedProtocolVersionValidation, nonRootDelegationProto)
        .value shouldBe nonRootFromScala

      // Test case: is_root_delegation=false, restriction=non-empty <=> CanSignSpecificMappings
      Seq(
        v30.NamespaceDelegation.Restriction
          .CanSignAllMappings(v30.NamespaceDelegation.CanSignAllMappings()) -> CanSignAllMappings,
        v30.NamespaceDelegation.Restriction.CanSignAllButNamespaceDelegations(
          v30.NamespaceDelegation.CanSignAllButNamespaceDelegations()
        ) -> CanSignAllButNamespaceDelegations,
        v30.NamespaceDelegation.Restriction.CanSignSpecificMapings(
          v30.NamespaceDelegation.CanSignSpecificMappings(
            // all but UNSPECIFIED
            (v30.Enums.TopologyMappingCode.values.toSet - v30.Enums.TopologyMappingCode.TOPOLOGY_MAPPING_CODE_UNSPECIFIED).toSeq
              .sortBy(_.value)
          )
        ) -> CanSignSpecificMappings(NonEmpty.from(TopologyMapping.Code.all).value.toSet),
      ).foreach { case (protoRestriction, scalaRestriction) =>
        val restrictedDelegationProto = v30.NamespaceDelegation(
          uid.namespace.toProtoPrimitive,
          Some(publicKey.toProtoV30.valueOrFail("serializing public key")),
          isRootDelegation = false,
          restriction = protoRestriction,
        )
        val restrictedFromScala = NamespaceDelegation
          .create(
            uid.namespace,
            publicKey,
            scalaRestriction,
          )
          .value
        NamespaceDelegation
          .fromProtoV30(testedProtocolVersionValidation, restrictedDelegationProto)
          .value shouldBe restrictedFromScala
        restrictedFromScala.toProtoNamespaceDelegationV30.valueOrFail(
          "serializing transaction"
        ) shouldBe restrictedDelegationProto
      }
    }

    "unknown proto versions" should {
      val tx = mk(NamespaceDelegation.tryCreate(uid.namespace, publicKey, CanSignAllMappings))
      val txP = tx.toProtoV30.valueOrFail("serializing transaction")

      def untypedVersionedMessage(data: ByteString, protoVersion: Int): ByteString =
        UntypedVersionedMessage(
          UntypedVersionedMessage.Wrapper.Data(data),
          protoVersion,
        ).toByteString

      def noDeserializerError(protoVersion: Int, name: String): ProtoDeserializationError =
        OtherError(
          s"Unable to find deserializer for version ${ProtoVersion(protoVersion)} and message $name. This indicates that the node is trying to parse a message with a proto version that is not supported anymore."
        )

      // There is a topology transaction with proto version 29 on MainNet
      s"accept proto version 29 and known versions" in {
        val knownVersions = TopologyTransaction.versioningTable.converters.map {
          case (protoVersion, converter) => (protoVersion.v, converter.fromInclusive.representative)
        }.toList
        val testedVersions = (29, ProtocolVersion.stable.min1) +: knownVersions

        testedVersions.foreach { case (protoVersion, protocolVersion) =>
          clue(s"proto version $protoVersion") {
            val bytes = untypedVersionedMessage(txP.toByteString, protoVersion)
            val deserialized = TopologyTransaction
              .fromByteString(
                ProtocolVersionValidation(protocolVersion),
                testedTopologyTransactionContext,
                bytes,
              )
              .value

            deserialized shouldBe tx

            val expectedProtoVersion = if (protoVersion == 29) 30 else protoVersion

            // Legacy proto versions are read with the lowest supported proto version
            deserialized.representativeProtocolVersion shouldBe
              TopologyTransaction
                .protocolVersionRepresentativeFor(ProtoVersion(expectedProtoVersion))
                .value

            // The original proto version is kept
            deserialized.getCryptographicEvidence shouldBe bytes
          }
        }
      }

      s"reject proto with unknown versions" in {
        val lastKnown = TopologyTransaction.versioningTable.converters.keys.max.v

        Seq(lastKnown + 1, 999).foreach { protoVersion =>
          clue(s"proto version $protoVersion") {
            TopologyTransaction
              .fromByteString(
                ProtocolVersionValidation(testedProtocolVersion),
                testedTopologyTransactionContext,
                untypedVersionedMessage(txP.toByteString, protoVersion),
              )
              .left
              .value shouldBe noDeserializerError(protoVersion, TopologyTransaction.name)
          }
        }
      }

      "reject proto version 29 for other messages" in {
        val signedTx = SignedTopologyTransaction.withSignatures(
          tx,
          NonEmpty.mk(Seq, Signature.noSignature),
          isProposal = false,
          testedProtocolVersion,
        )
        val signedTxP = signedTx.toProtoV30.toByteString

        clue("sanity check: proto version 30 is accepted") {
          SignedTopologyTransaction
            .fromTrustedByteString(testedTopologyTransactionContext)(
              untypedVersionedMessage(signedTxP, 30)
            )
            .value shouldBe signedTx
        }

        SignedTopologyTransaction
          .fromTrustedByteString(testedTopologyTransactionContext)(
            untypedVersionedMessage(signedTxP, 29)
          )
          .left
          .value shouldBe noDeserializerError(29, SignedTopologyTransaction.name)
      }
    }

    "key to owner mappings" should {
      val k1 = mk(OwnerToKeyMapping.tryCreate(sequencerId, NonEmpty(Seq, publicKey)))
      val k2 = mk(OwnerToKeyMapping.tryCreate(sequencerId, NonEmpty(Seq, publicKey)))
      runTest(k1, k2)
    }

    // The newest supported version on which the locally defined bounds do not apply yet
    lazy val pvBeforeValidationBound: ProtocolVersion =
      ProtocolVersion.supported.forgetNE.filter(_ < ProtocolVersion.newBoundsOnPV37).max

    "synchronizer trust certificate feature flags bounded" should {

      val max = SynchronizerTrustCertificate.MaxFeatureFlags.unwrap

      def certificateP(flagCount: Int): v30.SynchronizerTrustCertificate =
        v30.SynchronizerTrustCertificate(
          participantUid = uid.toProtoPrimitive.toProtoUnvalidated,
          synchronizerId = SynchronizerId(uid).toProtoPrimitive.toProtoUnvalidated,
          featureFlags = ProtoUnvalidatedSeq(
            Seq.fill(flagCount)(
              v30.Enums.ParticipantFeatureFlag.PARTICIPANT_FEATURE_FLAG_ENABLE_MULTI_SYNCHRONIZER
            )
          ),
        )

      "leave room above the known feature flags" in {
        SynchronizerTrustCertificate.ParticipantTopologyFeatureFlag.all.size should be < max
      }

      "accept a certificate at the limit" in {
        SynchronizerTrustCertificate
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, certificateP(max))
          .value
          .featureFlags shouldBe Set(EnableMultiSynchronizer)
      }

      "reject a certificate above the limit, naming the field" in {
        SynchronizerTrustCertificate
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, certificateP(max + 1))
          .left
          .value shouldBe ProtoDeserializationError.InvariantViolation(
          "feature_flags",
          s"repeated field has ${max + 1} elements, exceeding the maximum of $max",
        )
      }

      "apply the limit from ProtocolVersion.newBoundsOnPV37" in {
        SynchronizerTrustCertificate
          .fromProtoV30(
            ProtocolVersionValidation(ProtocolVersion.newBoundsOnPV37),
            certificateP(max + 1),
          )
          .left
          .value shouldBe a[ProtoDeserializationError.InvariantViolation]
      }

      "not apply the limit before ProtocolVersion.newBoundsOnPV37" in {
        SynchronizerTrustCertificate
          .fromProtoV30(ProtocolVersionValidation(pvBeforeValidationBound), certificateP(max + 1))
          .value
          .featureFlags shouldBe Set(EnableMultiSynchronizer)
      }

      "deduplicate the feature flags" in {
        SynchronizerTrustCertificate
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, certificateP(3))
          .value
          .featureFlags shouldBe Set(EnableMultiSynchronizer)
      }

      "serialize the feature flags independently of their order" in {
        val flags = ParticipantTopologyFeatureFlag.all
        def certificate(ordered: Seq[ParticipantTopologyFeatureFlag]) =
          SynchronizerTrustCertificate(
            ParticipantId(uid),
            SynchronizerId(uid),
            ordered.toSet,
          ).toProto

        certificate(flags) shouldBe certificate(flags.reverse)
        certificate(flags).featureFlags shouldBe ProtoUnvalidatedSeq(
          flags.sortBy(_.value).map(_.toProtoV30)
        )
      }
    }

    "delegation restriction mappings bounded" should {

      val max = DelegationRestriction.MaxMappings.unwrap

      // Repeats NamespaceDelegation, because more mappings than known codes need duplicates
      def restrictionP(mappingCount: Int): v30.NamespaceDelegation.Restriction =
        v30.NamespaceDelegation.Restriction.CanSignSpecificMapings(
          v30.NamespaceDelegation.CanSignSpecificMappings(
            ProtoUnvalidatedSeq(
              Seq.fill(mappingCount)(TopologyMapping.Code.NamespaceDelegation.toProtoV30)
            )
          )
        )

      "leave room above the known mapping codes" in {
        TopologyMapping.Code.all.size should be < max
      }

      "accept a restriction at the limit" in {
        DelegationRestriction
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, restrictionP(max))
          .value shouldBe Some(CanSignSpecificMappings(TopologyMapping.Code.NamespaceDelegation))
      }

      "reject a restriction above the limit, naming the field" in {
        DelegationRestriction
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, restrictionP(max + 1))
          .left
          .value shouldBe ProtoDeserializationError.InvariantViolation(
          "mappings",
          s"repeated field has ${max + 1} elements, exceeding the maximum of $max",
        )
      }

      "apply the limit from ProtocolVersion.newBoundsOnPV37" in {
        DelegationRestriction
          .fromProtoV30(
            ProtocolVersionValidation(ProtocolVersion.newBoundsOnPV37),
            restrictionP(max + 1),
          )
          .left
          .value shouldBe a[ProtoDeserializationError.InvariantViolation]
      }

      "not apply the limit before ProtocolVersion.newBoundsOnPV37" in {
        DelegationRestriction
          .fromProtoV30(ProtocolVersionValidation(pvBeforeValidationBound), restrictionP(max + 1))
          .value shouldBe Some(CanSignSpecificMappings(TopologyMapping.Code.NamespaceDelegation))
      }
    }

    "decentralized namespace owners bounded by the synchronizer's topology limits" should {

      val limits =
        TopologyLimits.default.copy(maxOwnersPerDecentralizedNamespace = PositiveInt.three)
      val max = limits.maxOwnersPerDecentralizedNamespace.unwrap

      def definitionP(ownerCount: Int): v30.DecentralizedNamespaceDefinition =
        v30.DecentralizedNamespaceDefinition(
          decentralizedNamespace = "decentralized".toProtoUnvalidated,
          threshold = PositiveInt.one.unwrap,
          owners = ProtoUnvalidatedSeq((1 to ownerCount).map(i => s"owner$i")),
        )

      "accept a definition at the limit" in {
        DecentralizedNamespaceDefinition
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, limits, definitionP(max))
          .value
          .owners
          .forgetNE should have size max.toLong
      }

      "reject a definition above it, naming the field" in {
        DecentralizedNamespaceDefinition
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, limits, definitionP(max + 1))
          .left
          .value shouldBe ProtoDeserializationError.InvariantViolation(
          "owners",
          s"repeated field has ${max + 1} elements, exceeding the maximum of $max",
        )
      }

      "apply the limit from ProtocolVersion.newBoundsOnPV37" in {
        DecentralizedNamespaceDefinition
          .fromProtoV30(
            ProtocolVersionValidation(ProtocolVersion.newBoundsOnPV37),
            limits,
            definitionP(max + 1),
          )
          .left
          .value shouldBe a[ProtoDeserializationError.InvariantViolation]
      }

      "not apply the limit before ProtocolVersion.newBoundsOnPV37" in {
        DecentralizedNamespaceDefinition
          .fromProtoV30(
            ProtocolVersionValidation(pvBeforeValidationBound),
            limits,
            definitionP(max + 1),
          )
          .value
          .owners
          .forgetNE should have size (max + 1).toLong
      }

      "serialize the owners independently of their order" in {
        // At most four owners, so that the set iterates in insertion order
        val owners = (1 to 4).map(i => Namespace(Fingerprint.tryFromString(s"owner$i")))
        def definition(ordered: Seq[Namespace]) =
          DecentralizedNamespaceDefinition
            .tryCreate(
              Namespace(Fingerprint.tryFromString("decentralized")),
              PositiveInt.one,
              NonEmpty.from(ordered.toSet).value,
            )
            .toProto

        definition(owners) shouldBe definition(owners.reverse)
        definition(owners.reverse).owners shouldBe ProtoUnvalidatedSeq(
          owners.map(_.toProtoPrimitive.toProtoUnvalidated)
        )
      }
    }

    "key mappings bounded by the synchronizer's topology limits" should {

      // Distinct keys, because OwnerToKeyMapping rejects duplicates
      def signingKeys(count: Int): Seq[SigningPublicKey] =
        Seq.fill(count)(
          crypto.crypto.privateCrypto
            .generateSigningKey(usage = SigningKeyUsage.All)
            .valueOrFail("create signing key")
            .futureValueUS
        )

      val limits = TopologyLimits.default.copy(maxSigningKeysPerEntity = PositiveInt.three)

      def ownerToKeyP(keys: Seq[SigningPublicKey]): v30.OwnerToKeyMapping =
        v30.OwnerToKeyMapping(
          member = sequencerId.toProtoPrimitive.toProtoUnvalidated,
          publicKeys = ProtoUnvalidatedSeq(
            keys.map(_.toProtoPublicKeyV30.valueOrFail("serialize key"))
          ),
        )

      @nowarn("cat=deprecation")
      def partyToKeyP(keys: Seq[SigningPublicKey]): v30.PartyToKeyMapping =
        v30.PartyToKeyMapping(
          party = PartyId(uid).toProtoPrimitive.toProtoUnvalidated,
          threshold = PositiveInt.one.unwrap,
          signingKeys = ProtoUnvalidatedSeq(
            keys.map(_.toProtoV30.valueOrFail("serialize key"))
          ),
        )

      "accept a mapping at the limit" in {
        val keys = signingKeys(limits.maxSigningKeysPerEntity.unwrap)

        OwnerToKeyMapping
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, limits, ownerToKeyP(keys))
          .value
          .keys
          .forgetNE should have size limits.maxSigningKeysPerEntity.unwrap.toLong

        PartyToKeyMapping
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, limits, partyToKeyP(keys))
          .value
          .signingKeys
          .forgetNE should have size limits.maxSigningKeysPerEntity.unwrap.toLong
      }

      "reject a mapping above it, naming the field" in {
        val keys = signingKeys(limits.maxSigningKeysPerEntity.unwrap + 1)
        val exceeded =
          s"repeated field has ${keys.size} elements, exceeding the maximum of ${limits.maxSigningKeysPerEntity.unwrap}"

        OwnerToKeyMapping
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, limits, ownerToKeyP(keys))
          .left
          .value shouldBe ProtoDeserializationError.InvariantViolation(
          "public_keys",
          exceeded,
        )

        PartyToKeyMapping
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, limits, partyToKeyP(keys))
          .left
          .value shouldBe ProtoDeserializationError.InvariantViolation(
          "signing_keys",
          exceeded,
        )
      }

      "bound the keys to MaxKeys from PV35 until ProtocolVersion.newBoundsOnPV37" in {
        val keys = signingKeys(MaxKeys.unwrap + 1)
        val higher =
          TopologyLimits.default.copy(maxSigningKeysPerEntity = PositiveInt.tryCreate(keys.size))
        val exceeded =
          s"repeated field has ${keys.size} elements, exceeding the maximum of $MaxKeys"

        forAll(Seq(ProtocolVersion.v35, pvBeforeValidationBound)) { pv =>
          val pvv = ProtocolVersionValidation(pv)

          OwnerToKeyMapping
            .fromProtoV30(pvv, higher, ownerToKeyP(keys))
            .left
            .value shouldBe ProtoDeserializationError.InvariantViolation(
            "public_keys",
            exceeded,
          )

          PartyToKeyMapping
            .fromProtoV30(pvv, higher, partyToKeyP(keys))
            .left
            .value shouldBe ProtoDeserializationError.InvariantViolation(
            "signing_keys",
            exceeded,
          )
        }
      }

      "apply a synchronizer limit below MaxKeys from ProtocolVersion.newBoundsOnPV37" in {
        val keys = signingKeys(limits.maxSigningKeysPerEntity.unwrap + 1)
        val exceeded =
          s"repeated field has ${keys.size} elements, exceeding the maximum of ${limits.maxSigningKeysPerEntity.unwrap}"
        val nextBound = ProtocolVersionValidation(ProtocolVersion.newBoundsOnPV37)

        OwnerToKeyMapping
          .fromProtoV30(nextBound, limits, ownerToKeyP(keys))
          .left
          .value shouldBe ProtoDeserializationError.InvariantViolation(
          "public_keys",
          exceeded,
        )
        PartyToKeyMapping
          .fromProtoV30(nextBound, limits, partyToKeyP(keys))
          .left
          .value shouldBe ProtoDeserializationError.InvariantViolation(
          "signing_keys",
          exceeded,
        )

        val beforeBound = ProtocolVersionValidation(pvBeforeValidationBound)
        OwnerToKeyMapping
          .fromProtoV30(beforeBound, limits, ownerToKeyP(keys))
          .value
          .keys
          .forgetNE should have size keys.size.toLong
        PartyToKeyMapping
          .fromProtoV30(beforeBound, limits, partyToKeyP(keys))
          .value
          .signingKeys
          .forgetNE should have size keys.size.toLong
      }

      "take the limit from the caller, not from a constant" in {
        val keys = signingKeys(limits.maxSigningKeysPerEntity.unwrap + 1)
        val higher =
          TopologyLimits.default.copy(maxSigningKeysPerEntity = PositiveInt.tryCreate(keys.size))

        OwnerToKeyMapping
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, higher, ownerToKeyP(keys))
          .value
          .keys
          .forgetNE should have size keys.size.toLong
      }

      def partySigningKeysP(
          keys: Seq[SigningPublicKey]
      ): com.digitalasset.canton.crypto.v30.SigningKeysWithThreshold =
        com.digitalasset.canton.crypto.v30.SigningKeysWithThreshold(
          keys = ProtoUnvalidatedSeq(keys.map(_.toProtoV30.valueOrFail("serialize key"))),
          threshold = PositiveInt.one.unwrap,
        )

      "bound the party signing keys to MaxKeys before ProtocolVersion.newBoundsOnPV37" in {
        val keys = signingKeys(MaxKeys.unwrap + 1)
        val higher = PositiveInt.tryCreate(keys.size)

        forAll(Seq(ProtocolVersion.v35, pvBeforeValidationBound)) { pv =>
          SigningKeysWithThreshold
            .fromProtoV30(ProtocolVersionValidation(pv), higher, partySigningKeysP(keys))
            .left
            .value shouldBe ProtoDeserializationError.InvariantViolation(
            "keys",
            s"repeated field has ${keys.size} elements, exceeding the maximum of $MaxKeys",
          )
        }
      }

      "bound the party signing keys by the caller's limit from ProtocolVersion.newBoundsOnPV37" in {
        val keys = signingKeys(MaxKeys.unwrap + 1)
        val nextBound = ProtocolVersionValidation(ProtocolVersion.newBoundsOnPV37)

        SigningKeysWithThreshold
          .fromProtoV30(nextBound, PositiveInt.tryCreate(keys.size), partySigningKeysP(keys))
          .value
          .keys
          .forgetNE should have size keys.size.toLong

        SigningKeysWithThreshold
          .fromProtoV30(nextBound, limits.maxSigningKeysPerEntity, partySigningKeysP(keys))
          .left
          .value shouldBe a[ProtoDeserializationError.InvariantViolation]
      }
    }

    "hosting participants bounded by the synchronizer's topology limits" should {

      val limits = TopologyLimits.default.copy(maxHostingParticipantsPerParty = PositiveInt.three)
      val max = limits.maxHostingParticipantsPerParty.unwrap

      def mappingP(participantCount: Int): v30.PartyToParticipant =
        PartyToParticipant
          .tryCreate(
            PartyId(uid),
            PositiveInt.one,
            (1 to participantCount).map(i =>
              HostingParticipant(
                ParticipantId(UniqueIdentifier.tryCreate(s"participant$i", uid.namespace)),
                ParticipantPermission.Confirmation,
              )
            ),
          )
          .toProtoPartyToParticipantV30
          .valueOrFail("serialize party to participant")

      "accept a mapping at the limit" in {
        PartyToParticipant
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, limits, mappingP(max))
          .value
          .participants should have size max.toLong
      }

      "reject a mapping above it, naming the field" in {
        PartyToParticipant
          .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, limits, mappingP(max + 1))
          .left
          .value shouldBe ProtoDeserializationError.InvariantViolation(
          "participants",
          s"repeated field has ${max + 1} elements, exceeding the maximum of $max",
        )
      }

      "apply the limit from ProtocolVersion.newBoundsOnPV37" in {
        PartyToParticipant
          .fromProtoV30(
            ProtocolVersionValidation(ProtocolVersion.newBoundsOnPV37),
            limits,
            mappingP(max + 1),
          )
          .left
          .value shouldBe a[ProtoDeserializationError.InvariantViolation]
      }

      "not apply the limit before ProtocolVersion.newBoundsOnPV37" in {
        PartyToParticipant
          .fromProtoV30(
            ProtocolVersionValidation(pvBeforeValidationBound),
            limits,
            mappingP(max + 1),
          )
          .value
          .participants should have size (max + 1).toLong
      }
    }

    "party to participant" should {
      val p1 =
        mk(
          PartyToParticipant.tryCreate(
            PartyId(uid),
            PositiveInt.one,
            Seq(HostingParticipant(ParticipantId(uid2), ParticipantPermission.Observation)),
          )
        )

      val p2 =
        mk(
          PartyToParticipant.tryCreate(
            PartyId(uid),
            PositiveInt.two,
            Seq(
              HostingParticipant(ParticipantId(uid2), ParticipantPermission.Confirmation),
              HostingParticipant(ParticipantId(uid), ParticipantPermission.Submission),
            ),
          )
        )

      runTest(p1, p2)
    }

    "participant state" should {
      val ps1 = mk(
        ParticipantSynchronizerPermission(
          synchronizerId,
          ParticipantId(uid),
          ParticipantPermission.Submission,
          limits = None,
          loginAfter = None,
        )
      )
      val ps2 = mk(
        ParticipantSynchronizerPermission(
          synchronizerId,
          ParticipantId(uid),
          ParticipantPermission.Observation,
          limits = Some(ParticipantSynchronizerLimits(NonNegativeInt.tryCreate(13))),
          loginAfter = Some(CantonTimestamp.MinValue.plusSeconds(17)),
        )
      )

      runTest(ps1, ps2)

    }

    "synchronizer parameters change" should {
      val dmp1 =
        mk(SynchronizerParametersState(SynchronizerId(uid), defaultDynamicSynchronizerParameters))
      val dmp2 =
        mk(SynchronizerParametersState(SynchronizerId(uid), defaultDynamicSynchronizerParameters))
      runTest(dmp1, dmp2)
    }

  }

  "the protocol version to create a topology transaction for" should {
    import TopologyTransaction.protocolVersionToCreate

    "default to the store's protocol version" in {
      protocolVersionToCreate(ProtocolVersion.v37, None) shouldBe Right(ProtocolVersion.v37)
    }

    "accept protocol version 36 on a store of protocol version 37" in {
      protocolVersionToCreate(ProtocolVersion.v37, Some(ProtocolVersion.v36)) shouldBe Right(
        ProtocolVersion.v36
      )
    }

    "reject a higher protocol version than the store's" in {
      protocolVersionToCreate(ProtocolVersion.v36, Some(ProtocolVersion.v37)).left.value should
        include("Admissible protocol versions: 36")
      protocolVersionToCreate(ProtocolVersion.v37, Some(ProtocolVersion.dev)).left.value should
        include("Admissible protocol versions: 36, 37")
    }

    "accept a lower admissible protocol version on a store of the dev protocol version" in {
      protocolVersionToCreate(ProtocolVersion.dev, Some(ProtocolVersion.v36)) shouldBe Right(
        ProtocolVersion.v36
      )
    }
  }

  "authorized store topology transactions" when {
    "package vetting" should {
      "honor specified LET boundaries" in {
        val validFrom = CantonTimestamp.ofEpochSecond(20)
        val validUntil = CantonTimestamp.ofEpochSecond(30)
        val vp =
          VettedPackage(LfPackageId.assertFromString("pkg-id"), Some(validFrom), Some(validUntil))
        assert(!vp.validAt(validFrom.immediatePredecessor), "before valid-from invalid")
        // see https://github.com/DACH-NY/canton-network-node/issues/18259 regarding valid-from inclusivity:
        assert(vp.validAt(validFrom), "valid-from must be inclusive")
        assert(vp.validAt(validFrom.immediateSuccessor), "between must be valid")
        assert(!vp.validAt(validUntil), "valid-until must be exclusive")
      }

      "honor open ended LET boundaries" in {
        val validFrom = CantonTimestamp.ofEpochSecond(20)
        val untilForever =
          VettedPackage(LfPackageId.assertFromString("pkg-id"), Some(validFrom), None)
        assert(untilForever.validAt(CantonTimestamp.MaxValue), "valid until forever")

        val validUntil = CantonTimestamp.ofEpochSecond(20)
        val sinceForever =
          VettedPackage(LfPackageId.assertFromString("pkg-id"), None, Some(validUntil))
        assert(sinceForever.validAt(CantonTimestamp.MinValue), "valid since forever")
      }
    }
  }
}
