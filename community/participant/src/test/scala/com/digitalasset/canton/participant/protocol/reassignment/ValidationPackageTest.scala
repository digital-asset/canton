// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.NoCommonPackage
import com.digitalasset.canton.protocol.{ContractInstance, ExampleContractFactory, LfContractId}
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.store.packagemeta.PackageMetadata.{
  LocalPackagePreference,
  PackageResolution,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Source
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfPackageId,
  LfPackageVersion,
  LfPartyId,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.nonempty.NonEmpty
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

@SuppressWarnings(Array("org.wartremover.warts.Return"))
class ValidationPackageTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with MockitoSugar {

  private val packageName: Ref.PackageName = Ref.PackageName.assertFromString("pkg-name")

  private val overridePkgId: LfPackageId = LfPackageId.assertFromString("override-pkg-id")

  private def pkgVersion(i: Int): (LfPackageId, LfPackageVersion) = (
    LfPackageId.assertFromString(s"pkg-id-v$i"),
    LfPackageVersion.assertFromString(s"$i.0.0"),
  )

  private val (pkgIdV1, v1) = pkgVersion(1)
  private val (pkgIdV2, v2) = pkgVersion(2)
  private val (pkgIdV3, v3) = pkgVersion(3)
  private val (pkgIdV4, v4) = pkgVersion(4)

  private val alice: LfPartyId = LfPartyId.assertFromString("alice::default")
  private val bob: LfPartyId = LfPartyId.assertFromString("bob::default")

  private val p1: ParticipantId = ParticipantId("participant1")
  private val p2: ParticipantId = ParticipantId("participant2")

  private val contract: ContractInstance = ExampleContractFactory.build(
    packageName = packageName,
    signatories = Set(alice),
    stakeholders = Set(alice, bob),
  )

  private val contractId: LfContractId = contract.contractId

  private val packageMetadata: PackageMetadata = PackageMetadata(
    packageNameMap = Map(
      packageName -> PackageResolution(
        preference = LocalPackagePreference(v1, pkgIdV1),
        allPackageIdsForName = NonEmpty(Set, pkgIdV1, pkgIdV2, pkgIdV3, pkgIdV4),
      )
    ),
    packageIdVersionMap = Map(
      pkgIdV1 -> (packageName, v1),
      pkgIdV2 -> (packageName, v2),
      pkgIdV3 -> (packageName, v3),
      pkgIdV4 -> (packageName, v4),
    ),
  )

  private val snapshotTime = CantonTimestamp.now()

  private def snapshotWith(vetted: Map[ParticipantId, Set[VettedPackage]]): TopologySnapshot = {
    val snapshot = mock[TopologySnapshot]
    when(snapshot.timestamp)
      .thenReturn(snapshotTime)
    when(snapshot.activeParticipantsOfParties(any[Seq[LfPartyId]])(any[TraceContext]))
      .thenReturn(FutureUnlessShutdown.pure(Map(alice -> Set(p1), bob -> Set(p2))))
    when(snapshot.vettedPackages(eqTo(Set(p1, p2)))(any[TraceContext]))
      .thenReturn(FutureUnlessShutdown.pure(vetted))
    snapshot
  }

  private def unbounded(ids: LfPackageId*): Set[VettedPackage] =
    VettedPackage.unbounded(ids).toSet

  private def choose(
      snapshot: TopologySnapshot,
      overrides: Map[LfContractId, LfPackageId] = Map.empty,
      metadata: PackageMetadata = packageMetadata,
      validatedContractPackageIds: Map[LfContractId, Set[LfPackageId]] = Map.empty,
  ): Either[ReassignmentProcessingSteps.ReassignmentProcessorError, LfPackageId] =
    ValidationPackage
      .choose(contract, Source(overrides), Source(snapshot), metadata, validatedContractPackageIds)
      .value
      .futureValueUS

  "ValidationPackage.choose" should {

    "return the overridden package id without consulting topology" in {
      val snapshot = mock[TopologySnapshot]
      choose(snapshot, overrides = Map(contractId -> overridePkgId)) shouldBe Right(overridePkgId)
      verifyZeroInteractions(snapshot)
    }

    "return the highest versioned package vetted by all participants" in {
      val snapshot = snapshotWith(
        Map(
          p1 -> unbounded(pkgIdV1, pkgIdV2, pkgIdV3),
          p2 -> unbounded(pkgIdV2, pkgIdV3, pkgIdV4),
        )
      )
      choose(snapshot) shouldBe Right(pkgIdV3)
    }

    "return the highest validated vetted package" in {
      val snapshot = snapshotWith(
        Map(
          p1 -> unbounded(pkgIdV1, pkgIdV2, pkgIdV3),
          p2 -> unbounded(pkgIdV1, pkgIdV2, pkgIdV3),
        )
      )
      choose(
        snapshot,
        validatedContractPackageIds = Map(contractId -> Set(pkgIdV1, pkgIdV2)),
      ) shouldBe Right(pkgIdV2)
    }

    "ignore packages that are not valid at the snapshot timestamp" in {
      val snapshot = snapshotWith(
        Map(
          p1 -> unbounded(pkgIdV2, pkgIdV3, pkgIdV4),
          p2 -> Set(
            VettedPackage(pkgIdV2, None, None),
            VettedPackage(pkgIdV3, Some(snapshotTime.plusSeconds(1)), None),
            VettedPackage(pkgIdV4, None, Some(snapshotTime.plusSeconds(-1))),
          ),
        )
      )

      choose(snapshot) shouldBe Right(pkgIdV2)
    }

    "fail when there is no commonly vetted package" in {
      val snapshot = snapshotWith(
        Map(
          p1 -> unbounded(pkgIdV1),
          p2 -> unbounded(pkgIdV2),
        )
      )

      choose(snapshot) shouldBe Left(NoCommonPackage(contractId, packageName, Set(p1, p2)))
    }

    "fail when the package name is unknown to the package metadata" in {
      val snapshot = snapshotWith(
        Map(
          p1 -> unbounded(pkgIdV1, pkgIdV2),
          p2 -> unbounded(pkgIdV1, pkgIdV2),
        )
      )

      choose(snapshot, metadata = PackageMetadata()) shouldBe
        Left(NoCommonPackage(contractId, packageName, Set(p1, p2)))
    }
  }
}
