// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.backend.ContractStorageBackend.{
  KeysPageQuery,
  KeysPageResult,
}
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.daml.lf.value.Value.{ValueText, ValueUnit}
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsContracts
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues.*

  behavior of "StorageBackend (contracts)"

  it should "correctly find key states using non-unique key lookup with limit 1" in {
    val key1 = GlobalKey.assertBuild(
      Identifier.assertFromString("A:B:C"),
      someTemplateId.pkg.name,
      ValueUnit,
      crypto.Hash.hashPrivateKey("dummy-key-hash-1"),
    )
    val key2 = GlobalKey.assertBuild(
      Identifier.assertFromString("A:B:C"),
      someTemplateId.pkg.name,
      ValueText("value"),
      crypto.Hash.hashPrivateKey("dummy-key-hash-2"),
    )
    val internalContractId = 123L
    val internalContractId2 = 223L
    val internalContractId3 = 323L
    val internalContractId4 = 423L
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      dtosCreate(
        event_offset = 1L,
        event_sequential_id = 1L,
        internal_contract_id = internalContractId4,
        create_key_hash = Some(key2.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosCreate(
        event_offset = 2L,
        event_sequential_id = 2L,
        internal_contract_id = internalContractId,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosCreate(
        event_offset = 3L,
        event_sequential_id = 3L,
        internal_contract_id = internalContractId2,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosConsumingExercise(
        event_offset = 4L,
        event_sequential_id = 4L,
        internal_contract_id = Some(internalContractId2),
        deactivated_event_sequential_id = Some(3L),
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosCreate(
        event_offset = 5L,
        event_sequential_id = 5L,
        internal_contract_id = internalContractId3,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosConsumingExercise(
        event_offset = 6L,
        event_sequential_id = 6L,
        internal_contract_id = Some(internalContractId4),
        deactivated_event_sequential_id = Some(1L),
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(6), 6L)
    )

    def lookupKeyState(key: GlobalKey, validAt: Long): Option[Long] =
      executeSql(
        backend.contract.contractKey(
          KeysPageQuery(key = key, validAtEventSeqId = validAt, limit = 1, nextPageToken = None)
        )
      ).internalContractIds.headOption

    def lookupKeyStates(keys: List[GlobalKey], validAt: Long): Map[GlobalKey, Long] = {
      val queries = keys.map(key =>
        KeysPageQuery(key = key, validAtEventSeqId = validAt, limit = 1, nextPageToken = None)
      )
      val results = executeSql(
        backend.contract.contractKeysPlain(queries, validAt)
      )
      keys
        .zip(results)
        .flatMap { case (key, result) =>
          result.internalContractIds.headOption.map(key -> _)
        }
        .toMap
    }

    val keyStates2 = lookupKeyStates(List(key1, key2), 2L)
    val keyStateKey1_2 = lookupKeyState(key1, 2L)
    val keyStateKey2_2 = lookupKeyState(key2, 2L)
    val keyStates3 = lookupKeyStates(List(key1, key2), 3L)
    val keyStateKey1_3 = lookupKeyState(key1, 3L)
    val keyStateKey2_3 = lookupKeyState(key2, 3L)
    val keyStates4 = lookupKeyStates(List(key1, key2), 4L)
    val keyStateKey1_4 = lookupKeyState(key1, 4L)
    val keyStateKey2_4 = lookupKeyState(key2, 4L)
    val keyStates5 = lookupKeyStates(List(key1, key2), 5L)
    val keyStateKey1_5 = lookupKeyState(key1, 5L)
    val keyStateKey2_5 = lookupKeyState(key2, 5L)
    val keyStates6 = lookupKeyStates(List(key1, key2), 6L)
    val keyStateKey1_6 = lookupKeyState(key1, 6L)
    val keyStateKey2_6 = lookupKeyState(key2, 6L)

    keyStates2 shouldBe Map(
      key1 -> internalContractId,
      key2 -> internalContractId4,
    )
    keyStateKey1_2 shouldBe Some(internalContractId)
    keyStateKey2_2 shouldBe Some(internalContractId4)
    keyStates3 shouldBe Map(
      key1 -> internalContractId2,
      key2 -> internalContractId4,
    )
    keyStateKey1_3 shouldBe Some(internalContractId2)
    keyStateKey2_3 shouldBe Some(internalContractId4)
    keyStates4 shouldBe Map(
      key1 -> internalContractId,
      key2 -> internalContractId4,
    )
    keyStateKey1_4 shouldBe Some(internalContractId)
    keyStateKey2_4 shouldBe Some(internalContractId4)
    keyStates5 shouldBe Map(
      key1 -> internalContractId3,
      key2 -> internalContractId4,
    )
    keyStateKey1_5 shouldBe Some(internalContractId3)
    keyStateKey2_5 shouldBe Some(internalContractId4)
    keyStates6 shouldBe Map(
      key1 -> internalContractId3
    )
    keyStateKey1_6 shouldBe Some(internalContractId3)
    keyStateKey2_6 shouldBe None
  }

  it should "correctly find non unique contract key contracts" in {
    val key1 = GlobalKey.assertBuild(
      Identifier.assertFromString("A:B:C"),
      someTemplateId.pkg.name,
      ValueUnit,
      crypto.Hash.hashPrivateKey("1"),
    )
    val key2 = GlobalKey.assertBuild(
      Identifier.assertFromString("A:B:C"),
      someTemplateId.pkg.name,
      ValueText("value"),
      keyHash = crypto.Hash.hashPrivateKey("2"),
    )
    val internalContractId = 123L
    val internalContractId2 = 223L
    val internalContractId3 = 323L
    val internalContractId4 = 423L
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      dtosCreate(
        event_offset = 1L,
        event_sequential_id = 1L,
        internal_contract_id = internalContractId4,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosCreate(
        event_offset = 2L,
        event_sequential_id = 2L,
        internal_contract_id = internalContractId,
        create_key_hash = Some(key2.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosCreate(
        event_offset = 3L,
        event_sequential_id = 3L,
        internal_contract_id = internalContractId2,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosConsumingExercise(
        event_offset = 4L,
        event_sequential_id = 4L,
        internal_contract_id = Some(internalContractId2),
        stakeholders = Set(signatory),
        template_id = someTemplateId,
        deactivated_event_sequential_id = Some(3L),
      ),
      dtosCreate(
        event_offset = 5L,
        event_sequential_id = 5L,
        internal_contract_id = internalContractId3,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(5), 5L)
    )

    executeSql(
      backend.contract.contractKey(
        KeysPageQuery(
          key = key1,
          limit = 1,
          nextPageToken = None,
          validAtEventSeqId = 5,
        )
      )
    ) shouldBe KeysPageResult(
      internalContractIds = Vector(internalContractId3),
      nextPageToken = Some(2L),
    )

    executeSql(
      backend.contract.contractKey(
        KeysPageQuery(
          key = key1,
          limit = 1,
          nextPageToken = Some(5L),
          validAtEventSeqId = 5,
        )
      )
    ) shouldBe KeysPageResult(
      internalContractIds = Vector(internalContractId4),
      nextPageToken = None,
    )

    executeSql(
      backend.contract.contractKey(
        KeysPageQuery(
          key = key1,
          limit = 10,
          nextPageToken = None,
          validAtEventSeqId = 5,
        )
      )
    ) shouldBe KeysPageResult(
      internalContractIds = Vector(internalContractId3, internalContractId4),
      nextPageToken = None,
    )

    executeSql(
      backend.contract.contractKey(
        KeysPageQuery(
          key = key1,
          limit = 3,
          nextPageToken = None,
          validAtEventSeqId = 5,
        )
      )
    ) shouldBe KeysPageResult(
      internalContractIds = Vector(internalContractId3, internalContractId4),
      nextPageToken = None,
    )

    executeSql(
      backend.contract.contractKey(
        KeysPageQuery(
          key = key1,
          limit = 2,
          nextPageToken = None,
          validAtEventSeqId = 5,
        )
      )
    ) shouldBe KeysPageResult(
      internalContractIds = Vector(internalContractId3, internalContractId4),
      nextPageToken = None,
    )

    executeSql(
      backend.contract.contractKey(
        KeysPageQuery(
          key = key1,
          limit = 3,
          nextPageToken = None,
          validAtEventSeqId = 4,
        )
      )
    ) shouldBe KeysPageResult(
      internalContractIds = Vector(internalContractId4),
      nextPageToken = None,
    )

    executeSql(
      backend.contract.contractKey(
        KeysPageQuery(
          key = key1,
          limit = 3,
          nextPageToken = None,
          validAtEventSeqId = 3,
        )
      )
    ) shouldBe KeysPageResult(
      internalContractIds = Vector(internalContractId2, internalContractId4),
      nextPageToken = None,
    )

    executeSql(
      backend.contract.contractKey(
        KeysPageQuery(
          key = key2,
          limit = 3,
          nextPageToken = None,
          validAtEventSeqId = 5,
        )
      )
    ) shouldBe KeysPageResult(
      internalContractIds = Vector(internalContractId),
      nextPageToken = None,
    )
  }

  it should "correctly find non unique contract key contracts in batch via nonUniqueContractKeysPlain" in {
    val key1 = GlobalKey.assertBuild(
      Identifier.assertFromString("A:B:C"),
      someTemplateId.pkg.name,
      ValueUnit,
      crypto.Hash.hashPrivateKey("batch-1"),
    )
    val key2 = GlobalKey.assertBuild(
      Identifier.assertFromString("A:B:C"),
      someTemplateId.pkg.name,
      ValueText("value"),
      keyHash = crypto.Hash.hashPrivateKey("batch-2"),
    )
    val iid1 = 101L
    val iid2 = 102L
    val iid3 = 103L
    val iid4 = 104L
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      dtosCreate(
        event_offset = 1L,
        event_sequential_id = 1L,
        internal_contract_id = iid1,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosCreate(
        event_offset = 2L,
        event_sequential_id = 2L,
        internal_contract_id = iid2,
        create_key_hash = Some(key2.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosCreate(
        event_offset = 3L,
        event_sequential_id = 3L,
        internal_contract_id = iid3,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosConsumingExercise(
        event_offset = 4L,
        event_sequential_id = 4L,
        internal_contract_id = Some(iid3),
        stakeholders = Set(signatory),
        template_id = someTemplateId,
        deactivated_event_sequential_id = Some(3L),
      ),
      dtosCreate(
        event_offset = 5L,
        event_sequential_id = 5L,
        internal_contract_id = iid4,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(5), 5L)
    )

    // Batch query: multiple keys in a single call
    val batchResults = executeSql(
      backend.contract.contractKeysPlain(
        Seq(
          KeysPageQuery(key = key1, limit = 10, nextPageToken = None, validAtEventSeqId = 5),
          KeysPageQuery(key = key2, limit = 10, nextPageToken = None, validAtEventSeqId = 5),
        ),
        validAtEventSeqId = 5L,
      )
    )
    batchResults should have size 2
    batchResults(0) shouldBe KeysPageResult(
      internalContractIds = Vector(iid4, iid1),
      nextPageToken = None,
    )
    batchResults(1) shouldBe KeysPageResult(
      internalContractIds = Vector(iid2),
      nextPageToken = None,
    )

    val pagedResult = executeSql(
      backend.contract.contractKeysPlain(
        Seq(
          KeysPageQuery(key = key1, limit = 1, nextPageToken = None, validAtEventSeqId = 5)
        ),
        validAtEventSeqId = 5L,
      )
    )
    pagedResult.loneElement shouldBe KeysPageResult(
      internalContractIds = Vector(iid4),
      nextPageToken = Some(2L),
    )

    // Batch query with nextPageToken
    val nextPageResult = executeSql(
      backend.contract.contractKeysPlain(
        Seq(
          KeysPageQuery(key = key1, limit = 10, nextPageToken = Some(5L), validAtEventSeqId = 5)
        ),
        validAtEventSeqId = 5L,
      )
    )
    nextPageResult.loneElement shouldBe KeysPageResult(
      internalContractIds = Vector(iid1),
      nextPageToken = None,
    )

    // Batch query respects deactivation visibility (validAtEventSeqId = 3, before the archive)
    val beforeArchive = executeSql(
      backend.contract.contractKeysPlain(
        Seq(
          KeysPageQuery(key = key1, limit = 10, nextPageToken = None, validAtEventSeqId = 3)
        ),
        validAtEventSeqId = 3L,
      )
    )
    beforeArchive.loneElement shouldBe KeysPageResult(
      internalContractIds = Vector(iid3, iid1),
      nextPageToken = None,
    )

    // Batch query with mixed limits: limit=1 for key1, limit=2 for key2
    val mixedLimits = executeSql(
      backend.contract.contractKeysPlain(
        Seq(
          KeysPageQuery(key = key1, limit = 1, nextPageToken = None, validAtEventSeqId = 5),
          KeysPageQuery(key = key2, limit = 2, nextPageToken = None, validAtEventSeqId = 5),
        ),
        validAtEventSeqId = 5L,
      )
    )
    mixedLimits should have size 2
    // key1 has 2 active contracts (iid4, iid1), limit=1 returns first and a nextPageToken
    mixedLimits(0) shouldBe KeysPageResult(
      internalContractIds = Vector(iid4),
      nextPageToken = Some(2L),
    )
    // key2 has 1 active contract (iid2), limit=2 returns it with no next page
    mixedLimits(1) shouldBe KeysPageResult(
      internalContractIds = Vector(iid2),
      nextPageToken = None,
    )

    // Empty batch
    val emptyResult = executeSql(
      backend.contract.contractKeysPlain(
        Seq.empty,
        validAtEventSeqId = 5L,
      )
    )
    emptyResult shouldBe Seq.empty

    // Batch query with identical KeysPageQuery entries: order is preserved and results are duplicated
    val key1PageQuery =
      KeysPageQuery(key = key1, limit = 10, nextPageToken = None, validAtEventSeqId = 5)
    val key2PageQuery =
      KeysPageQuery(key = key2, limit = 10, nextPageToken = None, validAtEventSeqId = 5)

    val identicalQueries = executeSql(
      backend.contract.contractKeysPlain(
        Seq(
          key1PageQuery,
          key2PageQuery,
          key1PageQuery,
        ),
        validAtEventSeqId = 5L,
      )
    )
    identicalQueries should have size 3
    identicalQueries(0) shouldBe KeysPageResult(
      internalContractIds = Vector(iid4, iid1),
      nextPageToken = None,
    )
    identicalQueries(1) shouldBe KeysPageResult(
      internalContractIds = Vector(iid2),
      nextPageToken = None,
    )
    identicalQueries(2) shouldBe identicalQueries(0)
  }

  it should "correctly handle various create, assign, unassign, archive event sequences in batch" in {
    val keyHash = crypto.Hash.hashPrivateKey("mixed-events")
    val key = GlobalKey.assertBuild(
      Identifier.assertFromString("A:B:C"),
      someTemplateId.pkg.name,
      ValueUnit,
      keyHash,
    )
    val signatory = Ref.Party.assertFromString("signatory")
    val keyHashHex = Some(key.hash.toHexString)

    def c(iid: Long, seqId: Long): Seq[DbDto] =
      dtosCreate(
        event_offset = seqId,
        event_sequential_id = seqId,
        internal_contract_id = iid,
        create_key_hash = keyHashHex,
      )(stakeholders = Set(signatory), template_id = someTemplateId)

    def a(iid: Long, seqId: Long): Seq[DbDto] =
      dtosAssign(
        event_offset = seqId,
        event_sequential_id = seqId,
        internal_contract_id = iid,
        create_key_hash = keyHashHex,
      )(stakeholders = Set(signatory), template_id = someTemplateId)

    def u(iid: Long, seqId: Long, deactivates: Long): Seq[DbDto] =
      dtosUnassign(
        event_offset = seqId,
        event_sequential_id = seqId,
        internal_contract_id = Some(iid),
        deactivated_event_sequential_id = Some(deactivates),
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      )

    def d(iid: Long, seqId: Long, deactivates: Long): Seq[DbDto] =
      dtosConsumingExercise(
        event_offset = seqId,
        event_sequential_id = seqId,
        internal_contract_id = Some(iid),
        deactivated_event_sequential_id = Some(deactivates),
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      )

    def queryAt(validAt: Long): KeysPageResult =
      executeSql(
        backend.contract.contractKeysPlain(
          Seq(
            KeysPageQuery(key = key, limit = 100, nextPageToken = None, validAtEventSeqId = validAt)
          ),
          validAtEventSeqId = validAt,
        )
      ).loneElement

    // Two contracts per sequence pattern, all sharing the same key.
    val iid00 = 1000L
    val iid01 = 1001L
    val iid10 = 1010L
    val iid11 = 1011L
    val iid20 = 1020L
    val iid21 = 1021L
    val iid30 = 1030L
    val iid31 = 1031L
    val iid40 = 1040L
    val iid41 = 1041L
    val iid50 = 1050L
    val iid51 = 1051L

    val dtos: Vector[DbDto] = Vector(
      // create(iid00,1), unassign(iid00,2), assign(iid01,3), archive(iid01,4)
      c(iid00, 1),
      u(iid00, 2, deactivates = 1),
      a(iid01, 3),
      d(iid01, 4, deactivates = 3),
      // create(iid10,5), assign(iid11,6), unassign(iid11,7), archive(iid10,8)
      c(iid10, 5),
      a(iid11, 6),
      u(iid11, 7, deactivates = 6),
      d(iid10, 8, deactivates = 5),
      // create(iid20,9), assign(iid21,10), archive(iid20,11), unassign(iid21,12)
      c(iid20, 9),
      a(iid21, 10),
      d(iid20, 11, deactivates = 9),
      u(iid21, 12, deactivates = 10),
      // assign(iid30,13), archive(iid30,14), create(iid31,15), unassign(iid31,16)
      a(iid30, 13),
      d(iid30, 14, deactivates = 13),
      c(iid31, 15),
      u(iid31, 16, deactivates = 15),
      // assign(iid40,17), create(iid41,18), archive(iid41,19), unassign(iid40,20)
      a(iid40, 17),
      c(iid41, 18),
      d(iid41, 19, deactivates = 18),
      u(iid40, 20, deactivates = 17),
      // assign(iid50,21), create(iid51,22), unassign(iid50,23), archive(iid51,24)
      a(iid50, 21),
      c(iid51, 22),
      u(iid50, 23, deactivates = 21),
      d(iid51, 24, deactivates = 22),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(24L), 24L))

    // create(1), unassign(2), assign(3), archive(4)
    queryAt(1) shouldBe KeysPageResult(internalContractIds = Vector(iid00), nextPageToken = None)
    queryAt(2) shouldBe KeysPageResult(internalContractIds = Vector.empty, nextPageToken = None)
    queryAt(3) shouldBe KeysPageResult(internalContractIds = Vector(iid01), nextPageToken = None)
    queryAt(4) shouldBe KeysPageResult(internalContractIds = Vector.empty, nextPageToken = None)

    // create(5), assign(6), unassign(7) the second contract, archive(8)
    queryAt(5) shouldBe KeysPageResult(internalContractIds = Vector(iid10), nextPageToken = None)
    queryAt(6) shouldBe KeysPageResult(
      internalContractIds = Vector(iid11, iid10),
      nextPageToken = None,
    )
    queryAt(7) shouldBe KeysPageResult(internalContractIds = Vector(iid10), nextPageToken = None)
    queryAt(8) shouldBe KeysPageResult(internalContractIds = Vector.empty, nextPageToken = None)

    // create(9), assign(10), archive(11), unassign(12)
    queryAt(9) shouldBe KeysPageResult(internalContractIds = Vector(iid20), nextPageToken = None)
    queryAt(10) shouldBe KeysPageResult(
      internalContractIds = Vector(iid21, iid20),
      nextPageToken = None,
    )
    queryAt(11) shouldBe KeysPageResult(internalContractIds = Vector(iid21), nextPageToken = None)
    queryAt(12) shouldBe KeysPageResult(internalContractIds = Vector.empty, nextPageToken = None)

    // assign(13), archive(14), create(15), unassign(16)
    queryAt(13) shouldBe KeysPageResult(internalContractIds = Vector(iid30), nextPageToken = None)
    queryAt(14) shouldBe KeysPageResult(internalContractIds = Vector.empty, nextPageToken = None)
    queryAt(15) shouldBe KeysPageResult(internalContractIds = Vector(iid31), nextPageToken = None)
    queryAt(16) shouldBe KeysPageResult(internalContractIds = Vector.empty, nextPageToken = None)

    // assign(17), create(18), archive(19), unassign(20)
    queryAt(17) shouldBe KeysPageResult(internalContractIds = Vector(iid40), nextPageToken = None)
    queryAt(18) shouldBe KeysPageResult(
      internalContractIds = Vector(iid41, iid40),
      nextPageToken = None,
    )
    queryAt(19) shouldBe KeysPageResult(internalContractIds = Vector(iid40), nextPageToken = None)
    queryAt(20) shouldBe KeysPageResult(internalContractIds = Vector.empty, nextPageToken = None)

    // assign(21), create(22), unassign(23) , archive(24)
    queryAt(21) shouldBe KeysPageResult(internalContractIds = Vector(iid50), nextPageToken = None)
    queryAt(22) shouldBe KeysPageResult(
      internalContractIds = Vector(iid51, iid50),
      nextPageToken = None,
    )
    queryAt(23) shouldBe KeysPageResult(internalContractIds = Vector(iid51), nextPageToken = None)
    queryAt(24) shouldBe KeysPageResult(internalContractIds = Vector.empty, nextPageToken = None)
  }

  it should "correctly find active contracts" in {
    val internalContractId = 123L
    val internalContractId2 = 223L
    val internalContractId3 = 323L
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      dtosCreate(
        event_offset = 1L,
        event_sequential_id = 1L,
        internal_contract_id = internalContractId,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosAssign(
        event_offset = 2L,
        event_sequential_id = 2L,
        internal_contract_id = internalContractId2,
        synchronizer_id = someSynchronizerId2,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosAssign(
        event_offset = 3L,
        event_sequential_id = 3L,
        internal_contract_id = internalContractId3,
        synchronizer_id = someSynchronizerId2,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosAssign(
        event_offset = 4L,
        event_sequential_id = 4L,
        internal_contract_id = internalContractId3,
        synchronizer_id = someSynchronizerId,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(3), 3L)
    )
    val activeContracts2 = executeSql(
      backend.contract.activeContracts(
        List(
          internalContractId,
          internalContractId2,
          internalContractId3,
        ),
        2L,
      )
    )
    val activeContracts3 = executeSql(
      backend.contract.activeContracts(
        List(
          internalContractId,
          internalContractId2,
          internalContractId3,
        ),
        3L,
      )
    )
    val activeIds = executeSql(
      backend.event.updateStreamingQueries
        .fetchActiveIds(
          stakeholderO = Some(signatory),
          templateIdO = None,
          activeAtEventSeqId = 1000,
        )
        .fetchPage(_)(
          PaginatingAsyncStream.PaginationFromTo.ascending(
            startExclusive = 0L,
            endInclusive = 1000L,
          )
        )
    )
    val lastActivations = executeSql(
      backend.contract.lastActivations(
        List(
          someSynchronizerId -> internalContractId,
          someSynchronizerId -> internalContractId3,
          someSynchronizerId2 -> internalContractId2,
          someSynchronizerId2 -> internalContractId3,
        )
      )
    )

    activeContracts2 shouldBe Map(
      internalContractId -> true,
      internalContractId2 -> true,
    )
    activeContracts3 shouldBe Map(
      internalContractId -> true,
      internalContractId2 -> true,
      internalContractId3 -> true,
    )
    activeIds shouldBe Vector(1L, 2L, 3L, 4L)
    lastActivations shouldBe Map(
      (someSynchronizerId, internalContractId) -> 1L,
      (someSynchronizerId2, internalContractId2) -> 2L,
      (someSynchronizerId2, internalContractId3) -> 3L,
    )
  }

  it should "correctly find deactivated contracts" in {
    val internalContractId = 123L
    val internalContractId2 = 223L
    val internalContractId3 = 323L
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      dtosCreate(
        event_offset = 1L,
        event_sequential_id = 1L,
        internal_contract_id = internalContractId,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosAssign(
        event_offset = 2L,
        event_sequential_id = 2L,
        internal_contract_id = internalContractId2,
        synchronizer_id = someSynchronizerId2,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosUnassign(
        event_offset = 3L,
        event_sequential_id = 3L,
        internal_contract_id = Some(internalContractId),
        deactivated_event_sequential_id = Some(1L),
        synchronizer_id = someSynchronizerId,
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosConsumingExercise(
        event_offset = 4L,
        event_sequential_id = 4L,
        internal_contract_id = Some(internalContractId2),
        deactivated_event_sequential_id = Some(2L),
        synchronizer_id = someSynchronizerId2,
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(4), 4L)
    )
    val activeContracts2 = executeSql(
      backend.contract.activeContracts(
        List(
          internalContractId,
          internalContractId2,
        ),
        2L,
      )
    )
    val activeContracts4 = executeSql(
      backend.contract.activeContracts(
        List(
          internalContractId,
          internalContractId2,
          internalContractId3,
        ),
        4L,
      )
    )
    val activeIds2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchActiveIds(
          stakeholderO = Some(signatory),
          templateIdO = None,
          activeAtEventSeqId = 2L,
        )
        .fetchPage(_)(
          PaginatingAsyncStream.PaginationFromTo.ascending(
            startExclusive = 0L,
            endInclusive = 2L,
          )
        )
    )
    val activeIds4 = executeSql(
      backend.event.updateStreamingQueries
        .fetchActiveIds(
          stakeholderO = Some(signatory),
          templateIdO = None,
          activeAtEventSeqId = 4,
        )
        .fetchPage(_)(
          PaginatingAsyncStream.PaginationFromTo.ascending(
            startExclusive = 0L,
            endInclusive = 4L,
          )
        )
    )
    val lastActivations = executeSql(
      backend.contract.lastActivations(
        List(
          someSynchronizerId -> internalContractId,
          someSynchronizerId2 -> internalContractId2,
        )
      )
    )

    activeContracts2 shouldBe Map(
      internalContractId -> true,
      internalContractId2 -> true,
    )
    activeContracts4 shouldBe Map(
      internalContractId -> true, // although deactivated, this logic only cares about archivals
      internalContractId2 -> false,
    )
    activeIds2 shouldBe Vector(1L, 2L)
    activeIds4 shouldBe Vector.empty
    // lastActivation does not care about deactivations
    lastActivations shouldBe Map(
      (someSynchronizerId, internalContractId) -> 1L,
      (someSynchronizerId2, internalContractId2) -> 2L,
    )
  }

  it should "be able to query with 1000 contract ids" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(
      updateLedgerEnd(offset(3), 6L)
    )
    val activeContracts = executeSql(
      backend.contract.activeContracts(
        1.to(1000).map(_.toLong),
        2,
      )
    )
    activeContracts shouldBe empty
  }
}
