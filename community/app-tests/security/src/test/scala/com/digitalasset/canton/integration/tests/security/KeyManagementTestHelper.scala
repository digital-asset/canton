// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.concurrent.Eventually

import java.time.Instant
import scala.collection.Seq
import scala.util.matching.Regex

trait KeyManagementTestHelper extends Eventually {
  this: BaseTest =>

  protected def getCurrentKey(
      node: InstanceReference,
      purpose: KeyPurpose,
      usageO: Option[NonEmpty[Set[SigningKeyUsage]]],
      synchronizerId: SynchronizerId,
  ): PublicKey =
    // TODO(#23814): Add filter usage to owner_to_key_mappings
    node.topology.owner_to_key_mappings
      .list(
        store = synchronizerId,
        filterKeyOwnerUid = node.id.filterString,
      )
      .find(_.item.member == node.id)
      .getOrElse(sys.error(s"No keys found for $node"))
      .item
      .keys
      .find {
        case _: EncryptionPublicKey =>
          KeyPurpose.Encryption == purpose
        case SigningPublicKey(_, _, _, keyUsage, _) =>
          KeyPurpose.Signing == purpose && SigningKeyUsage.matchesRelevantUsages(
            keyUsage,
            usageO.valueOrFail("no usage"),
          )
        case _ => sys.error("Unknown key type")
      }
      .getOrElse(sys.error(s"No $purpose keys found for $node"))

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  def rotateAndTest(
      nodeInstance: InstanceReference,
      synchronizerId: SynchronizerId,
      ct: CantonTimestamp = new SimClock(loggerFactory = loggerFactory).now,
  ): Unit = {

    def allKeysInSynchronizerStore: Seq[Fingerprint] =
      nodeInstance.topology.owner_to_key_mappings
        .list(
          store = synchronizerId,
          filterKeyOwnerUid = nodeInstance.id.member.filterString,
          filterKeyOwnerType = Some(nodeInstance.id.member.code),
        )
        .loneElement
        .item
        .keys
        .forgetNE
        .map(_.fingerprint)

    val keysInternalStore = nodeInstance.keys.secret.list()
    val keyIdsInternalStore = keysInternalStore.map(_.publicKey.fingerprint)
    val keyIdsSynchronizerStore = allKeysInSynchronizerStore

    val keyToRotateName = keysInternalStore.collect {
      case keyMetadata if keyIdsSynchronizerStore.contains(keyMetadata.publicKey.fingerprint) =>
        keyMetadata.name
    }

    keyIdsInternalStore should contain allElementsOf keyIdsSynchronizerStore

    nodeInstance.keys.secret.rotate_node_keys(synchronizerId = synchronizerId)

    eventually() {
      val keysInternalStoreNew = nodeInstance.keys.secret.list()
      val keyIdsInternalStoreNew = keysInternalStoreNew.map(_.publicKey.fingerprint)
      val keyIdsSynchronizerStoreNew = allKeysInSynchronizerStore

      keyIdsInternalStoreNew should contain allElementsOf keyIdsSynchronizerStoreNew
      keyIdsSynchronizerStoreNew should not equal keyIdsSynchronizerStore
      keyIdsInternalStore.size should be < keyIdsInternalStoreNew.size

      // Test that rotated keys have a new tag -rotated-<timestamp>
      keyToRotateName.foreach { nameO =>
        val oldName = nameO.valueOrFail("old key has no name").unwrap
        keysInternalStoreNew.map(_.name).exists { keyName =>
          val newName = keyName.map(_.unwrap).getOrElse("")
          val pattern: Regex = ".*-rotated-(.*)".r
          newName match {
            case pattern(value) =>
              newName
                .contains(oldName) && CantonTimestamp.assertFromInstant(Instant.parse(value)) >= ct
            case _ => false
          }
        } shouldBe true
      }
    }
  }
}
