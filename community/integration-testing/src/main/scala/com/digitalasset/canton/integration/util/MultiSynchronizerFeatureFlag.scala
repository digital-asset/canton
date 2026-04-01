// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.digitalasset.canton.BaseTest.eventually
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.SynchronizerTrustCertificate.ParticipantTopologyFeatureFlag
import org.scalatest.LoneElement.*
import org.scalatest.matchers.should.Matchers.*

object MultiSynchronizerFeatureFlag {
  def enable(
      participants: Seq[ParticipantReference],
      synchronizerId: SynchronizerId,
  ): Unit =
    participants.foreach { p =>
      val currentFeatureFlags = p.topology.synchronizer_trust_certificates
        .list(
          store = Some(TopologyStoreId.Synchronizer(synchronizerId)),
          filterUid = p.uid.toProtoPrimitive,
        )
        .loneElement
        .item
        .featureFlags

      if (
        !currentFeatureFlags.contains(ParticipantTopologyFeatureFlag.EnableUnsafeMultiSynchronizer)
      ) {
        p.topology.synchronizer_trust_certificates.propose(
          p.id,
          synchronizerId,
          featureFlags =
            currentFeatureFlags :+ ParticipantTopologyFeatureFlag.EnableUnsafeMultiSynchronizer,
        )
      } else ()

      eventually() {
        p.topology.synchronizer_trust_certificates
          .list(
            store = Some(TopologyStoreId.Synchronizer(synchronizerId)),
            filterUid = p.uid.toProtoPrimitive,
          )
          .loneElement
          .item
          .featureFlags should contain(ParticipantTopologyFeatureFlag.EnableUnsafeMultiSynchronizer)
      }
    }

  def disable(
      participants: Seq[ParticipantReference],
      synchronizerId: SynchronizerId,
  ): Unit =
    participants.foreach { p =>
      val currentFeatureFlags = p.topology.synchronizer_trust_certificates
        .list(
          store = Some(TopologyStoreId.Synchronizer(synchronizerId)),
          filterUid = p.uid.toProtoPrimitive,
        )
        .loneElement
        .item
        .featureFlags

      if (
        currentFeatureFlags.contains(ParticipantTopologyFeatureFlag.EnableUnsafeMultiSynchronizer)
      )
        p.topology.synchronizer_trust_certificates.propose(
          p.id,
          synchronizerId,
          featureFlags = currentFeatureFlags.filterNot(
            _ == ParticipantTopologyFeatureFlag.EnableUnsafeMultiSynchronizer
          ),
        )
      else ()
      eventually() {
        p.topology.synchronizer_trust_certificates
          .list(
            store = Some(TopologyStoreId.Synchronizer(synchronizerId)),
            filterUid = p.uid.toProtoPrimitive,
          )
          .loneElement
          .item
          .featureFlags should not contain (ParticipantTopologyFeatureFlag.EnableUnsafeMultiSynchronizer)
      }
    }

}
