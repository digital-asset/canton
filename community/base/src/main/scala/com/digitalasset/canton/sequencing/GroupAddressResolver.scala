// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorGroup, Member}
import com.digitalasset.canton.tracing.TraceContext

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

object GroupAddressResolver {

  def resolveMediatorAndSequencerGroupRecipients(
      groupRecipients: Set[GroupRecipient],
      topologyOrSequencingSnapshot: TopologySnapshot,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[GroupRecipient, Set[Member]]] =
    if (groupRecipients.isEmpty) FutureUnlessShutdown.pure(Map.empty)
    else
      for {
        mediatorGroupByMember <- resolveMediatorGroupRecipients(
          groupRecipients,
          topologyOrSequencingSnapshot,
        )
        sequencersOfSynchronizer <- resolveSequencersOfSynchronizers(
          groupRecipients,
          topologyOrSequencingSnapshot,
        )
      } yield mediatorGroupByMember ++ Map(SequencersOfSynchronizer -> sequencersOfSynchronizer)

  private def resolveMediatorGroupRecipients(
      groupRecipients: Set[GroupRecipient],
      topologyOrSequencingSnapshot: TopologySnapshot,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[GroupRecipient, Set[Member]]] = {
    val mediatorGroups = groupRecipients.collect { case MediatorGroupRecipient(group) =>
      group
    }.toSeq
    if (mediatorGroups.isEmpty)
      FutureUnlessShutdown.pure(Map.empty)
    else
      for {
        groups <- topologyOrSequencingSnapshot
          .mediatorGroupsOfAll(mediatorGroups)
          .leftMap(_ => Seq.empty[MediatorGroup])
          .merge
      } yield asGroupRecipientsToMembers(groups)
  }

  def resolveSequencersOfSynchronizers(
      groupRecipients: Set[GroupRecipient],
      topologyOrSequencingSnapshot: TopologySnapshot,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Set[Member]] =
    if (groupRecipients.contains(SequencersOfSynchronizer))
      for {
        sequencers <-
          topologyOrSequencingSnapshot
            .sequencerGroup()
            .map(
              _.map(group => (group.active ++ group.passive).toSet[Member])
                .getOrElse(Set.empty[Member])
            )
      } yield sequencers
    else FutureUnlessShutdown.pure(Set.empty)

  @nowarn("cat=deprecation")
  def resolveGroupsToMembers(
      groupRecipients: Set[GroupRecipient],
      topologyOrSequencingSnapshot: TopologySnapshot,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[GroupRecipient, Set[Member]]] =
    if (groupRecipients.isEmpty) FutureUnlessShutdown.pure(Map.empty)
    else
      for {
        resolvedMediatorAndSequencerGroupRecipients <-
          resolveMediatorAndSequencerGroupRecipients(
            groupRecipients,
            topologyOrSequencingSnapshot,
          )
        allMembers <- {
          if (!groupRecipients.contains(AllMembersOfSynchronizer)) {
            FutureUnlessShutdown.pure(Map.empty)
          } else {

            topologyOrSequencingSnapshot
              .allMembers()
              .map(members => Map((AllMembersOfSynchronizer: GroupRecipient, members)))
          }
        }
      } yield resolvedMediatorAndSequencerGroupRecipients ++ allMembers

  def asGroupRecipientsToMembers(
      groups: Seq[MediatorGroup]
  ): Map[GroupRecipient, Set[Member]] =
    groups
      .map(group =>
        MediatorGroupRecipient(group.index) -> (group.active ++ group.passive)
          .toSet[Member]
      )
      .toMap
}
