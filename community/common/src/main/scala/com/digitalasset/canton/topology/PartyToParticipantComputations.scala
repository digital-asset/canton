// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.syntax.either.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.nonempty.NonEmpty

import scala.collection.immutable.SeqMap

class PartyToParticipantComputations(override protected val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

  /** Compute the new list of permissions from existing permissions and permissions that need to be
    * added and removed.
    *
    * If a participant in `adds` is already permissioned, the permissions are updated.
    */
  def computeNewPermissions(
      existingPermissions: SeqMap[ParticipantId, ParticipantPermission],
      adds: Seq[(ParticipantId, ParticipantPermission)] = Nil,
      removes: Seq[ParticipantId] = Nil,
  ): Either[String, SeqMap[ParticipantId, ParticipantPermission]] = {

    val conflictsO =
      NonEmpty.from(adds.map { case (participantId, _) => participantId }.intersect(removes))

    val conflictsCheck: Either[String, Unit] = conflictsO
      .toLeft(())
      .leftMap(conflicts =>
        s"Permissions for the following participant were found in adds and removes: $conflicts"
      )

    val unknownRemovesCheck = NonEmpty
      .from(removes.toSet.diff(existingPermissions.keySet))
      .toLeft(())
      .leftMap(unknowns =>
        s"Cannot remove permission for participants that are not permissioned: $unknowns"
      )

    for {
      _ <- conflictsCheck
      _ <- unknownRemovesCheck
    } yield existingPermissions.removedAll(removes) ++ adds

  }

  /** Computes the list of new participant hosting states, taking into account the existing states,
    * the newly added states, removed participants and onboarding flag.
    *
    * The following rules apply:
    *   - A participant in `adds` without a specified state becomes Active, which clears an existing
    *     onboarding flag, unless it is Nominated or Freezing: it keeps that state
    *
    * If `requiresPartyToBeOnboarded` is true, the following additional rules apply to the
    * participants in `adds` only:
    *   - Cannot have an existing hosting state other than Onboarding
    *   - Participants without a specified state get the Onboarding state
    */
  def computeNewParticipantStates(
      existingStates: Map[ParticipantId, HostingState],
      specifiedStates: Map[ParticipantId, HostingState],
      adds: Seq[ParticipantId] = Nil,
      removes: Seq[ParticipantId] = Nil,
      requiresPartyToBeOnboarded: Boolean = false,
  ): Either[String, Map[ParticipantId, HostingState]] = {
    val addsSet = adds.toSet

    val existingStatesVsOnboardingCheck =
      if (requiresPartyToBeOnboarded)
        NonEmpty
          .from(
            addsSet.filter(pid => existingStates.get(pid).exists(_ != HostingState.Onboarding))
          )
          .toLeft(())
          .leftMap(conflicts =>
            "Participants required to onboard the party already have a hosting state " +
              s"other than Onboarding: $conflicts"
          )
      else Either.unit

    val onboardingDefaults: Map[ParticipantId, HostingState] = {
      val hostingState =
        if (requiresPartyToBeOnboarded) HostingState.Onboarding else HostingState.Active
      Map.from(
        adds.distinct
          .filterNot(specifiedStates.contains)
          // A Nominated or Freezing participant moves forward only through an explicit hosting
          // state change
          .filterNot(pid => existingStates.get(pid).exists(!_.canHost))
          .map(pid => pid -> hostingState)
      )
    }

    for {
      _ <- existingStatesVsOnboardingCheck
    } yield existingStates.removedAll(removes) ++ onboardingDefaults ++ specifiedStates

  }
}
