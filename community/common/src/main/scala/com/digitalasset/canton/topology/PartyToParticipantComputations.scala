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
    *   - Cannot specify a state of removed participant
    *   - Cannot specify a state of a participant that doesn't exist in the current mapping nor is
    *     being added
    *   - Cannot remove a participant that doesn't exist in the current mapping
    *   - A participant in `adds` without a specified state becomes Active, which clears an existing
    *     onboarding flag, unless it is Nominated or Freezing: it keeps that state
    *
    * If `requiresPartyToBeOnboarded` is true, the following additional rules apply to the
    * participants in `adds` only:
    *   - Cannot specify a hosting state other than Onboarding
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

    val conflictsSpecifiedVsRemovesO =
      NonEmpty.from(specifiedStates.keySet.intersect(removes.toSet))
    val conflictsSpecifiedVsRemovesCheck = conflictsSpecifiedVsRemovesO
      .toLeft(())
      .leftMap(conflicts =>
        s"Hosting states for the following participant were found in both specified states and removes: $conflicts"
      )

    val specifiedStatesNotAmongExistingNorAddsO =
      NonEmpty.from(specifiedStates.keySet.diff(existingStates.keySet ++ addsSet))
    val specifiedStatesNotAmongExistingNorAddsCheck =
      specifiedStatesNotAmongExistingNorAddsO
        .toLeft(())
        .leftMap(unknowns =>
          s"Cannot specify hosting states for participants that are not existing or being added: $unknowns"
        )

    val unknownRemovalsO = NonEmpty
      .from(removes.toSet.diff(existingStates.keySet))
    val unknownRemovalsCheck = unknownRemovalsO
      .toLeft(())
      .leftMap(unknowns =>
        s"Cannot remove hosting states for participants that are not part of the existing mapping: $unknowns"
      )

    val specifiedStatesVsOnboardingCheck =
      if (requiresPartyToBeOnboarded)
        NonEmpty
          .from(
            addsSet.filter(pid => specifiedStates.get(pid).exists(_ != HostingState.Onboarding))
          )
          .toLeft(())
          .leftMap(conflicts =>
            "Participants required to onboard the party have a specified hosting state " +
              s"other than Onboarding: $conflicts"
          )
      else Either.unit

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
          // Moving a Nominated or Freezing participant forward would skip the freeze or the import
          // of the party's contracts, so it must be explicit
          .filterNot(pid => existingStates.get(pid).exists(!_.canHost))
          .map(pid => pid -> hostingState)
      )
    }

    for {
      _ <- conflictsSpecifiedVsRemovesCheck
      _ <- specifiedStatesNotAmongExistingNorAddsCheck
      _ <- unknownRemovalsCheck
      _ <- specifiedStatesVsOnboardingCheck
      _ <- existingStatesVsOnboardingCheck
    } yield existingStates.removedAll(removes) ++ onboardingDefaults ++ specifiedStates

  }
}
