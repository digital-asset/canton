// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.generators

import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.testing.modelbased.ast.Concrete.*
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver
import org.scalacheck.Shrink

import java.util.concurrent.Executors
import scala.annotation.nowarn
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}

object Shrinker {

  private def validScenario(scenario: Scenario): Boolean = {
    val numParties = scenario.topology.flatMap(_.parties).toSet.size
    val maxPackageId =
      scenario.ledger.flatMap(_.commands.flatMap(_.packageId)).maxOption.getOrElse(0)
    val res =
      try {
        SymbolicSolver.valid(scenario, maxPackageId, numParties)
      } catch {
        case _: Throwable =>
          // sometimes z3 times out on ground constraints (!)
          false
      }
    res
  }

  lazy val shrinkPartyId: Shrink[PartyId] =
    Shrink.shrinkIntegral[KeyId].suchThat(_ >= 0)

  lazy val shrinkPartySet: Shrink[PartySet] =
    Shrink.shrinkContainer[Set, PartyId](implicitly, shrinkPartyId, implicitly)

  lazy val shrinkContractIdList: Shrink[ContractIdList] =
    Shrink.shrinkContainer[List, ContractId](implicitly, shrinkContractId, implicitly)

  lazy val shrinkPackageIdSet: Shrink[PackageIdSet] =
    Shrink.shrinkContainer[Set, PackageId](implicitly, shrinkPackageId, implicitly)

  lazy val shrinkKeyId: Shrink[KeyId] =
    Shrink.shrinkIntegral[KeyId].suchThat(_ >= 0)

  lazy val shrinkContractId: Shrink[ContractId] =
    Shrink.shrinkIntegral[KeyId].suchThat(_ >= 0)

  lazy val shrinkParticipantId: Shrink[ParticipantId] =
    Shrink.shrinkIntegral[ParticipantId].suchThat(_ >= 0)

  lazy val shrinkPackageId: Shrink[PackageId] =
    Shrink.shrinkIntegral[PackageId].suchThat(_ >= 0)

  @nowarn("cat=deprecation")
  lazy val shrinkExerciseKind: Shrink[ExerciseKind] = Shrink {
    case NonConsuming => Stream(Consuming)
    case Consuming => Stream.empty
  }

  lazy val shrinkTransaction: Shrink[Transaction] =
    Shrink.shrinkContainer[List, Action](implicitly, shrinkAction, implicitly)

  private def isNoRollback(action: Action): Boolean = action match {
    case _: Rollback => false
    case _ => true
  }

  lazy val shrinkAction: Shrink[Action] = Shrink {
    case Create(contractId, signatories, observers) =>
      Shrink
        .shrinkTuple2(shrinkPartySet, shrinkPartySet)
        .shrink((signatories, observers))
        .map { case (shrunkenSignatories, shrunkenObservers) =>
          Create(contractId, shrunkenSignatories, shrunkenObservers)
        }
    case CreateWithKey(contractId, keyId, maintainers, signatories, observers) =>
      Shrink
        .shrinkTuple3(shrinkPartySet, shrinkPartySet, shrinkPartySet)
        .shrink((maintainers, signatories, observers))
        .map[Action] { case (shrunkenMaintainers, shrunkenSignatories, shrunkenObservers) =>
          CreateWithKey(
            contractId,
            keyId,
            shrunkenMaintainers,
            shrunkenSignatories,
            shrunkenObservers,
          )
        }
        .lazyAppendedAll(
          List(
            Create(contractId, signatories, observers)
          )
        )
    case Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
      Shrink
        .shrinkTuple5(
          shrinkExerciseKind,
          shrinkContractId,
          shrinkPartySet,
          shrinkPartySet,
          shrinkTransaction,
        )
        .shrink((kind, contractId, controllers, choiceObservers, subTransaction))
        .map((Exercise.apply _).tupled)
        .lazyAppendedAll(subTransaction)
    case ExerciseByKey(
          kind,
          contractId,
          keyId,
          maintainers,
          controllers,
          choiceObservers,
          subTransaction,
        ) =>
      Shrink
        .shrinkTuple7(
          shrinkExerciseKind,
          shrinkContractId,
          shrinkKeyId,
          shrinkPartySet,
          shrinkPartySet,
          shrinkPartySet,
          shrinkTransaction,
        )
        .shrink(
          (kind, contractId, keyId, maintainers, controllers, choiceObservers, subTransaction)
        )
        .map((ExerciseByKey.apply _).tupled)
        .lazyAppendedAll(subTransaction)
        .lazyAppendedAll(
          List(
            Exercise(
              kind,
              contractId,
              controllers,
              choiceObservers,
              subTransaction,
            )
          )
        )
    case Fetch(contractId) =>
      shrinkContractId
        .shrink(contractId)
        .map(Fetch.apply)
    case LookupByKey(contractId, keyId, maintainers) =>
      Shrink
        .shrinkTuple3(
          Shrink.shrinkOption(shrinkContractId),
          shrinkKeyId,
          shrinkPartySet,
        )
        .shrink((contractId, keyId, maintainers))
        .map((LookupByKey.apply _).tupled)
    case FetchByKey(contractId, keyId, maintainers) =>
      Shrink
        .shrinkTuple3(
          shrinkContractId,
          shrinkKeyId,
          shrinkPartySet,
        )
        .shrink((contractId, keyId, maintainers))
        .map((FetchByKey.apply _).tupled)
    case QueryByKey(contractIds, keyId, maintainers, exhaustive) =>
      Shrink
        .shrinkTuple3(
          shrinkContractIdList,
          shrinkKeyId,
          shrinkPartySet,
        )
        .shrink((contractIds, keyId, maintainers))
        .map { case (shrunkenCids, shrunkenKeyId, shrunkenMaintainers) =>
          QueryByKey(shrunkenCids, shrunkenKeyId, shrunkenMaintainers, exhaustive)
        }
    case Rollback(subTransaction) =>
      shrinkTransaction
        .suchThat(as => as.nonEmpty && as.forall(isNoRollback))
        .shrink(subTransaction)
        .map(Rollback.apply)
        .lazyAppendedAll(subTransaction)
  }

  private def isToplevelAction(action: Action): Boolean = action match {
    case _: Create | _: CreateWithKey | _: Exercise | _: ExerciseByKey => true
    case _ => false
  }

  lazy val shrinkCommand: Shrink[Command] = Shrink { case Command(packageId, action) =>
    Shrink
      .shrinkTuple2(
        Shrink.shrinkOption(shrinkPackageId),
        shrinkAction.suchThat(isToplevelAction),
      )
      .shrink((packageId, action))
      .map((Command.apply _).tupled)
  }

  lazy val shrinkCommands: Shrink[Commands] = Shrink {
    case Commands(participantId, actAs, disclosures, actions) =>
      Shrink
        .shrinkTuple4(
          shrinkParticipantId,
          shrinkPartySet,
          shrinkContractIdList,
          Shrink
            .shrinkContainer[List, Command](implicitly, shrinkCommand, implicitly)
            .suchThat(_.nonEmpty),
        )
        .shrink((participantId, actAs, disclosures, actions))
        .map((Commands.apply _).tupled)
  }

  lazy val shrinkLedger: Shrink[Ledger] =
    Shrink.shrinkContainer[List, Commands](implicitly, shrinkCommands, implicitly)

  lazy val shrinkParticipant: Shrink[Participant] = Shrink {
    case Participant(participantId, pkgs, parties) =>
      Shrink
        .shrinkTuple2(shrinkPackageIdSet, shrinkPartySet)
        .shrink((pkgs, parties))
        .map { case (shrunkenPkgs, shrunkenParties) =>
          Participant(participantId, shrunkenPkgs, shrunkenParties)
        }
  }

  lazy val shrinkTopology: Shrink[Topology] =
    Shrink.shrinkContainer[Seq, Participant](implicitly, shrinkParticipant, implicitly)

  lazy val shrinkScenario: Shrink[Scenario] = Shrink[Scenario] { case Scenario(topology, ledger) =>
    Shrink
      .shrinkTuple2(shrinkTopology, shrinkLedger)
      .shrink((topology, ledger))
      .map((Scenario.apply _).tupled)
  }.suchThat(validScenario)

  // -- Utility methods for finding smaller failing values using shrinkers --

  /** Result of shrinking a failing value.
    *
    * @param value
    *   the smallest failing value found
    * @param error
    *   the error message associated with the smallest failing value
    * @param shrinkSteps
    *   the number of successful shrink steps performed
    * @param timedOut
    *   if the shrinking process was stopped due to a timeout, the elapsed duration
    */
  final case class ShrinkResult[A](
      value: A,
      error: String,
      shrinkSteps: Int,
      timedOut: Option[FiniteDuration],
  ) {
    def summary: String =
      s"Shrinking completed after $shrinkSteps step(s)${timedOut
          .fold("")(d => s", timed out after ${d.toMinutes}m${d.toSeconds % 60}s")}"
  }

  private object ShrinkToFailureAuxiliaryTypes {
    // Used by shrinkToFailure to represent the outcome of evaluating a single shrink candidate.
    sealed trait CandidateResult[+A]
    final case class CandidateFailed[A](value: A, error: String) extends CandidateResult[A]
    case object CandidatePassed extends CandidateResult[Nothing]
    case object CandidateTimedOut extends CandidateResult[Nothing]

    // Used by findFailingCandidate to represent the outcome of searching through all candidates.
    sealed trait SearchResult[+A]
    final case class SearchFoundSmallerValue[A](value: A, error: String) extends SearchResult[A]
    case object SearchDidNotFindSmallerValue extends SearchResult[Nothing]
    case object SearchTimedOut extends SearchResult[Nothing]
  }

  /** Shrinks a failing value by repeatedly trying smaller variants (as defined by the given
    * `Shrink` instance) and keeping the first one that still fails the given property. Returns the
    * smallest failing value together with its error message.
    *
    * The shrinking process will stop after `timeout` and return the smallest failing value found so
    * far. Each property evaluation is run in a separate thread and interrupted if the remaining
    * time runs out.
    *
    * @param value
    *   the initial failing value
    * @param error
    *   the error message associated with the initial failure
    * @param property
    *   a property that returns `Right(())` on success and `Left(errorMessage)` on failure
    * @param timeout
    *   maximum duration for the entire shrinking process (defaults to 365 days)
    * @param shrink
    *   the shrink instance defining how to produce smaller candidates
    */
  def shrinkToFailure[A](
      value: A,
      error: String,
      property: A => Either[String, Unit],
      timeout: FiniteDuration = 365.days,
  )(implicit shrink: Shrink[A]): ShrinkResult[A] = {
    import ShrinkToFailureAuxiliaryTypes.*

    val deadline = timeout.fromNow

    def elapsedSoFar(): FiniteDuration = timeout - deadline.timeLeft

    val executor = Executors.newSingleThreadExecutor()
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    def evaluateWithTimeout(candidate: A, remaining: FiniteDuration): CandidateResult[A] =
      try
        Await.result(Future(property(candidate)), remaining) match {
          case Left(e) => CandidateFailed(candidate, e)
          case Right(()) => CandidatePassed
        }
      catch {
        case _: TimeoutException => CandidateTimedOut
      }

    @scala.annotation.tailrec
    def findFailingCandidate(
        candidates: Iterator[A]
    ): SearchResult[A] =
      if (deadline.isOverdue()) SearchTimedOut
      else if (!candidates.hasNext) SearchDidNotFindSmallerValue
      else {
        val candidate = candidates.next()
        val remaining = deadline.timeLeft
        evaluateWithTimeout(candidate, remaining) match {
          case CandidateFailed(v, e) => SearchFoundSmallerValue(v, e)
          case CandidatePassed => findFailingCandidate(candidates)
          case CandidateTimedOut => SearchTimedOut
        }
      }

    @scala.annotation.tailrec
    def shrinkLoop(
        value: A,
        error: String,
        steps: Int,
    ): ShrinkResult[A] =
      findFailingCandidate(shrink.shrink(value).iterator) match {
        case SearchFoundSmallerValue(smallerValue, smallerError) =>
          shrinkLoop(smallerValue, smallerError, steps + 1)
        case SearchDidNotFindSmallerValue =>
          ShrinkResult(value, error, steps, timedOut = None)
        case SearchTimedOut =>
          ShrinkResult(value, error, steps, timedOut = Some(elapsedSoFar()))
      }

    try shrinkLoop(value, error, 0)
    finally {
      discard(executor.shutdownNow())
    }
  }
}
