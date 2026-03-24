// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.solver

import cats.data.State as CatsState
import cats.implicits.*
import com.digitalasset.canton.testing.modelbased.ast.Implicits.*
import com.digitalasset.canton.testing.modelbased.ast.Symbolic.*
import com.digitalasset.canton.testing.modelbased.ast.{Concrete, Skeleton, Symbolic}
import com.digitalasset.canton.testing.modelbased.conversions.{
  ConcreteToSymbolic,
  SkeletonToSymbolic,
  SymbolicToConcrete,
}
import com.microsoft.z3.*
import com.microsoft.z3.Status.{SATISFIABLE, UNKNOWN, UNSATISFIABLE}

import scala.util.Random

object SymbolicSolver {
  sealed trait KeyMode
  object KeyMode {
    case object UniqueContractKeys extends KeyMode
    case object NonUniqueContractKeys extends KeyMode
  }

  /** Attempts to find a concrete scenario that matches the given skeleton and satisfies all ledger
    * model invariants (authorization, contract lifecycle, key uniqueness, visibility, etc.).
    *
    * @param skeleton
    *   the abstract scenario shape to concretise.
    * @param numPackages
    *   the size of the package-ID universe. Each participant in the topology will host a non-empty
    *   subset of these packages, and commands may only reference packages available on the
    *   submitting participant.
    * @param numParties
    *   the size of the party universe. Every party set in the generated scenario (signatories,
    *   observers, controllers, …) is constrained to be a subset of this universe.
    * @param distinctKeyToContractRatio
    *   controls the upper bound on distinct key IDs relative to the number of `CreateWithKey`
    *   actions in the skeleton. Concretely, each symbolic key ID is constrained to lie in `[1,
    *   round(numCreateWithKeyActions * distinctKeyToContractRatio)]`.
    *
    *   - A ratio of `1.0` (the default) means the range of key IDs equals the number of
    *     `CreateWithKey` actions, so every created keyed contract could get its own unique key —
    *     maximising key diversity.
    *   - A ratio less than `1.0` (e.g. `0.3`) shrinks the key-ID range, forcing more contracts to
    *     share keys and producing scenarios with higher key contention.
    *
    * Note that this only sets an upper bound on the range: the solver is always free to unify keys
    * within the allowed range when other constraints require it.
    * @param keyMode
    *   determines whether contract-key uniqueness is strictly enforced
    *   ([[KeyMode.UniqueContractKeys]]) or relaxed ([[KeyMode.NonUniqueContractKeys]], the
    *   default).
    * @return
    *   `Some(concreteScenario)` if a satisfying assignment was found, or `None` if the problem is
    *   unsatisfiable or the solver timed out.
    */
  def solve(
      skeleton: Skeleton.Scenario,
      numPackages: Int,
      numParties: Int,
      distinctKeyToContractRatio: Double = 1,
      keyMode: KeyMode = KeyMode.NonUniqueContractKeys,
  ): Option[Concrete.Scenario] = {
    Global.setParameter("model_validate", "true")
    val ctx = new Context()
    val res = new SymbolicSolver(ctx, numPackages, numParties, distinctKeyToContractRatio, keyMode)
      .solve(skeleton)
    ctx.close()
    res
  }

  def valid(
      scenario: Concrete.Scenario,
      numPackages: Int,
      numParties: Int,
      keyMode: KeyMode = KeyMode.NonUniqueContractKeys,
  ): Boolean = {
    val ctx = new Context()
    val res =
      new SymbolicSolver(
        ctx,
        numPackages,
        numParties,
        distinctKeyToContractRatio = 1,
        keyMode = keyMode,
      )
        .validate(scenario)
    ctx.close()
    res
  }

  // -- Utility extension methods --

  private implicit class SeqZipWithPrefixOps[A](private val seq: Seq[A]) extends AnyVal {

    /** Pairs each element with the strict prefix of elements before it.
      *
      * {{{
      * Seq(a,b,c).zipWithPrefix = Seq((a, Seq()), (b, Seq(a)), (c, Seq(a,b)))
      * }}}
      */
    def zipWithPrefix: Seq[(A, Seq[A])] =
      seq.zip(seq.inits.toSeq.reverse)
  }
}

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
private class SymbolicSolver(
    ctx: Context,
    numPackages: Int,
    numParties: Int,
    distinctKeyToContractRatio: Double,
    keyMode: SymbolicSolver.KeyMode,
) {
  import SymbolicSolver.{KeyMode, SeqZipWithPrefixOps}

  private val participantIdSort = ctx.mkIntSort()
  private val contractIdSort = ctx.mkIntSort()
  private val keyIdSort = ctx.mkIntSort()
  private val partySort = ctx.mkIntSort()
  private val packageIdSort = ctx.mkIntSort()
  private val partySetSort = ctx.mkSetSort(ctx.mkIntSort())
  private val packageIdSetSort = ctx.mkSetSort(ctx.mkIntSort())

  private def and(bools: Seq[BoolExpr]): BoolExpr =
    ctx.mkAnd(bools*)

  private def or(bools: Seq[BoolExpr]): BoolExpr =
    ctx.mkOr(bools*)

  private def union[E <: Sort](
      elemSort: E,
      sets: Seq[ArrayExpr[E, BoolSort]],
  ): ArrayExpr[E, BoolSort] =
    sets.foldLeft(ctx.mkEmptySet(elemSort))(ctx.mkSetUnion(_, _))

  private def toSet[E <: Sort](elemSort: E, elems: Iterable[Expr[E]]): ArrayExpr[E, BoolSort] =
    elems.foldLeft(ctx.mkEmptySet(elemSort))((acc, cid) => ctx.mkSetAdd(acc, cid))

  private def isEmptyPartySet(partySet: PartySet): BoolExpr =
    ctx.mkEq(partySet, ctx.mkEmptySet(partySort))

  private def isEmptyPackageIdSet(packageIdSet: PackageIdSet): BoolExpr =
    ctx.mkEq(packageIdSet, ctx.mkEmptySet(packageIdSort))

  private def listContains(list: BoundedContractIdList, elem: ContractId): BoolExpr =
    or(list.elements.indices.map { i =>
      ctx.mkAnd(ctx.mkLt(ctx.mkInt(i), list.length), ctx.mkEq(list.elements(i), elem))
    })

  // for each element at index < length, the predicate holds
  private def allListElementsSatisfy(
      list: BoundedContractIdList,
      predicate: ContractId => BoolExpr,
  ): BoolExpr =
    and(list.elements.indices.map { i =>
      ctx.mkImplies(
        ctx.mkLt(ctx.mkInt(i), list.length),
        predicate(list.elements(i)),
      )
    })

  /** Enum used by [[listRelationshipConstraint]] */
  private sealed trait ListRelationship
  private case object Prefix extends ListRelationship
  private case object ListEquality extends ListRelationship

  /** Expresses a relationship constraint between two "virtual sequences" of symbolic contract IDs,
    * xs and ys.
    *
    *   - xs is represented by a sequence of contract IDs (xs) and booleans (bs). It corresponds to
    *     the filtered sequence [x_i | i <- 0..n-1 such that b_i].
    *   - ys is represented by a BoundedContractIdList, a sequence of symbolic integers (ys) and a
    *     symbolic length (l). It corresponds to the truncated sequence [ys_0, ys_1, ..., ys_{l-1}].
    *
    * Depending on the relationship flag, this function enforces that ys is either a strict prefix
    * of xs or exactly equal to xs.
    *
    * How this works: Because the elements of the virtual sequence represented by xs do not have
    * fixed indices (their positions depend dynamically on how many preceding booleans are true), we
    * use cumulative sums to calculate the virtual index of each of its elements: we build a list of
    * symbolic expressions sum c_{i+1} = c_i + (if b_i then 1 else 0), starting with c_0 = 0. Thus,
    * provided that b_i is true, c_i is the virtual index of element x_i.
    *
    * To enforce the prefix/equality, the function avoids using symbolic array indexing, which can
    * degrade solver performance. Instead, it generates the following conjunction of constraints.
    * For every element x in xs and every element y at physical index j in ys, it asserts: If x is
    * active (b is true), AND its virtual index equals j, AND j < l, then the value of x must
    * exactly equal y.
    *
    * @param xs
    *   A sequence of tuples `(x, b)` representing the filtered virtual list xs.
    * @param ys
    *   A [[BoundedContractIdList]] representing the truncated virtual list ys.
    * @param relationship
    *   If Prefix, enforces that ys is a prefix of xs (l <= length of xs). If ListEqual, enforces
    *   that ys is exactly equal to xs (l == length of xs).
    */
  private def listRelationshipConstraint(
      xs: Seq[(IntExpr, BoolExpr)],
      ys: BoundedContractIdList,
      relationship: ListRelationship,
  ): BoolExpr = {

    // build "virtual indices"
    val c0 = ctx.mkInt(0).asInstanceOf[IntExpr]
    val cumulativeSums = xs.scanLeft(c0) { case (prevSum, (_, b)) =>
      val addedValue = ctx.mkITE(b, ctx.mkInt(1), ctx.mkInt(0)).asInstanceOf[IntExpr]
      ctx.mkAdd(prevSum, addedValue).asInstanceOf[IntExpr]
    }

    // length constraints
    val totalLengthxs = cumulativeSums.lastOption.getOrElse(
      throw new IllegalStateException("cumulativeSums should have at least one element")
    )
    val lengthConstraints = Seq(ctx.mkGe(ys.length, ctx.mkInt(0))) ++ (relationship match {
      case ListEquality => Seq(ctx.mkEq(ys.length, totalLengthxs))
      case Prefix => Seq(ctx.mkLe(ys.length, totalLengthxs))
    })

    // element matching
    val matchConstraints = for {
      ((x, b), posI) <- xs.zip(cumulativeSums)
      (y, j) <- ys.elements.zipWithIndex
    } yield {
      val posJ = ctx.mkInt(j)

      val posMatch = ctx.mkEq(posI, posJ)
      val inBounds = ctx.mkLt(posJ, ys.length)

      ctx.mkImplies(
        ctx.mkAnd(b, posMatch, inBounds), // Premise
        ctx.mkEq(x, y), // Conclusion
      )
    }

    // combine everything
    and(lengthConstraints ++ matchConstraints)
  }

  // length is in valid range and active elements are pairwise distinct.
  private def disclosureListWellFormed(list: BoundedContractIdList): BoolExpr =
    ctx.mkAnd(
      // Length is in valid range
      ctx.mkGe(list.length, ctx.mkInt(0)),
      ctx.mkLe(list.length, ctx.mkInt(list.elements.length)),
      // No duplicates among active elements
      and(
        for {
          i <- list.elements.indices
          j <- 0 until i
        } yield ctx.mkImplies(
          ctx.mkLt(ctx.mkInt(i), list.length),
          ctx.mkNot(ctx.mkEq(list.elements(i), list.elements(j))),
        )
      ),
    )

  // TODO(#30398): where possible, scope the auxiliary functions of these "collect from ledger" functions to the
  //   body of the main function, to avoid polluting the namespace.
  private def collectCreatedContractIds(ledger: Ledger): Set[ContractId] =
    ledger.flatMap(_.commands.flatMap(collectCreatedContractIds)).toSet

  private def collectCreatedContractIds(command: Command): Set[ContractId] = command match {
    case Command(_, action) =>
      collectCreatedContractIds(action)
  }

  private def collectCreatedContractIds(action: Action): Set[ContractId] = action match {
    case Create(contractId, _, _) =>
      Set(contractId)
    case CreateWithKey(contractId, _, _, _, _) =>
      Set(contractId)
    case Exercise(_, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectCreatedContractIds).toSet
    case ExerciseByKey(_, _, _, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectCreatedContractIds).toSet
    case Fetch(_) =>
      Set.empty
    case FetchByKey(_, _, _) =>
      Set.empty
    case LookupByKey(_, _, _) =>
      Set.empty
    case QueryByKey(_, _, _, _) =>
      Set.empty
    case Rollback(subTransaction) =>
      subTransaction.view.flatMap(collectCreatedContractIds).toSet
  }

  private def collectCreatedKeyIds(ledger: Ledger): Set[KeyId] =
    ledger.flatMap(_.commands.flatMap(collectCreatedKeyIds)).toSet

  private def collectCreatedKeyIds(command: Command): Set[KeyId] = command match {
    case Command(_, action) => collectCreatedKeyIds(action)
  }

  private def collectCreatedKeyIds(action: Action): Set[KeyId] = action match {
    case Create(_, _, _) =>
      Set.empty
    case CreateWithKey(_, keyId, _, _, _) =>
      Set(keyId)
    case Exercise(_, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectCreatedKeyIds).toSet
    case ExerciseByKey(_, _, _, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectCreatedKeyIds).toSet
    case Fetch(_) =>
      Set.empty
    case FetchByKey(_, _, _) =>
      Set.empty
    case LookupByKey(_, _, _) =>
      Set.empty
    case QueryByKey(_, keyId, _, _) =>
      Set.empty
    case Rollback(subTransaction) =>
      subTransaction.view.flatMap(collectCreatedKeyIds).toSet
  }

  private def collectQueryByKeyKeyIds(ledger: Ledger): Set[KeyId] =
    ledger.flatMap(_.commands.flatMap(c => collectQueryByKeyKeyIds(c.action))).toSet

  private def collectQueryByKeyKeyIds(action: Action): Set[KeyId] = action match {
    case Create(_, _, _) => Set.empty
    case CreateWithKey(_, _, _, _, _) => Set.empty
    case Exercise(_, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectQueryByKeyKeyIds).toSet
    case ExerciseByKey(_, _, _, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectQueryByKeyKeyIds).toSet
    case Fetch(_) => Set.empty
    case FetchByKey(_, _, _) => Set.empty
    case LookupByKey(_, keyId, _) => Set(keyId)
    case QueryByKey(_, keyId, _, _) => Set(keyId)
    case Rollback(subTransaction) =>
      subTransaction.view.flatMap(collectQueryByKeyKeyIds).toSet
  }

  private def collectReferences(ledger: Ledger): Set[ContractId] =
    ledger.flatMap(_.commands.flatMap(collectReferences)).toSet

  private def collectReferences(command: Command): Set[ContractId] = command match {
    case Command(_, action) => collectReferences(action)
  }

  private def collectReferences(action: Action): Set[ContractId] = action match {
    case Create(_, _, _) =>
      Set.empty
    case CreateWithKey(_, _, _, _, _) =>
      Set.empty
    case Exercise(_, contractId, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectReferences).toSet + contractId
    case ExerciseByKey(_, _, _, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectReferences).toSet
    case Fetch(contractId) =>
      Set(contractId)
    case FetchByKey(_, _, _) =>
      Set.empty
    case LookupByKey(_, _, _) =>
      Set.empty
    case QueryByKey(_, _, _, _) =>
      Set.empty
    case Rollback(subTransaction) =>
      subTransaction.view.flatMap(collectReferences).toSet
  }

  private def collectPartySets(ledger: Ledger): List[PartySet] = {
    def collectCommandsPartySets(commands: Commands): List[PartySet] =
      commands.actAs +: commands.commands.flatMap(collectCommandPartySets)

    def collectCommandPartySets(command: Command): List[PartySet] = command match {
      case Command(_, action) => collectActionPartySets(action)
    }

    def collectActionPartySets(action: Action): List[PartySet] = action match {
      case Create(_, signatories, observers) =>
        List(signatories, observers)
      case CreateWithKey(_, _, maintainers, signatories, observers) =>
        List(maintainers, signatories, observers)
      case Exercise(_, _, controllers, choiceObservers, subTransaction) =>
        controllers +: choiceObservers +: subTransaction.flatMap(collectActionPartySets)
      case ExerciseByKey(_, _, _, maintainers, controllers, choiceObservers, subTransaction) =>
        maintainers +: controllers +: choiceObservers +: subTransaction.flatMap(
          collectActionPartySets
        )
      case Fetch(_) =>
        List.empty
      case FetchByKey(_, _, maintainers) =>
        List(maintainers)
      case LookupByKey(_, _, maintainers) =>
        List(maintainers)
      case QueryByKey(_, _, maintainers, _) =>
        List(maintainers)
      case Rollback(subTransaction) =>
        subTransaction.flatMap(collectActionPartySets)
    }

    ledger.flatMap(collectCommandsPartySets)
  }

  private def collectQueryByKeyAnswers(ledger: Ledger): List[BoundedContractIdList] =
    ledger.flatMap(_.commands.flatMap(c => collectQueryByKeyAnswers(c.action)))

  private def collectQueryByKeyAnswers(action: Action): List[BoundedContractIdList] = action match {
    case Create(_, _, _) => List.empty
    case CreateWithKey(_, _, _, _, _) => List.empty
    case Exercise(_, _, _, _, subTransaction) => subTransaction.flatMap(collectQueryByKeyAnswers)
    case ExerciseByKey(_, _, _, _, _, _, subTransaction) =>
      subTransaction.flatMap(collectQueryByKeyAnswers)
    case Fetch(_) => List.empty
    case FetchByKey(_, _, _) => List.empty
    case LookupByKey(_, _, _) => List.empty
    case QueryByKey(contractIds, _, _, _) => List(contractIds)
    case Rollback(subTransaction) => subTransaction.flatMap(collectQueryByKeyAnswers)
  }

  private def collectDisclosures(ledger: Ledger): List[BoundedContractIdList] =
    ledger.map(_.disclosures)

  private def disclosuresWellFormed(ledger: Ledger): BoolExpr =
    and(collectDisclosures(ledger).map(disclosureListWellFormed))

  private def collectNonEmptyPartySets(ledger: Ledger): List[PartySet] = {
    def collectCommandsNonEmptyPartySets(commands: Commands): List[PartySet] =
      commands.actAs +: commands.commands.flatMap(collectCommandNonEmptyPartySets)

    def collectCommandNonEmptyPartySets(command: Command): List[PartySet] = command match {
      case Command(_, action) => collectActionNonEmptyPartySets(action)
    }

    def collectActionNonEmptyPartySets(action: Action): List[PartySet] = action match {
      case Create(_, signatories, _) =>
        List(signatories)
      case CreateWithKey(_, _, maintainers, signatories, _) =>
        List(maintainers, signatories)
      case Exercise(_, _, controllers, _, subTransaction) =>
        controllers +: subTransaction.flatMap(collectActionNonEmptyPartySets)
      case ExerciseByKey(_, _, _, maintainers, controllers, _, subTransaction) =>
        maintainers +: controllers +: subTransaction.flatMap(collectActionNonEmptyPartySets)
      case Fetch(_) =>
        List.empty
      case FetchByKey(_, _, maintainers) =>
        List(maintainers)
      case LookupByKey(_, _, maintainers) =>
        List(maintainers)
      case QueryByKey(_, _, maintainers, _) =>
        List(maintainers)
      case Rollback(subTransaction) =>
        subTransaction.flatMap(collectActionNonEmptyPartySets)
    }

    ledger.flatMap(collectCommandsNonEmptyPartySets)
  }

  private def collectPackageIds(ledger: Ledger): Set[PackageId] =
    ledger.flatMap(_.commands.flatMap(_.packageId)).toSet

  /** Assigns sequential integer IDs (0, 1, 2, …) to every created contract in the ledger, following
    * a pre-order traversal of the action tree. This removes symmetry between alpha-equivalent
    * scenarios by canonicalising contract IDs, preventing the solver from exploring redundant
    * permutations.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def assignCreatedContractIds(ledger: Ledger): BoolExpr = {
    var lastContractId = -1

    def numberCommands(commands: Commands): BoolExpr =
      and(commands.commands.map(numberCommand))

    def numberCommand(command: Command): BoolExpr = command match {
      case Command(_, action) => numberAction(action)
    }

    def numberAction(action: Action): BoolExpr = action match {
      case Create(contractId, _, _) =>
        lastContractId += 1
        ctx.mkEq(contractId, ctx.mkInt(lastContractId))
      case CreateWithKey(contractId, _, _, _, _) =>
        lastContractId += 1
        ctx.mkEq(contractId, ctx.mkInt(lastContractId))
      case Exercise(_, _, _, _, subTransaction) =>
        and(subTransaction.map(numberAction))
      case ExerciseByKey(_, _, _, _, _, _, subTransaction) =>
        and(subTransaction.map(numberAction))
      case Fetch(_) =>
        ctx.mkTrue()
      case FetchByKey(_, _, _) =>
        ctx.mkTrue()
      case LookupByKey(_, _, _) =>
        ctx.mkTrue()
      case QueryByKey(_, _, _, _) =>
        ctx.mkTrue()
      case Rollback(subTransaction) =>
        and(subTransaction.map(numberAction))
    }

    and(ledger.map(numberCommands))
  }

  /** Unlike contract IDs or participant IDs which are assigned sequential integers, key IDs use a
    * looser bounding constraint. This is because the final number of distinct keys is variable: two
    * different CreateWithKey actions may or may not share the same key. We only constrain each key
    * ID to lie in [1, numKeys], which bounds the search space to the simplest case where every key
    * in the transaction is different. The solver is still free to unify key IDs if the constraints
    * allow it.
    */
  private def constrainKeyIds(ledger: Ledger): BoolExpr = {
    val createdKeyIds = collectCreatedKeyIds(ledger)
    val queryByKeyKeyIds = collectQueryByKeyKeyIds(ledger)
    val upperBound = (createdKeyIds.size * distinctKeyToContractRatio).round
    ctx.mkAnd(
      and(
        createdKeyIds
          .map(keyId =>
            ctx.mkAnd(
              ctx.mkGe(keyId, ctx.mkInt(1)),
              ctx.mkLe(keyId, ctx.mkInt(upperBound)),
            )
          )
          .toSeq
      ),
      and(
        queryByKeyKeyIds
          .map(keyId =>
            ctx.mkAnd(
              ctx.mkGe(keyId, ctx.mkInt(1)),
              // Leave room for querying keys that have not been created
              ctx.mkLe(keyId, ctx.mkInt(upperBound + 1)),
            )
          )
          .toSeq
      ),
    )
  }

  /** Assigns sequential integer IDs (0, 1, 2, …) to each participant in the topology, breaking
    * symmetry between alpha-equivalent topologies and preventing the solver from exploring
    * redundant permutations of participant identifiers.
    */
  private def assignParticipantIds(topology: Symbolic.Topology): BoolExpr = {
    def numberParticipant(participant: Participant, i: Int): BoolExpr =
      ctx.mkEq(participant.participantId, ctx.mkInt(i))

    and(topology.zipWithIndex.map((numberParticipant _).tupled))
  }

  private def tiePartiesToParticipants(
      topology: Topology,
      partiesOf: FuncDecl[PartySetSort],
  ): BoolExpr = {
    def tie(participant: Participant): BoolExpr =
      ctx.mkEq(ctx.mkApp(partiesOf, participant.participantId), participant.parties)

    and(topology.map(tie))
  }

  private def allPartiesCoveredBy(
      allParties: Symbolic.PartySet,
      partySets: Seq[Symbolic.PartySet],
  ): BoolExpr =
    ctx.mkEq(union(partySort, partySets), allParties)

  /** Enforces contract lifecycle consistency across the entire ledger:
    *   - Contracts must be created before they can be exercised, fetched, or looked up.
    *   - Consumed (archived) contracts cannot be exercised or fetched again.
    *   - Key uniqueness: no two active contracts may share the same key (keyId + maintainers).
    *   - Disclosures only reference created, non-consumed contracts.
    *   - Rollback actions restore the contract state to what it was before the rollback.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def consistentLedger(
      hasKey: FuncDecl[BoolSort],
      keyIdOf: FuncDecl[KeyIdSort],
      maintainersOf: FuncDecl[PartySetSort],
      ledger: Ledger,
  ): BoolExpr = {
    // State threaded through the computation.
    case class State(created: List[ContractId], consumed: Set[ContractId])

    def allCreated(global: State, local: State): Set[ContractId] =
      (local.created ++ global.created).toSet

    def allConsumed(global: State, local: State): Set[ContractId] =
      global.consumed ++ local.consumed

    def elem(contractId: ContractId, set: Set[ContractId]): BoolExpr =
      or(set.map(ctx.mkEq(contractId, _)).toList)

    def isActive(contractId: ContractId, global: State, local: State): BoolExpr =
      ctx.mkAnd(
        elem(contractId, allCreated(global, local)),
        ctx.mkNot(elem(contractId, allConsumed(global, local))),
      )

    def contractHasKey(contractId: ContractId, keyId: KeyId, maintainers: PartySet): BoolExpr =
      ctx.mkAnd(
        ctx.mkApp(hasKey, contractId),
        ctx.mkEq(ctx.mkApp(keyIdOf, contractId), keyId),
        ctx.mkEq(ctx.mkApp(maintainersOf, contractId), maintainers),
      )

    /* Asserts that no currently active contract has the given key. For every previously created
     * contract, if it has the specified (keyId, maintainers) pair then it must already be
     * consumed.
     */
    def noActiveContractWithKey(
        keyId: KeyId,
        maintainers: PartySet,
        global: State,
        local: State,
    ): BoolExpr =
      and(
        for {
          cid <- allCreated(global, local).toSeq
        } yield ctx.mkImplies(
          contractHasKey(cid, keyId, maintainers),
          elem(cid, allConsumed(global, local)),
        )
      )

    // Builds the ordered list of candidate contracts for key resolution.
    // Priority (most recent first):
    //   1. Contracts created in the current Commands (local.created)
    //   2. Disclosed contracts (first disclosure first)
    //   3. Contracts created in previous Commands (global.created)
    // Each candidate is paired with a bool that is true when the candidate exists.
    def buildKeyCandidates(
        global: State,
        local: State,
        disclosures: BoundedContractIdList,
    ): Seq[(ContractId, BoolExpr)] = {
      val currentCandidates = local.created.map(cid => (cid, ctx.mkTrue()))
      val disclosureCandidates = disclosures.elements.indices.map { i =>
        (disclosures.elements(i), ctx.mkLt(ctx.mkInt(i), disclosures.length))
      }
      val previousCandidates = global.created.map(cid => (cid, ctx.mkTrue()))
      currentCandidates ++ disclosureCandidates ++ previousCandidates
    }

    // Constrains contractId to be the most recent active contract with the given
    // key among the ordered candidates.
    def mostRecentActiveContractWithKey(
        contractId: ContractId,
        keyId: KeyId,
        maintainers: PartySet,
        candidates: Seq[(ContractId, BoolExpr)],
        global: State,
        local: State,
    ): BoolExpr =
      or(candidates.zipWithPrefix.map { case ((cid, exists), prefix) =>
        val allMoreRecentCandidatesAreInvalid =
          and(prefix.map { case (prevCid, prevExists) =>
            ctx.mkOr(
              ctx.mkNot(prevExists),
              elem(prevCid, allConsumed(global, local)),
              ctx.mkNot(contractHasKey(prevCid, keyId, maintainers)),
            )
          })
        ctx.mkAnd(
          exists,
          ctx.mkEq(contractId, cid),
          contractHasKey(cid, keyId, maintainers),
          isActive(cid, global, local),
          allMoreRecentCandidatesAreInvalid,
        )
      })

    /* Enforces per-action lifecycle and key invariants, updating some state as it goes:
     *   - Create: registers the contract as created; asserts it has no key.
     *   - CreateWithKey: registers the contract as created with a key; asserts no other active
     *     contract shares the same key (keyId + maintainers).
     *   - Exercise / ExerciseByKey: the target contract must be the most recent active contract
     *     with the given key, where "most recent" is defined by the NUCK lookup semantics.
     *   - Fetch: the target contract must be active
     *   - FetchByKey: the target contract must be the most recent active contract with the given
     *     key, where "most recent" is defined by the NUCK lookup semantics.
     *   - LookupByKey: If the lookup is successful then the looked up contract must be the most
     *     recent active contract with the given key, where "most recent" is defined by the NUCK
     *     lookup semantics. If the lookup failed then there must be no active contract with that
     *     key.
     *   - Rollback: processes the sub-transaction for constraint generation but restores the
     *     created/consumed state afterwards, modelling the rollback semantics.
     *
     * Threads the local state via State monad. The global state and disclosures are read-only
     * context.
     */
    def consistentActionM(
        global: State,
        disclosures: BoundedContractIdList,
        action: Action,
    ): CatsState[State, BoolExpr] =
      action match {
        case Create(contractId, _, _) =>
          CatsState
            .modify[State](l => l.copy(created = contractId :: l.created))
            .as(ctx.mkNot(ctx.mkApp(hasKey, contractId)))
        case CreateWithKey(contractId, keyId, maintainers, _, _) =>
          keyMode match {
            case KeyMode.UniqueContractKeys =>
              for {
                local <- CatsState.get[State]
                constraint = ctx.mkAnd(
                  contractHasKey(contractId, keyId, maintainers),
                  noActiveContractWithKey(keyId, maintainers, global, local),
                )
                _ <- CatsState.modify[State](l => l.copy(created = contractId :: l.created))
              } yield constraint
            case KeyMode.NonUniqueContractKeys =>
              CatsState
                .modify[State](l => l.copy(created = contractId :: l.created))
                .as(contractHasKey(contractId, keyId, maintainers))
          }
        case Exercise(kind, contractId, _, _, subTransaction) =>
          for {
            local <- CatsState.get[State]
            pre = isActive(contractId, global, local)
            _ <- CatsState
              .modify[State](l => l.copy(consumed = l.consumed + contractId))
              .whenA(kind == Consuming)
            subConstraints <- subTransaction.traverse(consistentActionM(global, disclosures, _))
          } yield ctx.mkAnd(pre, and(subConstraints))
        case ExerciseByKey(kind, contractId, keyId, maintainers, _, _, subTransaction) =>
          for {
            local <- CatsState.get[State]
            pre = keyMode match {
              case KeyMode.UniqueContractKeys =>
                ctx.mkAnd(
                  isActive(contractId, global, local),
                  contractHasKey(contractId, keyId, maintainers),
                )
              case KeyMode.NonUniqueContractKeys =>
                val candidates = buildKeyCandidates(global, local, disclosures)
                mostRecentActiveContractWithKey(
                  contractId,
                  keyId,
                  maintainers,
                  candidates,
                  global,
                  local,
                )
            }
            _ <- CatsState
              .modify[State](l => l.copy(consumed = l.consumed + contractId))
              .whenA(kind == Consuming)
            subConstraints <- subTransaction.traverse(consistentActionM(global, disclosures, _))
          } yield ctx.mkAnd(pre, and(subConstraints))
        case Fetch(contractId) =>
          CatsState.inspect(local => isActive(contractId, global, local))
        case FetchByKey(contractId, keyId, maintainers) =>
          CatsState.inspect { local =>
            keyMode match {
              case KeyMode.UniqueContractKeys =>
                ctx.mkAnd(
                  isActive(contractId, global, local),
                  contractHasKey(contractId, keyId, maintainers),
                )
              case KeyMode.NonUniqueContractKeys =>
                val candidates = buildKeyCandidates(global, local, disclosures)
                mostRecentActiveContractWithKey(
                  contractId,
                  keyId,
                  maintainers,
                  candidates,
                  global,
                  local,
                )
            }
          }
        case LookupByKey(contractId, keyId, maintainers) =>
          contractId match {
            case Some(cid) =>
              CatsState.inspect { local =>
                keyMode match {
                  case KeyMode.UniqueContractKeys =>
                    ctx.mkAnd(
                      isActive(cid, global, local),
                      contractHasKey(cid, keyId, maintainers),
                    )
                  case KeyMode.NonUniqueContractKeys =>
                    val candidates = buildKeyCandidates(global, local, disclosures)
                    mostRecentActiveContractWithKey(
                      cid,
                      keyId,
                      maintainers,
                      candidates,
                      global,
                      local,
                    )
                }
              }
            case None =>
              CatsState.inspect(local => noActiveContractWithKey(keyId, maintainers, global, local))
          }
        case QueryByKey(contractIds, keyId, maintainers, exhaustive) =>
          CatsState.inspect { local =>
            val candidates = buildKeyCandidates(global, local, disclosures)
            // Filter to active candidates with the looked-up key, deduplicating contracts
            // that appear in multiple candidate sources (e.g. both in disclosures and
            // in global.created): retain only the most recent one.
            val activeCandidatesWithKey =
              candidates.zipWithPrefix.map { case ((cid, exists), prefix) =>
                val active = isActive(cid, global, local)
                val hasKey = contractHasKey(cid, keyId, maintainers)
                val notSeenBefore = and(prefix.map { case (prevCid, prevExists) =>
                  ctx.mkOr(ctx.mkNot(prevExists), ctx.mkNot(ctx.mkEq(cid, prevCid)))
                })
                (cid, ctx.mkAnd(exists, active, hasKey, notSeenBefore))
              }
            ctx.mkAnd(
              listRelationshipConstraint(
                activeCandidatesWithKey,
                contractIds,
                if (exhaustive) ListEquality else Prefix,
              ),
              // Asking for 0 contracts is not allowed, so we should never end up with a QueryByKey node in which
              // no contracts were returned but the answer is not exhaustive.
              if (exhaustive) ctx.mkTrue() else ctx.mkGt(contractIds.length, ctx.mkInt(0)),
            )
          }
        case Rollback(subTransaction) =>
          // Run sub-transaction but discard local state changes
          CatsState.inspect { local =>
            subTransaction
              .traverse(consistentActionM(global, disclosures, _))
              .map(and)
              .runA(local)
              .value
          }
      }

    /* Enforces that a command group's disclosures are well-formed and all sub-commands are
     * consistent:
     *   - Disclosed contract IDs must be a subset of previously created contracts.
     *   - Disclosed contract IDs must not include any consumed (archived) contracts. This is
     *     actually allowed by the ledger model, but retrieving the disclosure of archived
     *     contracts against the IDE ledger is tricky so we disallow it.
     *   - Actions in commands must satisfy [[consistentActionM]].
     *
     * Threads the global state via State monad.
     */
    def consistentCommandsM(commands: Commands): CatsState[State, BoolExpr] =
      CatsState { global =>
        val emptyLocal = State(List.empty, Set.empty)
        val (finalLocal, commandConstraint) =
          commands.commands
            .map(_.action)
            .traverse(consistentActionM(global, commands.disclosures, _))
            .map(and)
            .run(emptyLocal)
            .value
        val constraint = ctx.mkAnd(
          // All disclosed contract IDs are created (in global Commands)
          allListElementsSatisfy(
            commands.disclosures,
            cid => elem(cid, global.created.toSet),
          ),
          // in theory you can disclose consumed contracts, but we don't allow it
          // yet because their disclosure is tricky to fetch.
          allListElementsSatisfy(
            commands.disclosures,
            cid => ctx.mkNot(elem(cid, global.consumed)),
          ),
          commandConstraint,
        )
        val updatedGlobal = State(
          finalLocal.created ++ global.created,
          finalLocal.consumed ++ global.consumed,
        )
        (updatedGlobal, constraint)
      }

    ledger
      .traverse(consistentCommandsM)
      .map(and)
      .runA(State(List.empty, Set.empty))
      .value
  }

  /** Constrains every contract referenced by a command to be visible to the submitting participant.
    * A contract is visible to a participant if either:
    *   - the contract has been explicitly disclosed in the command, or
    *   - the participant hosts at least one party that is a stakeholder (signatory or observer) of
    *     the contract.
    */
  private def visibleContracts(
      partiesOf: FuncDecl[PartySetSort],
      signatoriesOf: FuncDecl[PartySetSort],
      observersOf: FuncDecl[PartySetSort],
      ledger: Symbolic.Ledger,
  ): BoolExpr = {

    def visibleContractId(
        disclosures: BoundedContractIdList,
        participantId: ParticipantId,
        contractId: ContractId,
    ): BoolExpr =
      ctx.mkOr(
        listContains(disclosures, contractId),
        ctx.mkNot(
          isEmptyPartySet(
            ctx.mkSetIntersection(
              ctx.mkApp(partiesOf, participantId).asInstanceOf[PartySet],
              ctx.mkSetUnion(
                ctx.mkApp(signatoriesOf, contractId).asInstanceOf[PartySet],
                ctx.mkApp(observersOf, contractId).asInstanceOf[PartySet],
              ),
            )
          )
        ),
      )

    def visibleAction(
        disclosures: BoundedContractIdList,
        participantId: ParticipantId,
        action: Action,
    ): BoolExpr = action match {
      case Create(_, _, _) =>
        ctx.mkTrue()
      case CreateWithKey(_, _, _, _, _) =>
        ctx.mkTrue()
      case Exercise(_, contractId, _, _, subTransaction) =>
        ctx.mkAnd(
          visibleContractId(disclosures, participantId, contractId),
          and(subTransaction.map(visibleAction(disclosures, participantId, _))),
        )
      case ExerciseByKey(_, contractId, _, _, _, _, subTransaction) =>
        ctx.mkAnd(
          visibleContractId(disclosures, participantId, contractId),
          and(subTransaction.map(visibleAction(disclosures, participantId, _))),
        )
      case Fetch(contractId) =>
        visibleContractId(disclosures, participantId, contractId)
      case FetchByKey(contractId, _, _) =>
        visibleContractId(disclosures, participantId, contractId)
      case LookupByKey(contractId, _, _) =>
        contractId match {
          case Some(cid) =>
            visibleContractId(disclosures, participantId, cid)
          case None =>
            ctx.mkTrue()
        }
      case QueryByKey(contractIds, _, _, _) =>
        allListElementsSatisfy(
          contractIds,
          cid => visibleContractId(disclosures, participantId, cid),
        )
      case Rollback(subTransaction) =>
        and(subTransaction.map(visibleAction(disclosures, participantId, _)))
    }

    def visibleCommand(
        disclosures: BoundedContractIdList,
        participantId: ParticipantId,
        command: Command,
    ): BoolExpr = command match {
      case Command(_, action) => visibleAction(disclosures, participantId, action)
    }

    def visibleCommands(commands: Commands): BoolExpr =
      and(commands.commands.map(visibleCommand(commands.disclosures, commands.participantId, _)))

    and(ledger.map(visibleCommands))
  }

  private def hideCreatedContractsInSiblings(ledger: Ledger): BoolExpr = {
    def hideInCommands(commands: Commands): BoolExpr =
      hideInCommandList(commands.commands)

    def hideInCommandList(commands: List[Command]): BoolExpr = commands match {
      case Nil => ctx.mkBool(true)
      case command :: commands =>
        val constraints = for {
          contractId <- collectCreatedContractIds(command)
          reference <- commands.flatMap(collectReferences)
        } yield ctx.mkNot(ctx.mkEq(contractId, reference))
        ctx.mkAnd(and(constraints.toList), hideInCommandList(commands))
    }

    and(ledger.map(hideInCommands))
  }

  private def authorized(
      signatoriesOf: FuncDecl[PartySetSort],
      observersOf: FuncDecl[PartySetSort],
      ledger: Ledger,
  ): BoolExpr = {

    def authorizedCommands(commands: Commands): BoolExpr =
      and(commands.commands.map(authorizedCommand(commands.actAs, _)))

    def authorizedCommand(actAs: PartySet, command: Command): BoolExpr = command match {
      case Command(_, action) => authorizedAction(actAs, action)
    }

    def authorizedAction(actAs: PartySet, action: Action): BoolExpr = action match {
      case Create(contractId, signatories, observers) =>
        ctx.mkAnd(
          ctx.mkEq(ctx.mkApp(signatoriesOf, contractId), signatories),
          ctx.mkEq(ctx.mkApp(observersOf, contractId), observers),
          ctx.mkSetSubset(signatories, actAs),
        )
      case CreateWithKey(contractId, _, _, signatories, observers) =>
        ctx.mkAnd(
          ctx.mkEq(ctx.mkApp(signatoriesOf, contractId), signatories),
          ctx.mkEq(ctx.mkApp(observersOf, contractId), observers),
          ctx.mkSetSubset(signatories, actAs),
        )
      case Exercise(_, contractId, controllers, _, subTransaction) =>
        ctx.mkAnd(
          ctx.mkSetSubset(controllers, actAs),
          and(
            subTransaction.map(
              authorizedAction(
                ctx.mkSetUnion(
                  controllers,
                  ctx.mkApp(signatoriesOf, contractId).asInstanceOf[PartySet],
                ),
                _,
              )
            )
          ),
        )
      case ExerciseByKey(_, contractId, _, _, controllers, _, subTransaction) =>
        ctx.mkAnd(
          ctx.mkSetSubset(controllers, actAs),
          and(
            subTransaction.map(
              authorizedAction(
                ctx.mkSetUnion(
                  controllers,
                  ctx.mkApp(signatoriesOf, contractId).asInstanceOf[PartySet],
                ),
                _,
              )
            )
          ),
        )
      case Fetch(contractId) =>
        ctx.mkNot(
          isEmptyPartySet(
            ctx.mkSetIntersection(
              actAs,
              ctx.mkSetUnion(
                ctx.mkApp(signatoriesOf, contractId).asInstanceOf[PartySet],
                ctx.mkApp(observersOf, contractId).asInstanceOf[PartySet],
              ),
            )
          )
        )
      case FetchByKey(contractId, _, _) =>
        ctx.mkNot(
          isEmptyPartySet(
            ctx.mkSetIntersection(
              actAs,
              ctx.mkSetUnion(
                ctx.mkApp(signatoriesOf, contractId).asInstanceOf[PartySet],
                ctx.mkApp(observersOf, contractId).asInstanceOf[PartySet],
              ),
            )
          )
        )
      case LookupByKey(_, _, maintainers) =>
        ctx.mkSetSubset(maintainers, actAs)
      case QueryByKey(_, _, maintainers, _) =>
        ctx.mkSetSubset(maintainers, actAs)
      case Rollback(subTransaction) =>
        and(subTransaction.map(authorizedAction(actAs, _)))
    }

    and(ledger.map(authorizedCommands))
  }

  private def validParticipantIdsInCommands(scenario: Scenario): BoolExpr = {
    def allParticipantIds = scenario.topology.map(_.participantId)

    def isValidParticipantId(participantId: ParticipantId): BoolExpr =
      or(allParticipantIds.map(ctx.mkEq(participantId, _)))

    and(scenario.ledger.map(commands => isValidParticipantId(commands.participantId)))
  }

  private def actorsAreHostedOnParticipant(
      partiesOf: FuncDecl[PartySetSort],
      ledger: Symbolic.Ledger,
  ): BoolExpr = {
    def actorsAreHostedOnParticipant(commands: Commands): BoolExpr =
      ctx.mkSetSubset(
        commands.actAs,
        ctx.mkApp(partiesOf, commands.participantId).asInstanceOf[PartySet],
      )

    and(ledger.map(actorsAreHostedOnParticipant))
  }

  private val allPartiesSetLiteral = toSet(partySort, (1 to numParties).map(ctx.mkInt))

  private val allPackagesSetLiteral = toSet(packageIdSort, (0 until numPackages).map(ctx.mkInt))

  private def partySetsWellFormed(ledger: Ledger, allParties: PartySet): BoolExpr =
    and(collectPartySets(ledger).map(s => ctx.mkSetSubset(s, allParties)))

  private def nonEmptyPartySetsWellFormed(ledger: Ledger): BoolExpr =
    and(
      collectNonEmptyPartySets(ledger)
        .map(s => ctx.mkNot(isEmptyPartySet(s)))
    )

  private def tiePackagesToParticipants(
      packagesOf: FuncDecl[Symbolic.PackageIdSetSort],
      topology: Symbolic.Topology,
  ): BoolExpr =
    and(
      topology.map(participant =>
        ctx.mkEq(ctx.mkApp(packagesOf, participant.participantId), participant.packages)
      )
    )

  private def packagesAreHostedOnParticipant(
      packagesOf: FuncDecl[PackageIdSetSort],
      ledger: Ledger,
  ): BoolExpr =
    and(
      for {
        commands <- ledger
        command <- commands.commands
        packageId <- command.packageId
      } yield ctx.mkSetMembership(
        packageId,
        ctx.mkApp(packagesOf, commands.participantId).asInstanceOf[PackageIdSet],
      )
    )

  private def packageIdSetsWellFormed(
      topology: Symbolic.Topology,
      allPackages: PackageIdSet,
  ): BoolExpr = {
    val packageIdSets = topology.map(_.packages)
    and(
      packageIdSets
        .map(packageIdSet =>
          ctx.mkAnd(
            ctx.mkSetSubset(packageIdSet, allPackages),
            ctx.mkNot(isEmptyPackageIdSet(packageIdSet)),
          )
        )
    )
  }

  /** For every created contract in the ledger that has a key, constrains
    * `maintainersOfKeyId(keyIdOf(c)) = maintainersOf(c)`. This ensures that two contracts sharing
    * the same key ID must have the same maintainers, effectively limiting the number of distinct
    * keys the solver needs to consider.
    */
  private def tieMaintainersToKeyIds(
      maintainersOfKeyId: FuncDecl[PartySetSort],
      keyIdOf: FuncDecl[KeyIdSort],
      maintainersOf: FuncDecl[PartySetSort],
      hasKey: FuncDecl[BoolSort],
      ledger: Ledger,
  ): BoolExpr =
    and(
      collectCreatedContractIds(ledger).toSeq.map(c =>
        ctx.mkImplies(
          ctx.mkApp(hasKey, c),
          ctx.mkEq(
            ctx.mkApp(maintainersOfKeyId, ctx.mkApp(keyIdOf, c)),
            ctx.mkApp(maintainersOf, c),
          ),
        )
      )
    )

  private def maintainersAreSubsetsOfSignatories(ledger: Ledger): BoolExpr = {
    def validCommands(commands: Commands): BoolExpr =
      and(commands.commands.map(validCommand))

    def validCommand(command: Command): BoolExpr = command match {
      case Command(_, action) => validAction(action)
    }

    def validAction(action: Action): BoolExpr = action match {
      case Create(_, _, _) =>
        ctx.mkTrue()
      case CreateWithKey(_, _, maintainers, signatories, _) =>
        ctx.mkSetSubset(maintainers, signatories)
      case Exercise(_, _, _, _, subTransaction) =>
        and(subTransaction.map(validAction))
      case ExerciseByKey(_, _, _, _, _, _, subTransaction) =>
        and(subTransaction.map(validAction))
      case Fetch(_) =>
        ctx.mkTrue()
      case FetchByKey(_, _, _) =>
        ctx.mkTrue()
      case LookupByKey(_, _, _) =>
        ctx.mkTrue()
      case QueryByKey(_, _, _, _) =>
        ctx.mkTrue()
      case Rollback(subTransaction) =>
        and(subTransaction.map(validAction))
    }

    and(ledger.map(validCommands))
  }

  /** Simulates randomness by forcing the solver to produce diverse, non-trivial solutions.
    *
    * Without this constraint, the solver will tend to return the "simplest" satisfying assignment
    * it finds first — typically a degenerate or repetitive scenario (e.g. all parties identical,
    * all sets empty where allowed). To avoid this, we construct a random hash of all symbolic
    * variables in the scenario and constrain it to equal true, effectively partitioning the
    * solution space randomly and forcing the solver into an arbitrary region.
    *
    * The hash is built as follows:
    *   1. '''Bit-blast''' every symbolic value in the scenario (contract IDs, party sets, package
    *      IDs, disclosure sets, topology) into a flat sequence of boolean expressions.
    *   1. '''Randomly shuffle''' these booleans (using `scala.util.Random`), so that each call to
    *      `solve` explores a different part of the solution space.
    *   1. '''Chunk''' the shuffled bits into groups of `chunkSize` and XOR each group together,
    *      producing a shorter sequence of hash bits. Each hash bit is a parity constraint over a
    *      random subset of the original bits.
    *   1. '''Conjoin''' all hash bits (assert they are all true). This effectively constrains
    *      roughly half of the solution space away for each hash bit, steering the solver toward a
    *      random satisfying assignment.
    *
    * The `chunkSize` parameter controls the strength of the constraint: smaller chunks produce more
    * hash bits and therefore a tighter constraint (fewer solutions), while larger chunks are more
    * permissive. A value that is too small may make the problem unsatisfiable.
    */
  private def hashConstraint(chunkSize: Int, scenario: Scenario): BoolExpr = {

    val numBitsContractId =
      1.max((scala.math.log(scenario.ledger.numContracts.toDouble) / scala.math.log(2)).ceil.toInt)

    val numBitsPackageId =
      (scala.math.log(numPackages.toDouble) / scala.math.log(2)).ceil.toInt max 1

    def contractIdToBits(contractId: ContractId): Seq[BoolExpr] = {
      val bitVector = ctx.mkInt2BV(numBitsContractId, contractId)
      (0 until numBitsContractId).map(i => ctx.mkEq(ctx.mkExtract(i, i, bitVector), ctx.mkBV(1, 1)))
    }

    def partySetToBits(partySet: PartySet): Seq[BoolExpr] =
      (1 to numParties).map(i => ctx.mkSelect(partySet, ctx.mkInt(i)).asInstanceOf[BoolExpr])

    def intToBits(numBits: Int, value: IntExpr): Seq[BoolExpr] = {
      val bitVector = ctx.mkInt2BV(numBits, value)
      (0 until numBits).map(i => ctx.mkEq(ctx.mkExtract(i, i, bitVector), ctx.mkBV(1, 1)))
    }

    def boundedListToBits(list: BoundedContractIdList): Seq[BoolExpr] = {
      val numBitsLength =
        (scala.math.log((list.elements.length + 1).toDouble) / scala.math.log(2)).ceil.toInt max 1
      val lengthBits = intToBits(numBitsLength, list.length)
      val elementBits = list.elements.zipWithIndex.flatMap { case (elem, i) =>
        val active = ctx.mkLt(ctx.mkInt(i), list.length)
        contractIdToBits(elem).map(bit => ctx.mkAnd(active, bit))
      }
      lengthBits ++ elementBits
    }

    def packageIdToBits(packageId: PackageId): Seq[BoolExpr] = {
      val bitVector = ctx.mkInt2BV(numBitsPackageId, packageId)
      (0 until numBitsPackageId).map(i => ctx.mkEq(ctx.mkExtract(i, i, bitVector), ctx.mkBV(1, 1)))
    }

    def packageIdSetToBits(packageIdSet: PackageIdSet): Seq[BoolExpr] =
      (0 until numPackages).map(i =>
        ctx.mkSelect(packageIdSet, ctx.mkInt(i)).asInstanceOf[BoolExpr]
      )

    def ledgerToBits(ledger: Ledger): Seq[BoolExpr] =
      Seq.concat(
        collectReferences(ledger).toSeq.flatMap(contractIdToBits),
        collectPartySets(ledger).flatMap(partySetToBits),
        collectDisclosures(ledger).flatMap(boundedListToBits),
        collectQueryByKeyAnswers(ledger).flatMap(boundedListToBits),
        collectPackageIds(ledger).flatMap(packageIdToBits),
      )

    def participantToBits(participant: Participant): Seq[BoolExpr] =
      Seq.concat(
        packageIdSetToBits(participant.packages),
        partySetToBits(participant.parties),
      )

    def topologyToBits(topology: Topology): Seq[BoolExpr] =
      topology.flatMap(participantToBits)

    def scenarioToBits(scenario: Scenario): Seq[BoolExpr] =
      Seq.concat(
        topologyToBits(scenario.topology),
        ledgerToBits(scenario.ledger),
      )

    def xor(bits: Iterable[BoolExpr]): BoolExpr =
      bits.fold(ctx.mkFalse())((b1, b2) => ctx.mkXor(b1, b2))

    def mkRandomHash(chunkSize: Int, scenario: Scenario): Seq[BoolExpr] =
      Random.shuffle(scenarioToBits(scenario)).grouped(chunkSize).map(xor).toSeq

    and(mkRandomHash(chunkSize, scenario))
  }

  private def niceNumbers(symScenario: Scenario): BoolExpr = {
    val Scenario(symTopology, symLedger) = symScenario
    ctx.mkAnd(
      // Assign IDs 0..n to create events
      assignCreatedContractIds(symLedger),
      // Make sure that key nums are between 1 and number of creates with keys
      constrainKeyIds(symLedger),
      // Assign distinct participant IDs to participants in the topology
      assignParticipantIds(symTopology),
    )
  }

  private def validScenario(
      constants: Constants,
      allPackages: PackageIdSet,
      allParties: PartySet,
      symScenario: Scenario,
  ): BoolExpr = {
    import constants.*
    val Scenario(symTopology, symLedger) = symScenario
    ctx.mkAnd(
      // Assign distinct contract IDs to create actions
      ctx.mkDistinct(collectCreatedContractIds(symLedger).toSeq*),
      // Assign distinct participant IDs to participants in the topology
      ctx.mkDistinct(symTopology.map(_.participantId)*),
      // Tie parties to participants via partiesOf
      tiePartiesToParticipants(symTopology, partiesOf),
      // Participants cover all parties
      allPartiesCoveredBy(allParties, symTopology.map(_.parties)),
      // Participants have at least one party - not required but empty participants are kind of useless
      and(symTopology.map(p => ctx.mkNot(isEmptyPartySet(p.parties)))),
      // Every party set in ledger is a subset of allParties
      partySetsWellFormed(symLedger, allParties),
      // Signatories and controllers in ledger are non-empty
      nonEmptyPartySetsWellFormed(symLedger),
      // make sure that the same set of maintainers is always associated to the same key ID, in order to limit the
      // number of distinct keys.
      tieMaintainersToKeyIds(maintainersOfKeyId, keyIdOf, maintainersOf, hasKey, symLedger),
      // Only active contracts can be exercised or fetched
      consistentLedger(hasKey, keyIdOf, maintainersOf, symLedger),
      // Only contracts visible to the participant can be exercised of fetched on that participant
      visibleContracts(partiesOf, signatoriesOf, observersOf, symLedger),
      // Contracts created in a command cannot be referenced by other actions in the same command
      hideCreatedContractsInSiblings(symLedger),
      // Transactions adhere to the authorization rules
      authorized(signatoriesOf, observersOf, symLedger),
      // Participants IDs in a command refer to existing participants
      validParticipantIdsInCommands(symScenario),
      // Actors of a command should be hosted by the participant executing the command
      actorsAreHostedOnParticipant(partiesOf, symLedger),
      // Maintainers are subsets of signatories
      maintainersAreSubsetsOfSignatories(symLedger),
      // Disclosure lists are well-formed (valid length and no duplicates)
      disclosuresWellFormed(symLedger),
      // Package sets of participants are non-empty subsets of {0 .. numPackages-1}
      packageIdSetsWellFormed(symTopology, allPackages),
      // Tie packages to participants via packagesOf
      tiePackagesToParticipants(packagesOf, symTopology),
      // Package IDs of commands refer to packages present on the participant
      packagesAreHostedOnParticipant(packagesOf, symLedger),
    )
  }

  private case class Constants(
      // ContractId -> Set[Party]: the signatories of a contract.
      // Invariant: non-empty; must be a subset of the acting parties for Create actions.
      signatoriesOf: FuncDecl[PartySetSort],
      // ContractId -> Set[Party]: the observers of a contract.
      observersOf: FuncDecl[PartySetSort],
      // ParticipantId -> Set[Party]: the parties hosted on a participant.
      // Invariant: non-empty; the union of all partiesOf(p) covers the full party universe.
      // Parties may be hosted on more than one participant.
      partiesOf: FuncDecl[PartySetSort],
      // ParticipantId -> Set[PackageId]: the packages available on a participant.
      // Invariant: non-empty subset of the package universe; commands may only reference
      // packages hosted on the submitting participant.
      packagesOf: FuncDecl[PackageIdSetSort],
      // ContractId -> KeyId: the key identifier of a contract (only meaningful when hasKey is true).
      // Values are bounded to [1, numKeys] where numKeys is the number of CreateWithKey actions
      // in the ledger (see [[constrainKeyIds]]). Different contracts may share the same key ID.
      keyIdOf: FuncDecl[KeyIdSort],
      // ContractId -> Set[Party]: the maintainers of a contract's key (only meaningful when hasKey is true).
      // Invariant: must be a non-empty subset of the contract's signatories.
      maintainersOf: FuncDecl[PartySetSort],
      // KeyId -> Set[Party]: canonical maintainer set for a given key ID.
      // Constrained by [[tieMaintainersToKeyIds]] so that for every created contract `c` with a
      // key, `maintainersOfKeyId(keyIdOf(c)) == maintainersOf(c)`. This ensures that contracts
      // sharing the same key ID must have the same maintainers, limiting the number of distinct
      // keys the solver needs to consider.
      maintainersOfKeyId: FuncDecl[PartySetSort],
      // ContractId -> Bool: whether a contract has an associated key.
      // This is a per-contract property (rather than per-template) because the solver has no
      // notion of templates; it is constrained to be consistent across the ledger by
      // [[consistentLedger]]: Create actions set it to false, CreateWithKey actions set it to true.
      hasKey: FuncDecl[BoolSort],
  )

  private def mkFreshConstants(): Constants =
    Constants(
      signatoriesOf = ctx.mkFreshFuncDecl("signatories", Array[Sort](contractIdSort), partySetSort),
      observersOf = ctx.mkFreshFuncDecl("observers", Array[Sort](contractIdSort), partySetSort),
      partiesOf = ctx.mkFreshFuncDecl("parties", Array[Sort](participantIdSort), partySetSort),
      packagesOf =
        ctx.mkFreshFuncDecl("packages", Array[Sort](participantIdSort), packageIdSetSort),
      keyIdOf = ctx.mkFreshFuncDecl("key_nums", Array[Sort](contractIdSort), keyIdSort),
      maintainersOf = ctx.mkFreshFuncDecl("maintainers", Array[Sort](contractIdSort), partySetSort),
      maintainersOfKeyId =
        ctx.mkFreshFuncDecl("maintainers_of_key_id", Array[Sort](keyIdSort), partySetSort),
      hasKey = ctx.mkFreshFuncDecl("has_key", Array[Sort](contractIdSort), ctx.mkBoolSort()),
    )

  /** Returns the objective to maximize: the total length of QueryByKey lists so the solver prefers
    * non-trivial results over empty lists.
    */
  private def objective(symScenario: Scenario): Option[ArithExpr[IntSort]] = {
    val queryByKeyAnswers = collectQueryByKeyAnswers(symScenario.ledger)
    Option.when(queryByKeyAnswers.nonEmpty)(ctx.mkAdd(queryByKeyAnswers.map(_.length)*))
  }

  private def solve(scenario: Skeleton.Scenario): Option[Concrete.Scenario] = {
    val symScenario = SkeletonToSymbolic.toSymbolic(ctx, scenario)
    val constants = mkFreshConstants()

    // We make an optimizer, not a solver, in order to maximize the length of answers in QueryByKey nodes
    val solver = ctx.mkOptimize()
    val params = ctx.mkParams()
    params.add("timeout", 5000)
    solver.setParameters(params)

    // Constrain the various IDs without loss of generality in order to
    // prevent the generation of different but alpha-equivalent random
    // scenarios.
    solver.Add(niceNumbers(symScenario))
    // The scenario is valid
    val allParties = ctx.mkFreshConst("all_parties", partySetSort).asInstanceOf[PartySet]
    val allPackages = ctx.mkFreshConst("all_pkgs", packageIdSetSort).asInstanceOf[PackageIdSet]
    solver.Add(
      ctx.mkAnd(
        ctx.mkEq(allParties, allPartiesSetLiteral),
        ctx.mkEq(allPackages, allPackagesSetLiteral),
        validScenario(constants, allPackages, allParties, symScenario),
      )
    )
    // Equate a random hash of all the symbols to a constant
    solver.Add(hashConstraint(20, symScenario))
    // Maximize the total length of QueryByKey lists so the solver prefers non-trivial results over empty lists.
    objective(symScenario).foreach(solver.MkMaximize(_))

    solver.Check() match {
      case SATISFIABLE =>
        Some(
          new SymbolicToConcrete(numPackages, numParties, ctx, solver.getModel)
            .toConcrete(symScenario)
        )
      case UNSATISFIABLE =>
        None
      // Z3's Optimize solver reports timeouts as "canceled" (not "timeout")
      case UNKNOWN
          if solver.getReasonUnknown == "canceled" || solver.getReasonUnknown == "timeout" =>
        None
      case other =>
        throw new IllegalStateException(
          s"Unexpected solver result: $other, ${solver.getReasonUnknown}"
        )
    }
  }

  private def validate(scenario: Concrete.Scenario): Boolean = {
    // Global.setParameter("verbose", "1000")
    val solver = ctx.mkSolver()
    val params = ctx.mkParams()
    params.add("proof", false)
    params.add("model", false)
    params.add("unsat_core", false)
    params.add("timeout", 5000)
    solver.setParameters(params)

    val symScenario = ConcreteToSymbolic.toSymbolic(ctx, scenario)
    val allParties = union(partySort, symScenario.topology.map(_.parties))
    val allPackages = union(packageIdSort, symScenario.topology.map(_.packages))
    val constants = mkFreshConstants()

    solver.check(validScenario(constants, allPackages, allParties, symScenario)) match {
      case SATISFIABLE => true
      case UNSATISFIABLE => false
      case UNKNOWN =>
        throw new IllegalStateException(s"Unexpected solver result: ${solver.getReasonUnknown}")
    }
  }
}
