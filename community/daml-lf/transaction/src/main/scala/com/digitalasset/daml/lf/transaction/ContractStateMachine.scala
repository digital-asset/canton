// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.transaction.Transaction.{KeyCreate, KeyInput, NegativeKeyLookup}
import com.digitalasset.daml.lf.transaction.TransactionErrors.{
  CreateError,
  DuplicateContractId,
  DuplicateContractKey,
  InconsistentContractKey,
  KeyInputError,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.common.annotations.VisibleForTesting

/** Implements a state machine for contracts and their keys while interpreting a Daml-LF command
  * or iterating over a [[com.digitalasset.daml.lf.transaction.HasTxNodes]] in execution order.
  * The contract state machine keeps track of the updates to the [[ContractStateMachine.ActiveLedgerState]]
  * since the beginning of the interpretation or iteration.
  * Given a [[ContractStateMachine.State]] `s`, a client can compute the next state for a given action node `n`,
  * by calling `s.handleNode(..., n, ...)`.
  * For a rollback node `nr`, a client must call `beginRollback` before processing the first child of `nr` and
  * `endRollback` after processing the last child of `nr`.
  *
  * @tparam Nid Type parameter for [[com.digitalasset.daml.lf.transaction.NodeId]]s during interpretation.
  *             Use [[scala.Unit]] for iteration.
  *
  * @see com.digitalasset.daml.lf.transaction.HasTxNodes.contractKeyInputs for an iteration in mode
  *       [[Model.UCK]] and [[Mode.LegacyNuck]]
  * @see ContractStateMachineSpec.visitSubtree for iteration in all modes
  */
object ContractStateMachine {

  /** Controls whether the engine should error out when it encounters duplicate keys.  */
  sealed abstract class Mode extends Product with Serializable {
    def toString: String
  }

  object Mode {
    /**
      * Default mode used to configure contract state machines when PV!=dev.
      */
    val default: Mode = NoContractKey

    /**
     * Default mode used to configure contract state machines when in PV=dev.
     */
    val devDefault: Mode = UCKWithRollback

    /** Disable key uniqueness checks and only consider byKey operations.
      * Note that no stable semantics are provided for LegacyNUCK mode.
      */
    case object LegacyNUCK extends Mode {
      override val toString: String = "LegacyNUCK"
    }

    /** Considers all nodes mentioning keys as byKey operations and checks for contract key uniqueness. */
    case object UCKWithRollback extends Mode {
      override val toString: String = "UCKWithRollback"
    }

    /** Considers all nodes mentioning keys as byKey operations and checks for contract key uniqueness but disallow rollback */
    case object UCKWithoutRollback extends Mode {
      override val toString: String = "UCKWithoutRollback"
    }


    /* Disable key contract keys completely. For now all key operations crash at runtime */
    case object NoContractKey extends Mode {
      override val toString: String = "NoContractKey"
    }

    def fromString: String => Option[Mode] = {
      case "LegacyNUCK" => Some(Mode.LegacyNUCK)
      case "UCKWithRollback" => Some(Mode.UCKWithRollback)
      case "UCKWithoutRollback" => Some(Mode.UCKWithoutRollback)
      case "NoContractKey" => Some(Mode.NoContractKey)
      case _ => None
    }
  }

  sealed abstract class State[Nid] {

    /**  Tracks all contracts created by a node processed so far (including nodes under a rollback).
      */

    def locallyCreated: Set[ContractId]

    val inputContractIds: Set[ContractId]

    /** Contains the key mappings required by Daml Engine to get to the current state
      * (including [[Transaction.KeyCreate]] for create nodes).
      * That is, `globalKeyInputs` contains the answers to all [[engine.ResultNeedKey]] requests that Daml Engine would
      * emit while it is building the nodes passed to this contract state machine as input.
      *
      * The map `globalKeyInputs` grows monotonically. Its entries are never overwritten and not reset after a
      * rollback scope is left.
      * Formally, if a contract state machine transitions from state `s1` to state `s2`,
      * then `s1.globalKeyInputs` is a subset of `s2.globalKeyInputs`.
      *
      * The map `globalKeyInputs` can be used to resolve keys during re-interpretation.
      *
      * In UCK modes ([[Mode.UCKWithRollback]], [[Mode.UCKWithoutRollback]]),
      * `globalKeyInputs` stores the contract key states required at the beginning of the transaction.
      * The first node involving a key determines the state of the key in `globalKeyInputs`.
      *
      * In mode [[Mode.LegacyNUCK]],
      * `globalKeyInputs(k)` is defined by the first node `n` referring to the key `k`:
      *   - If `n` is a fetch-by-key or exercise-by-key, then `globalKeyInputs(k)` is [[Transaction.KeyActive]].
      *   - If `n` is lookup-by-key, then `globalKeyInputs(k)` is [[Transaction.KeyActive]] (positive lookup)
      *     or [[Transaction.NegativeKeyLookup]] (negative lookup).
      *   - If `n` is a create, then `globalKeyInputs(k)` is [[Transaction.KeyCreate]].
      *     Note: a plain fetch or exercise (with `byKey == false`) does not impact `globalKeyInputs`.
      */
    def globalKeyInputs: Map[GlobalKey, KeyInput]

    /** The return value indicates if the given contract is either consumed, inactive, or otherwise
      * - Some(Left(nid)) -- consumed by a specified node-id
      * - Some(Right(())) -- inactive, because the (local) contract creation has been rolled-back
      * - None -- neither consumed nor inactive
      */
    def consumedByOrInactive(cid: Value.ContractId): Option[Either[Nid, Unit]]

    def mode: Mode

    private[lf] def visitCreate(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
    ): Either[CreateError, State[Nid]]

    def handleExercise(nid: Nid, exe: Node.Exercise): Either[KeyInputError, State[Nid]]

    /** Omits the key lookup that is done in [[com.digitalasset.daml.lf.speedy.Compiler.compileChoiceByKey]] for by-key nodes,
      * which translates to a [[resolveKey]] below.
      * Use [[handleExercise]] when visiting an exercise node during iteration.
      */
    private[lf] def visitExercise(
        nodeId: Nid,
        targetId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
        consuming: Boolean,
    ): Either[InconsistentContractKey, State[Nid]]

    /** Must be used to handle lookups iff in UCK modes ([[Mode.UCKWithRollback]], [[Mode.UCKWithoutRollback]])
      */
    private[transaction] def handleLookup(
        lookup: Node.LookupByKey
    ): Either[KeyInputError, State[Nid]]

    private[lf] def visitLookup(
        gk: GlobalKey,
        keyInput: Option[ContractId],
        keyResolution: Option[ContractId],
    ): Either[InconsistentContractKey, State[Nid]]

    private[lf] def resolveKey(
        gkey: GlobalKey
    ): Either[Option[ContractId] => (KeyMapping, State[Nid]), (KeyMapping, State[Nid])]

    @VisibleForTesting
    private[transaction] def handleFetch(node: Node.Fetch): Either[KeyInputError, State[Nid]]

    private[lf] def visitFetch(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
    ): Either[InconsistentContractKey, State[Nid]]

    /** Utility method that takes a node and computes the corresponding next state.
      * The method does not handle any children of `node`; it is up to the caller to do that.
      *
      * @param keyInput will only be used in mode [[Mode.LegacyNUCK]] and if the node is a lookupByKey
      */
    def handleNode(
        id: Nid,
        node: Node.Action,
        keyInput: => Option[ContractId],
    ): Either[KeyInputError, State[Nid]]

    /** To be called when interpretation enters a try block or iteration enters a Rollback node
      * Must be matched by [[endRollback]] or [[dropRollback]].
      */
    def beginRollback(): State[Nid]

    /** To be called when interpretation does insert a Rollback node or iteration leaves a Rollback node.
      * Must be matched by a [[beginRollback]].
      */
    def endRollback(): State[Nid]

    /** To be called if interpretation notices that a try block did not lead to a Rollback node
      * Must be matched by a [[beginRollback]].
      */
    private[lf] def dropRollback(): State[Nid]

    @VisibleForTesting
    // ideally should be restricted to ContractStateMachine object
    private[transaction] def withinRollbackScope: Boolean

    /** Let `this` state be the result of iterating over a transaction `tx` until just before a node `n`.
      * Let `substate` be the state obtained after fully iterating over the subtree rooted at `n`
      * starting from [[State.empty]].
      * Then, `advance(resolver, substate)` equals the state resulting from iterating over `tx` until
      * processing all descendants of `n`.
      *
      * The call to `advance(resolver, substate)` fails if and only if
      * the iteration over the subtree rooted at `n` starting from `this` fails, but the error may be different.
      *
      * @param resolver
      * In modes [[Mode.UCKWithRollback]], [[Mode.UCKWithoutRollback]], and [[Mode.NoContractKey]], this parameter has no effect.
      * In mode [[Mode.LegacyNUCK]], `resolver` must be the resolver used while iterating over `tx`
      * until just before `n`.
      * While iterating over the subtree rooted at `n`, [[projectKeyResolver]](`resolver`) must be used as resolver.
      * @param substate
      * The state obtained after fully iterating over the subtree `n` starting from [[State.empty]].
      * Consumed contracts ([[activeState.consumedBy]]) in `this` and `substate` must be disjoint.
      * @see com.digitalasset.daml.lf.transaction.HasTxNodes.contractKeyInputs for an iteration in modes
      * [[Mode.UCKWithRollback]], [[Mode.UCKWithoutRollback]], and [[Mode.LegacyNUCK]]
      * @see ContractStateMachineSpec.visitSubtree for iteration in all modes
      */
    def advance(resolver: KeyResolver, substate: State[Nid]): Either[KeyInputError, State[Nid]]

    /** @see advance */
    def projectKeyResolver(resolver: KeyResolver): KeyResolver

    @VisibleForTesting
    private[lf] def withLocalContractKey(contractId: ContractId, key: GlobalKey): State[Nid]
  }

  final case class UniqueContractKeyStateWithRollback[Nid] private[lf] (
      override val locallyCreated: Set[ContractId],
      override val inputContractIds: Set[ContractId],
      override val globalKeyInputs: Map[GlobalKey, KeyInput],
      activeState: ContractStateMachine.ActiveLedgerState[Nid],
      rollbackStack: List[ContractStateMachine.ActiveLedgerState[Nid]],
  ) extends State[Nid] {

    override def consumedByOrInactive(cid: Value.ContractId): Option[Either[Nid, Unit]] = {
      activeState.consumedBy.get(cid) match {
        case Some(nid) => Some(Left(nid)) // consumed
        case None =>
          if (locallyCreated(cid) && !activeState.locallyCreatedThisTimeline.contains(cid)) {
            Some(Right(())) // inactive
          } else {
            None // neither
          }
      }
    }

    override def mode: Mode = Mode.UCKWithRollback

    private def lookupActiveKey(key: GlobalKey): Option[Option[ContractId]] =
      activeState.getLocalActiveKey(key).orElse(lookupActiveGlobalKeyInput(key))

    private def lookupActiveGlobalKeyInput(
        key: GlobalKey
    ): Option[ContractStateMachine.KeyMapping] =
      globalKeyInputs.get(key).map {
        case Transaction.KeyActive(cid) if !activeState.consumedBy.contains(cid) =>
          ContractStateMachine.KeyActive(cid)
        case _ =>
          ContractStateMachine.KeyInactive
      }

    /** Visit a create node */
    private def handleCreate(node: Node.Create): Either[KeyInputError, State[Nid]] =
      visitCreate(node.coid, node.gkeyOpt).left.map(KeyInputError.from)

    private[lf] def visitCreate(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
    ): Either[CreateError, State[Nid]] = {
      if (locallyCreated.union(inputContractIds).contains(contractId)) {
        Left(CreateError.inject(DuplicateContractId(contractId)))
      } else {
        val me =
          this.copy(
            locallyCreated = locallyCreated + contractId,
            activeState = this.activeState
              .copy(locallyCreatedThisTimeline =
                this.activeState.locallyCreatedThisTimeline + contractId
              ),
          )
        // if we have a contract key being added, include it in the list of
        // active keys
        mbKey match {
          case None => Right(me)
          case Some(gk) =>
            val conflict = lookupActiveKey(gk).exists(_ != KeyInactive)
            val newKeyInputs =
              if (globalKeyInputs.contains(gk)) globalKeyInputs
              else globalKeyInputs.updated(gk, KeyCreate)
            Either.cond(
              !conflict,
              me.copy(
                activeState = me.activeState.createKey(gk, contractId),
                globalKeyInputs = newKeyInputs,
              ),
              CreateError.inject(DuplicateContractKey(gk)),
            )
        }
      }
    }

    def handleExercise(nid: Nid, exe: Node.Exercise): Either[KeyInputError, State[Nid]] =
      visitExercise(
        nid,
        exe.targetCoid,
        exe.gkeyOpt,
        exe.byKey,
        exe.consuming,
      ).left.map(KeyInputError.inject)

    private[lf] override def visitExercise(
        nodeId: Nid,
        targetId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
        consuming: Boolean,
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = witnessContractId(targetId)
      for {
        state <- state.assertKeyMapping(targetId, mbKey)
      } yield {
        if (consuming) {
          val consumedState = state.activeState.consume(targetId, nodeId)
          state.copy(activeState = consumedState)
        } else state
      }
    }

    @VisibleForTesting
    private[transaction] override def handleLookup(
        lookup: Node.LookupByKey
    ): Either[KeyInputError, State[Nid]] =
      visitLookup(lookup.gkey, lookup.result, lookup.result).left.map(KeyInputError.inject)

    override private[lf] def visitLookup(
        gk: GlobalKey,
        keyInput: Option[ContractId],
        keyResolution: Option[ContractId],
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = keyInput match {
        case Some(contractId) => witnessContractId(contractId)
        case None => this
      }
      val (keyMapping, next) = state.resolveKey(gk) match {
        case Right(result) => result
        case Left(handle) => handle(keyInput)
      }
      Either.cond(
        keyMapping == keyResolution,
        next,
        InconsistentContractKey(gk),
      )
    }

    override private[lf] def resolveKey(
        gkey: GlobalKey
    ): Either[Option[
      ContractId
    ] => (KeyMapping, UniqueContractKeyStateWithRollback[Nid]), (KeyMapping, UniqueContractKeyStateWithRollback[Nid])] = {
      lookupActiveKey(gkey) match {
        case Some(keyMapping) => Right(keyMapping -> this)
        case None =>
          // if we cannot find it here, send help, and make sure to update keys after
          // that.
          def handleResult(
              result: Option[ContractId]
          ): (KeyMapping, UniqueContractKeyStateWithRollback[Nid]) = {
            // Update key inputs. Create nodes never call this method,
            // so NegativeKeyLookup is the right choice for the global key input.
            val keyInput = result match {
              case None => NegativeKeyLookup
              case Some(cid) => Transaction.KeyActive(cid)
            }
            val newKeyInputs = globalKeyInputs.updated(gkey, keyInput)
            val state = this.copy(globalKeyInputs = newKeyInputs)
            result match {
              case Some(cid) if !activeState.consumedBy.contains(cid) =>
                KeyActive(cid) -> state
              case _ =>
                KeyInactive -> state
            }
          }
          Left(handleResult)
      }
    }

    @VisibleForTesting
    override private[transaction] def handleFetch(
        node: Node.Fetch
    ): Either[KeyInputError, State[Nid]] =
      visitFetch(node.coid, node.gkeyOpt, node.byKey).left.map(KeyInputError.inject)

    override private[lf] def visitFetch(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = witnessContractId(contractId)
      state.assertKeyMapping(contractId, mbKey)
    }

    private def witnessContractId(contractId: ContractId): UniqueContractKeyStateWithRollback[Nid] =
      if (locallyCreated.contains(contractId)) this
      else this.copy(inputContractIds = inputContractIds + contractId)

    private def assertKeyMapping(
        cid: Value.ContractId,
        mbKey: Option[GlobalKey],
    ): Either[InconsistentContractKey, UniqueContractKeyStateWithRollback[Nid]] =
      mbKey match {
        case None => Right(this)
        case Some(gk) =>
          val (keyMapping, next) = resolveKey(gk) match {
            case Right(result) => result
            case Left(handle) => handle(Some(cid))
          }
          // Since keys is defined only where keyInputs is defined, we don't need to update keyInputs.
          Either.cond(keyMapping == KeyActive(cid), next, InconsistentContractKey(gk))
      }

    override def handleNode(
        id: Nid,
        node: Node.Action,
        keyInput: => Option[ContractId],
    ): Either[KeyInputError, State[Nid]] = node match {
      case create: Node.Create => handleCreate(create)
      case fetch: Node.Fetch => handleFetch(fetch)
      case lookup: Node.LookupByKey => handleLookup(lookup)
      case exercise: Node.Exercise => handleExercise(id, exercise)
    }

    override def beginRollback(): State[Nid] =
      this.copy(rollbackStack = activeState :: rollbackStack)

    override def endRollback(): State[Nid] = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case headState :: tailStack => this.copy(activeState = headState, rollbackStack = tailStack)
    }

    override private[lf] def dropRollback(): State[Nid] = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case _ :: tailStack => this.copy(rollbackStack = tailStack)
    }

    override private[transaction] def withinRollbackScope: Boolean = rollbackStack.nonEmpty

    override def advance(
        resolver: KeyResolver,
        substate_ : State[Nid],
    ): Either[KeyInputError, State[Nid]] = {
      val substate = substate_ match {
        case x: UniqueContractKeyStateWithRollback[_] => x
        case _ =>
          throw new IllegalArgumentException(
            s"Expected substate of type UniqueContractKeyState, but got ${substate_.getClass}"
          )
      }

      require(
        !substate.withinRollbackScope,
        "Cannot lift a state over a substate with unfinished rollback scopes",
      )

      // We want consistent key lookups within an action in any contract key mode.
      def consistentGlobalKeyInputs: Either[KeyInputError, Unit] = {
        substate.locallyCreated.find(locallyCreated.union(inputContractIds).contains) match {
          case Some(contractId) =>
            Left[KeyInputError, Unit](KeyInputError.inject(DuplicateContractId(contractId)))
          case None =>
            substate.globalKeyInputs
              .find {
                case (key, KeyCreate) => lookupActiveKey(key).exists(_ != KeyInactive)
                case (key, NegativeKeyLookup) => lookupActiveKey(key).exists(_ != KeyInactive)
                case (key, Transaction.KeyActive(cid)) =>
                  lookupActiveKey(key).exists(_ != KeyActive(cid))
                case _ => false
              } match {
              case Some((key, KeyCreate)) =>
                Left[KeyInputError, Unit](KeyInputError.inject(DuplicateContractKey(key)))
              case Some((key, NegativeKeyLookup)) =>
                Left[KeyInputError, Unit](KeyInputError.inject(InconsistentContractKey(key)))
              case Some((key, Transaction.KeyActive(_))) =>
                Left[KeyInputError, Unit](KeyInputError.inject(InconsistentContractKey(key)))
              case _ => Right[KeyInputError, Unit](())
            }
        }
      }

      for {
        _ <- consistentGlobalKeyInputs
      } yield {
        val next = this.activeState.advance(substate.activeState)
        val globalKeyInputs =
          // In strict mode, `key`'s state is the same at `this` as at the beginning
          // if `key` is not in `this.globalKeyInputs`.
          // So just extend `this.globalKeyInputs` with the new stuff.
          substate.globalKeyInputs ++ this.globalKeyInputs

        this.copy(
          locallyCreated = this.locallyCreated.union(substate.locallyCreated),
          inputContractIds =
            this.inputContractIds.union(substate.inputContractIds.diff(this.locallyCreated)),
          globalKeyInputs = globalKeyInputs,
          activeState = next,
        )
      }
    }

    override def projectKeyResolver(resolver: KeyResolver): KeyResolver = {
      val consumed = activeState.consumedBy.keySet
      resolver.map { case (key, keyMapping) =>
        val newKeyInput = activeState.getLocalActiveKey(key) match {
          case None => keyMapping.filterNot(consumed.contains)
          case Some(localMapping) => localMapping
        }
        key -> newKeyInput
      }
    }

    override private[lf] def withLocalContractKey(
        contractId: ContractId,
        key: GlobalKey,
    ): UniqueContractKeyStateWithRollback[Nid] =
      this.copy(
        locallyCreated = locallyCreated + contractId,
        activeState = activeState.createKey(key, contractId),
      )
  }

  final case class UniqueContractKeyStateWithoutRollback[Nid] private[lf] (
      override val locallyCreated: Set[ContractId],
      override val inputContractIds: Set[ContractId],
      override val globalKeyInputs: Map[GlobalKey, KeyInput],
      consumedBy: Map[ContractId, Nid],
      localKeys: Map[GlobalKey, Value.ContractId],
      rollbackStack: List[UniqueContractKeyStateWithoutRollback[Nid]],
  ) extends State[Nid] {

    override def consumedByOrInactive(cid: Value.ContractId): Option[Left[Nid, Nothing]] = {
      consumedBy.get(cid) match {
        case Some(nid) => Some(Left(nid)) // consumed
        case None => None
      }
    }

    override def mode: Mode = Mode.UCKWithoutRollback

    def getLocalActiveKey(key: GlobalKey): Option[KeyMapping] =
      localKeys.get(key) match {
        case None => None
        case Some(cid) =>
          Some(if (consumedBy.contains(cid)) KeyInactive else KeyActive(cid))
      }

    private def lookupActiveKey(key: GlobalKey): Option[ContractStateMachine.KeyMapping] =
      getLocalActiveKey(key).orElse(lookupActiveGlobalKeyInput(key))

    private def lookupActiveGlobalKeyInput(
        key: GlobalKey
    ): Option[ContractStateMachine.KeyMapping] =
      globalKeyInputs.get(key).map {
        case Transaction.KeyActive(cid) if !consumedBy.contains(cid) =>
          ContractStateMachine.KeyActive(cid)
        case _ =>
          ContractStateMachine.KeyInactive
      }

    /** Visit a create node */
    private def handleCreate(node: Node.Create): Either[KeyInputError, State[Nid]] =
      visitCreate(node.coid, node.gkeyOpt).left.map(KeyInputError.from)

    private[lf] def visitCreate(
        cid: ContractId,
        mbKey: Option[GlobalKey],
    ): Either[CreateError, State[Nid]] = {
      if (locallyCreated(cid) || inputContractIds(cid)) {
        Left(CreateError.inject(DuplicateContractId(cid)))
      } else {
        val me = this.copy(locallyCreated = locallyCreated + cid)
        // if we have a contract key being added, include it in the list of
        // active keys
        mbKey match {
          case None =>
            Right(me)
          case Some(gk) =>
            val conflict = lookupActiveKey(gk).exists(_ != KeyInactive)
            val newKeyInputs =
              if (globalKeyInputs.contains(gk)) globalKeyInputs
              else globalKeyInputs.updated(gk, KeyCreate)
            Either.cond(
              !conflict,
              me.copy(
                localKeys = localKeys.updated(gk, cid),
                globalKeyInputs = newKeyInputs,
              ),
              CreateError.inject(DuplicateContractKey(gk)),
            )
        }
      }
    }

    def handleExercise(nid: Nid, exe: Node.Exercise): Either[KeyInputError, State[Nid]] =
      visitExercise(
        nid,
        exe.targetCoid,
        exe.gkeyOpt,
        exe.byKey,
        exe.consuming,
      ).left.map(KeyInputError.inject)

    private[lf] override def visitExercise(
        nodeId: Nid,
        cid: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
        consuming: Boolean,
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = witnessContractId(cid)
      for {
        state <- state.assertKeyMapping(cid, mbKey)
      } yield {
        if (consuming) {
          state.copy(consumedBy = consumedBy.updated(cid, nodeId))
        } else {
          state
        }
      }
    }

    @VisibleForTesting
    private[transaction] override def handleLookup(
        lookup: Node.LookupByKey
    ): Either[KeyInputError, State[Nid]] =
      visitLookup(lookup.gkey, lookup.result, lookup.result).left.map(KeyInputError.inject)

    override private[lf] def visitLookup(
        gk: GlobalKey,
        keyInput: Option[ContractId],
        keyResolution: Option[ContractId],
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = keyInput match {
        case Some(contractId) => witnessContractId(contractId)
        case None => this
      }
      val (keyMapping, next) = state.resolveKey(gk) match {
        case Right(result) => result
        case Left(handle) => handle(keyInput)
      }
      Either.cond(
        keyMapping == keyResolution,
        next,
        InconsistentContractKey(gk),
      )
    }

    override private[lf] def resolveKey(
        gkey: GlobalKey
    ): Either[Option[
      ContractId
    ] => (KeyMapping, UniqueContractKeyStateWithoutRollback[Nid]), (KeyMapping, UniqueContractKeyStateWithoutRollback[Nid])] = {
      lookupActiveKey(gkey) match {
        case Some(keyMapping) => Right(keyMapping -> this)
        case None =>
          // if we cannot find it here, send help, and make sure to update keys after
          // that.
          def handleResult(
              result: Option[ContractId]
          ): (KeyMapping, UniqueContractKeyStateWithoutRollback[Nid]) = {
            // Update key inputs. Create nodes never call this method,
            // so NegativeKeyLookup is the right choice for the global key input.
            val keyInput = result match {
              case None => NegativeKeyLookup
              case Some(cid) => Transaction.KeyActive(cid)
            }
            val newKeyInputs = globalKeyInputs.updated(gkey, keyInput)
            val state = this.copy(globalKeyInputs = newKeyInputs)
            result match {
              case Some(cid) if !consumedBy.contains(cid) =>
                KeyActive(cid) -> state
              case _ =>
                KeyInactive -> state
            }
          }
          Left(handleResult)
      }
    }

    @VisibleForTesting
    override private[transaction] def handleFetch(
        node: Node.Fetch
    ): Either[KeyInputError, State[Nid]] =
      visitFetch(node.coid, node.gkeyOpt, node.byKey).left.map(KeyInputError.inject)

    override private[lf] def visitFetch(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = witnessContractId(contractId)
      state.assertKeyMapping(contractId, mbKey)
    }

    private def witnessContractId(
        contractId: ContractId
    ): UniqueContractKeyStateWithoutRollback[Nid] =
      if (locallyCreated.contains(contractId)) this
      else this.copy(inputContractIds = inputContractIds + contractId)

    private def assertKeyMapping(
        cid: Value.ContractId,
        mbKey: Option[GlobalKey],
    ): Either[InconsistentContractKey, UniqueContractKeyStateWithoutRollback[Nid]] =
      mbKey match {
        case None => Right(this)
        case Some(gk) =>
          val (keyMapping, next) = resolveKey(gk) match {
            case Right(result) => result
            case Left(handle) => handle(Some(cid))
          }
          // Since keys is defined only where keyInputs is defined, we don't need to update keyInputs.
          Either.cond(keyMapping == KeyActive(cid), next, InconsistentContractKey(gk))
      }

    override def handleNode(
        id: Nid,
        node: Node.Action,
        keyInput: => Option[ContractId],
    ): Either[KeyInputError, State[Nid]] = node match {
      case create: Node.Create => handleCreate(create)
      case fetch: Node.Fetch => handleFetch(fetch)
      case lookup: Node.LookupByKey => handleLookup(lookup)
      case exercise: Node.Exercise => handleExercise(id, exercise)
    }

    override def beginRollback(): State[Nid] =
      this.copy(rollbackStack = this :: rollbackStack)

    override def endRollback(): State[Nid] = rollbackStack match {
      case Nil =>
        throw new IllegalStateException("Not inside a rollback scope")
      case headState :: tailStack =>
        // locallyCreated and consumedBy increase monotonically, so can quickly check there size did not change since the last try block.
        if (locallyCreated.size != headState.locallyCreated.size) {
          throw new IllegalStateException("Rollback of create node is not supported")
        } else if (consumedBy.size != headState.consumedBy.size) {
          throw new IllegalStateException("Rollback of consuming exercise node is not supported")
        } else {
          this.copy(rollbackStack = tailStack)
        }
    }

    override private[lf] def dropRollback(): State[Nid] = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case _ :: tailStack => this.copy(rollbackStack = tailStack)
    }

    override private[transaction] def withinRollbackScope: Boolean = rollbackStack.nonEmpty

    override def advance(
        resolver: KeyResolver,
        substate_ : State[Nid],
    ): Either[KeyInputError, State[Nid]] = {
      val substate = substate_ match {
        case x: UniqueContractKeyStateWithoutRollback[_] => x
        case _ =>
          throw new IllegalArgumentException(
            s"Expected substate of type UniqueContractKeyState, but got ${substate_.getClass}"
          )
      }
      require(
        !substate.withinRollbackScope,
        "Cannot lift a state over a substate with unfinished rollback scopes",
      )

      // We want consistent key lookups within an action in any contract key mode.
      def consistentGlobalKeyInputs: Either[KeyInputError, Unit] =
        substate.locallyCreated.find(cid => locallyCreated(cid) || inputContractIds(cid)) match {
          case Some(contractId) =>
            Left[KeyInputError, Unit](KeyInputError.inject(DuplicateContractId(contractId)))
          case None =>
            substate.globalKeyInputs
              .collectFirst {
                case (key, KeyCreate) if lookupActiveKey(key).exists(_ != KeyInactive) =>
                  KeyInputError.inject(DuplicateContractKey(key))
                case (key, NegativeKeyLookup) if lookupActiveKey(key).exists(_ != KeyInactive) =>
                  KeyInputError.inject(InconsistentContractKey(key))
                case (key, Transaction.KeyActive(cid))
                    if lookupActiveKey(key).exists(_ != KeyActive(cid)) =>
                  KeyInputError.inject(InconsistentContractKey(key))
              }
              .toLeft(())
        }

      for {
        _ <- consistentGlobalKeyInputs
      } yield {
        this.copy(
          locallyCreated = this.locallyCreated union substate.locallyCreated,
          inputContractIds = this.inputContractIds union (substate.inputContractIds diff this.locallyCreated),
          globalKeyInputs = substate.globalKeyInputs ++ this.globalKeyInputs,
          consumedBy = this.consumedBy ++ substate.consumedBy,
          localKeys = this.localKeys ++ substate.localKeys,
        )
      }
    }

    override def projectKeyResolver(resolver: KeyResolver): KeyResolver = {
      val consumed = consumedBy.keySet
      resolver.map { case (key, keyMapping) =>
        val newKeyInput = getLocalActiveKey(key) match {
          case None => keyMapping.filterNot(consumed.contains)
          case Some(localMapping) => localMapping
        }
        key -> newKeyInput
      }
    }

    override private[lf] def withLocalContractKey(
        contractId: ContractId,
        key: GlobalKey,
    ): UniqueContractKeyStateWithoutRollback[Nid] =
      this.copy(
        locallyCreated = locallyCreated + contractId,
        localKeys = localKeys.updated(key, contractId),
      )
  }

  final case class NonUniqueContractKeyState[Nid] private[lf] (
      override val locallyCreated: Set[ContractId],
      override val inputContractIds: Set[ContractId],
      override val globalKeyInputs: Map[GlobalKey, KeyInput],
      activeState: ContractStateMachine.ActiveLedgerState[Nid],
      rollbackStack: List[ContractStateMachine.ActiveLedgerState[Nid]],
  ) extends State[Nid] {

    override val mode: Mode = Mode.LegacyNUCK

    override def consumedByOrInactive(cid: Value.ContractId): Option[Either[Nid, Unit]] = {
      activeState.consumedBy.get(cid) match {
        case Some(nid) => Some(Left(nid)) // consumed
        case None =>
          if (locallyCreated(cid) && !activeState.locallyCreatedThisTimeline.contains(cid)) {
            Some(Right(())) // inactive
          } else {
            None // neither
          }
      }
    }

    private def lookupActiveKey(key: GlobalKey): Option[ContractStateMachine.KeyMapping] =
      activeState.getLocalActiveKey(key).orElse(lookupActiveGlobalKeyInput(key))

    private def lookupActiveGlobalKeyInput(
        key: GlobalKey
    ): Option[ContractStateMachine.KeyMapping] =
      globalKeyInputs.get(key).map {
        case Transaction.KeyActive(cid) if !activeState.consumedBy.contains(cid) =>
          ContractStateMachine.KeyActive(cid)
        case _ => ContractStateMachine.KeyInactive
      }

    /** Visit a create node */
    private def handleCreate(node: Node.Create): Either[KeyInputError, State[Nid]] =
      visitCreate(node.coid, node.gkeyOpt).left.map(KeyInputError.from)

    private[lf] def visitCreate(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
    ): Either[CreateError, State[Nid]] = {
      if (locallyCreated.union(inputContractIds).contains(contractId)) {
        Left(CreateError.inject(DuplicateContractId(contractId)))
      } else {
        val me =
          this.copy(
            locallyCreated = locallyCreated + contractId,
            activeState = this.activeState
              .copy(locallyCreatedThisTimeline =
                this.activeState.locallyCreatedThisTimeline + contractId
              ),
          )
        // if we have a contract key being added, include it in the list of
        // active keys
        mbKey match {
          case None => Right(me)
          case Some(gk) =>
            val newKeyInputs =
              if (globalKeyInputs.contains(gk)) globalKeyInputs
              else globalKeyInputs.updated(gk, KeyCreate)
            Right(
              me.copy(
                activeState = me.activeState.createKey(gk, contractId),
                globalKeyInputs = newKeyInputs,
              )
            )
        }
      }
    }

    def handleExercise(nid: Nid, exe: Node.Exercise): Either[KeyInputError, State[Nid]] =
      visitExercise(
        nid,
        exe.targetCoid,
        exe.gkeyOpt,
        exe.byKey,
        exe.consuming,
      ).left.map(KeyInputError.inject)

    private[lf] override def visitExercise(
        nodeId: Nid,
        targetId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
        consuming: Boolean,
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = witnessContractId(targetId)
      for {
        state <-
          if (byKey)
            state.assertKeyMapping(targetId, mbKey)
          else
            Right(state)
      } yield {
        if (consuming) {
          val consumedState = state.activeState.consume(targetId, nodeId)
          state.copy(activeState = consumedState)
        } else state
      }
    }

    @VisibleForTesting
    private[transaction] override def handleLookup(
        lookup: Node.LookupByKey
    ): Either[KeyInputError, State[Nid]] =
      // If the key has not yet been resolved, we use the resolution from the lookup node,
      // but this only makes sense if `activeState.keys` is updated by every node and not only by by-key nodes.
      throw new UnsupportedOperationException(
        "handleLookup can only be used if all key nodes are considered"
      )

    private def handleLookupWith(
        lookup: Node.LookupByKey,
        keyInput: Option[ContractId],
    ): Either[KeyInputError, State[Nid]] =
      visitLookup(lookup.gkey, keyInput, lookup.result).left.map(KeyInputError.inject)

    override private[lf] def visitLookup(
        gk: GlobalKey,
        keyInput: Option[ContractId],
        keyResolution: Option[ContractId],
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = keyInput match {
        case Some(contractId) => witnessContractId(contractId)
        case None => this
      }
      val (keyMapping, next) = state.resolveKey(gk) match {
        case Right(result) => result
        case Left(handle) => handle(keyInput)
      }
      Either.cond(
        keyMapping == keyResolution,
        next,
        InconsistentContractKey(gk),
      )
    }

    override private[lf] def resolveKey(
        gkey: GlobalKey
    ): Either[Option[
      ContractId
    ] => (KeyMapping, NonUniqueContractKeyState[Nid]), (KeyMapping, NonUniqueContractKeyState[Nid])] = {
      lookupActiveKey(gkey) match {
        case Some(keyMapping) => Right(keyMapping -> this)
        case None =>
          // if we cannot find it here, send help, and make sure to update keys after
          // that.
          def handleResult(
              result: Option[ContractId]
          ): (KeyMapping, NonUniqueContractKeyState[Nid]) = {
            // Update key inputs. Create nodes never call this method,
            // so NegativeKeyLookup is the right choice for the global key input.
            val keyInput = result match {
              case None => NegativeKeyLookup
              case Some(cid) => Transaction.KeyActive(cid)
            }
            val newKeyInputs = globalKeyInputs.updated(gkey, keyInput)
            val state = this.copy(globalKeyInputs = newKeyInputs)
            result match {
              case Some(cid) if !activeState.consumedBy.contains(cid) =>
                KeyActive(cid) -> state
              case _ =>
                KeyInactive -> state
            }
          }

          Left(handleResult)
      }
    }

    @VisibleForTesting
    override private[transaction] def handleFetch(
        node: Node.Fetch
    ): Either[KeyInputError, State[Nid]] =
      visitFetch(node.coid, node.gkeyOpt, node.byKey).left.map(KeyInputError.inject)

    override private[lf] def visitFetch(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = witnessContractId(contractId)
      if (byKey)
        state.assertKeyMapping(contractId, mbKey)
      else
        Right(state)
    }

    private def witnessContractId(contractId: ContractId): NonUniqueContractKeyState[Nid] =
      if (locallyCreated.contains(contractId)) this
      else this.copy(inputContractIds = inputContractIds + contractId)

    private def assertKeyMapping(
        cid: Value.ContractId,
        mbKey: Option[GlobalKey],
    ): Either[InconsistentContractKey, NonUniqueContractKeyState[Nid]] =
      mbKey match {
        case None => Right(this)
        case Some(gk) =>
          val (keyMapping, next) = resolveKey(gk) match {
            case Right(result) => result
            case Left(handle) => handle(Some(cid))
          }
          // Since keys is defined only where keyInputs is defined, we don't need to update keyInputs.
          Either.cond(keyMapping == KeyActive(cid), next, InconsistentContractKey(gk))
      }

    override def handleNode(
        id: Nid,
        node: Node.Action,
        keyInput: => Option[ContractId],
    ): Either[KeyInputError, State[Nid]] = node match {
      case create: Node.Create => handleCreate(create)
      case fetch: Node.Fetch => handleFetch(fetch)
      case lookup: Node.LookupByKey => handleLookupWith(lookup, keyInput)
      case exercise: Node.Exercise => handleExercise(id, exercise)
    }

    override def beginRollback(): State[Nid] =
      this.copy(rollbackStack = activeState :: rollbackStack)

    override def endRollback(): State[Nid] = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case headState :: tailStack => this.copy(activeState = headState, rollbackStack = tailStack)
    }

    override private[lf] def dropRollback(): State[Nid] = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case _ :: tailStack => this.copy(rollbackStack = tailStack)
    }

    override private[transaction] def withinRollbackScope: Boolean = rollbackStack.nonEmpty

    override def advance(
        resolver: KeyResolver,
        substate_ : State[Nid],
    ): Either[KeyInputError, State[Nid]] = {
      val substate = substate_ match {
        case x: NonUniqueContractKeyState[_] => x
        case _ =>
          throw new IllegalArgumentException(
            s"Expected substate of type UniqueContractKeyState, but got ${substate_.getClass}"
          )
      }

      require(
        !substate.withinRollbackScope,
        "Cannot lift a state over a substate with unfinished rollback scopes",
      )

      // We want consistent key lookups within an action in any contract key mode.
      def consistentGlobalKeyInputs: Either[KeyInputError, Unit] = {
        substate.locallyCreated.find(locallyCreated.union(inputContractIds).contains) match {
          case Some(contractId) =>
            Left[KeyInputError, Unit](KeyInputError.inject(DuplicateContractId(contractId)))
          case None =>
            substate.globalKeyInputs
              .find {
                case (key, NegativeKeyLookup) => lookupActiveKey(key).exists(_ != KeyInactive)
                case (key, Transaction.KeyActive(cid)) =>
                  lookupActiveKey(key).exists(_ != KeyActive(cid))
                case _ => false
              } match {
              case Some((key, KeyCreate)) =>
                Left[KeyInputError, Unit](KeyInputError.inject(DuplicateContractKey(key)))
              case Some((key, NegativeKeyLookup)) =>
                Left[KeyInputError, Unit](KeyInputError.inject(InconsistentContractKey(key)))
              case Some((key, Transaction.KeyActive(_))) =>
                Left[KeyInputError, Unit](KeyInputError.inject(InconsistentContractKey(key)))
              case _ => Right[KeyInputError, Unit](())
            }
        }
      }

      for {
        _ <- consistentGlobalKeyInputs
      } yield {
        val next = this.activeState.advance(substate.activeState)
        val globalKeyInputs =
          substate.globalKeyInputs.foldLeft(this.globalKeyInputs) { case (acc, (key, keyInput)) =>
            if (acc.contains(key)) acc
            else {
              val resolution = keyInput match {
                case KeyCreate =>
                  // A create brought the contract key in scope without querying a resolver.
                  // So the global key input for `key` does not depend on the resolver.
                  KeyCreate
                case NegativeKeyLookup =>
                  // A lookup-by-key brought the key in scope. Use the resolver's resolution instead
                  // as the projected resolver's resolution might have been mapped to None.
                  resolver(key) match {
                    case None => keyInput
                    case Some(cid) => Transaction.KeyActive(cid)
                  }
                case active: Transaction.KeyActive =>
                  active
              }
              acc.updated(key, resolution)
            }
          }

        this.copy(
          locallyCreated = this.locallyCreated.union(substate.locallyCreated),
          inputContractIds =
            this.inputContractIds.union(substate.inputContractIds.diff(this.locallyCreated)),
          globalKeyInputs = globalKeyInputs,
          activeState = next,
        )
      }
    }

    override def projectKeyResolver(resolver: KeyResolver): KeyResolver = {
      val consumed = activeState.consumedBy.keySet
      resolver.map { case (key, keyMapping) =>
        val newKeyInput = activeState.getLocalActiveKey(key) match {
          case None => keyMapping.filterNot(consumed.contains)
          case Some(localMapping) => localMapping
        }
        key -> newKeyInput
      }
    }

    override private[lf] def withLocalContractKey(
        contractId: ContractId,
        key: GlobalKey,
    ): NonUniqueContractKeyState[Nid] =
      this.copy(
        locallyCreated = locallyCreated + contractId,
        activeState = activeState.createKey(key, contractId),
      )
  }


  object NoContractKeyState{
    case class ActiveLedgerState[Nid](
                                       locallyCreatedThisTimeline: Set[ContractId],
                                       consumedBy: Map[ContractId, Nid],
                                     ) {

      private[lf] def advance(
                               substate: ActiveLedgerState[Nid]
                             ): ActiveLedgerState[Nid] =
        ActiveLedgerState(
          locallyCreatedThisTimeline = this.locallyCreatedThisTimeline
            .union(substate.locallyCreatedThisTimeline),
          consumedBy = this.consumedBy ++ substate.consumedBy,
        )

    }

    object ActiveLedgerState {
      def empty[Nid]: ActiveLedgerState[Nid] = ActiveLedgerState(Set.empty, Map.empty)
    }
  }

  final case class NoContractKeyState[Nid] private[lf] (
    override val locallyCreated: Set[ContractId],
    override val inputContractIds: Set[ContractId],
    activeState: NoContractKeyState.ActiveLedgerState[Nid],
    rollbackStack: List[NoContractKeyState.ActiveLedgerState[Nid]],
  ) extends State[Nid] {

    override def globalKeyInputs: Map[GlobalKey, KeyInput] = Map.empty

    private[this] def keyOperationError: Nothing = throw new UnsupportedOperationException(
      "by-key operation are not supported by NoContractKeyMachine"
    )

    override def consumedByOrInactive(cid: Value.ContractId): Option[Either[Nid, Unit]] = {
      activeState.consumedBy.get(cid) match {
        case Some(nid) => Some(Left(nid)) // consumed
        case None =>
          if (locallyCreated(cid) && !activeState.locallyCreatedThisTimeline.contains(cid)) {
            Some(Right(())) // inactive
          } else {
            None // neither
          }
      }
    }

    override val mode: Mode = Mode.NoContractKey

    private def lookupActiveKey(key: GlobalKey): Option[ContractStateMachine.KeyMapping] =
      keyOperationError

    /** Visit a create node */
    private def handleCreate(node: Node.Create): Either[KeyInputError, State[Nid]] =
      visitCreate(node.coid, node.gkeyOpt).left.map(KeyInputError.from)

    private[lf] def visitCreate(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
    ): Either[CreateError, State[Nid]] = {
      mbKey.foreach(_ => keyOperationError)
      if (locallyCreated.union(inputContractIds).contains(contractId)) {
        Left(CreateError.inject(DuplicateContractId(contractId)))
      } else {
        val me =
          this.copy(
            locallyCreated = locallyCreated + contractId,
            activeState = this.activeState
              .copy(locallyCreatedThisTimeline =
                this.activeState.locallyCreatedThisTimeline + contractId
              ),
          )
        Right(me)
      }
    }

    def handleExercise(nid: Nid, exe: Node.Exercise): Either[KeyInputError, State[Nid]] =
      visitExercise(
        nid,
        exe.targetCoid,
        exe.gkeyOpt,
        exe.byKey,
        exe.consuming,
      ).left.map(KeyInputError.inject)

    private[lf] override def visitExercise(
        nodeId: Nid,
        targetId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
        consuming: Boolean,
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = witnessContractId(targetId)
      for {
        state <-
          if (byKey) keyOperationError else Right(state)
      } yield {
        if (consuming) {
          state.copy(activeState =
            state.activeState.copy(consumedBy = state.activeState.consumedBy.updated(targetId, nodeId))
          )
        } else state
      }
    }

    @VisibleForTesting
    private[transaction] override def handleLookup(
        lookup: Node.LookupByKey
    ): Either[KeyInputError, State[Nid]] =
      // If the key has not yet been resolved, we use the resolution from the lookup node,
      // but this only makes sense if `activeState.keys` is updated by every node and not only by by-key nodes.
      throw new UnsupportedOperationException(
        "handleLookup can only be used if all key nodes are considered"
      )

    override private[lf] def visitLookup(
        gk: GlobalKey,
        keyInput: Option[ContractId],
        keyResolution: Option[ContractId],
    ): Nothing =
      keyOperationError

    override private[lf] def resolveKey(
        gkey: GlobalKey
    ): Nothing = keyOperationError

    @VisibleForTesting
    override private[transaction] def handleFetch(
        node: Node.Fetch
    ): Either[KeyInputError, State[Nid]] =
      visitFetch(node.coid, node.gkeyOpt, node.byKey).left.map(KeyInputError.inject)

    override private[lf] def visitFetch(
        contractId: ContractId,
        mbKey: Option[GlobalKey],
        byKey: Boolean,
    ): Either[InconsistentContractKey, State[Nid]] = {
      val state = witnessContractId(contractId)
      if (byKey)
        keyOperationError
      else
        Right(state)
    }

    private def witnessContractId(contractId: ContractId): NoContractKeyState[Nid] =
      if (locallyCreated.contains(contractId)) this
      else this.copy(inputContractIds = inputContractIds + contractId)

    override def handleNode(
        id: Nid,
        node: Node.Action,
        keyInput: => Option[ContractId],
    ): Either[KeyInputError, State[Nid]] = node match {
      case create: Node.Create => handleCreate(create)
      case fetch: Node.Fetch => handleFetch(fetch)
      case _: Node.LookupByKey => keyOperationError
      case exercise: Node.Exercise => handleExercise(id, exercise)
    }

    override def beginRollback(): State[Nid] =
      this.copy(rollbackStack = activeState :: rollbackStack)

    override def endRollback(): State[Nid] = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case headState :: tailStack => this.copy(activeState = headState, rollbackStack = tailStack)
    }

    override private[lf] def dropRollback(): State[Nid] = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case _ :: tailStack => this.copy(rollbackStack = tailStack)
    }

    override private[transaction] def withinRollbackScope: Boolean = rollbackStack.nonEmpty

    override def advance(
        resolver: KeyResolver,
        substate_ : State[Nid],
    ): Either[KeyInputError, State[Nid]] = {

      val substate = substate_ match {
        case x: NoContractKeyState[_] => x
        case _ =>
          throw new IllegalArgumentException(
            s"Expected substate of type UniqueContractKeyState, but got ${substate_.getClass}"
          )
      }

      require(
        !substate.withinRollbackScope,
        "Cannot lift a state over a substate with unfinished rollback scopes",
      )

      // We want consistent key lookups within an action in any contract key mode.
      def consistentGlobalKeyInputs: Either[KeyInputError, Unit] = {
        substate.locallyCreated.find(locallyCreated.union(inputContractIds).contains) match {
          case Some(contractId) =>
            Left[KeyInputError, Unit](KeyInputError.inject(DuplicateContractId(contractId)))
          case None =>
            substate.globalKeyInputs
              .find {
                case (key, NegativeKeyLookup) => lookupActiveKey(key).exists(_ != KeyInactive)
                case (key, Transaction.KeyActive(cid)) =>
                  lookupActiveKey(key).exists(_ != KeyActive(cid))
                case _ => false
              } match {
              case Some((key, KeyCreate)) =>
                Left[KeyInputError, Unit](KeyInputError.inject(DuplicateContractKey(key)))
              case Some((key, NegativeKeyLookup)) =>
                Left[KeyInputError, Unit](KeyInputError.inject(InconsistentContractKey(key)))
              case Some((key, Transaction.KeyActive(_))) =>
                Left[KeyInputError, Unit](KeyInputError.inject(InconsistentContractKey(key)))
              case _ => Right[KeyInputError, Unit](())
            }
        }
      }

      for {
        _ <- consistentGlobalKeyInputs
      } yield {
        val next = this.activeState.advance(substate.activeState)
        this.copy(
          locallyCreated = this.locallyCreated.union(substate.locallyCreated),
          inputContractIds =
            this.inputContractIds.union(substate.inputContractIds.diff(this.locallyCreated)),
          activeState = next,
        )
      }
    }

    override def projectKeyResolver(resolver: KeyResolver): KeyResolver =
      keyOperationError

    private[lf] def withLocalContractKey(
        contractId: com.digitalasset.daml.lf.value.Value.ContractId,
        key: com.digitalasset.daml.lf.transaction.GlobalKey,
    ): Nothing =
      keyOperationError

  }

  object State {
    def empty[Nid](
                    mode: Mode
                  ): State[Nid] =
      mode match {
        case Mode.UCKWithRollback =>
          new UniqueContractKeyStateWithRollback(
            locallyCreated = Set.empty,
            inputContractIds = Set.empty,
            globalKeyInputs = Map.empty,
            activeState = ContractStateMachine.ActiveLedgerState.empty,
            rollbackStack = List.empty,
          )
        case Mode.UCKWithoutRollback =>
          new UniqueContractKeyStateWithoutRollback(
            locallyCreated = Set.empty,
            inputContractIds = Set.empty,
            globalKeyInputs = Map.empty,
            consumedBy = Map.empty,
            localKeys = Map.empty,
            rollbackStack = List.empty,
          )
        case Mode.LegacyNUCK =>
          new NonUniqueContractKeyState(
            Set.empty,
            Set.empty,
            Map.empty,
            ContractStateMachine.ActiveLedgerState.empty,
            List.empty,
          )
        case Mode.NoContractKey =>
          new NoContractKeyState(
            Set.empty,
            Set.empty,
            NoContractKeyState.ActiveLedgerState.empty,
            List.empty,
          )
      }
  }

  def initial[Nid](mode: Mode): State[Nid] = State.empty(mode)

  /** Represents the answers for [[com.digitalasset.daml.lf.engine.ResultNeedKey]] requests
    * that may arise during Daml interpretation.
    */
  type KeyResolver = Map[GlobalKey, KeyMapping]

  type KeyMapping = Option[Value.ContractId]

  /** There is no active contract with the given key. */
  val KeyInactive = None

  /** The contract with the given cid is active and has the given key. */
  val KeyActive = Some

  /** Summarizes the updates to the current ledger state by nodes up to now.
    *
    * @param locallyCreatedThisTimeline
    *   Tracks contracts created by a node processed so far that have not been rolled back.
    *   This is a subset of [[ContractStateMachine.State.locallyCreated]].
    *
    * @param consumedBy [[com.digitalasset.daml.lf.value.Value.ContractId]]s of all contracts
    *                   that have been consumed by nodes up to now.
    * @param localKeys
    *   A store of the latest local contract that has been created with the given key in this timeline.
    *   Later creates overwrite earlier ones. Note that this does not track whether the contract
    *   was consumed or not. That information is stored in consumedBy.
    *   It also _only_ includes local contracts not global contracts.
    */
  private[lf] final case class ActiveLedgerState[Nid](
      locallyCreatedThisTimeline: Set[ContractId],
      consumedBy: Map[ContractId, Nid],
      private[lf] val localKeys: Map[GlobalKey, Value.ContractId],
  ) {
    def consume(contractId: ContractId, nodeId: Nid): ActiveLedgerState[Nid] =
      this.copy(consumedBy = consumedBy.updated(contractId, nodeId))

    def createKey(key: GlobalKey, cid: Value.ContractId): ActiveLedgerState[Nid] =
      this.copy(localKeys = localKeys.updated(key, cid))

    /** Equivalence relative to locallyCreatedThisTimeline, consumedBy & localActiveKeys.
      */
    def isEquivalent(other: ActiveLedgerState[Nid]): Boolean =
      this.locallyCreatedThisTimeline == other.locallyCreatedThisTimeline &&
        this.consumedBy == other.consumedBy &&
        this.localActiveKeys == other.localActiveKeys

    /** See docs of [[ContractStateMachine.advance]]
      */
    private[lf] def advance(
        substate: ActiveLedgerState[Nid]
    ): ActiveLedgerState[Nid] =
      ActiveLedgerState(
        locallyCreatedThisTimeline = this.locallyCreatedThisTimeline
          .union(substate.locallyCreatedThisTimeline),
        consumedBy = this.consumedBy ++ substate.consumedBy,
        localKeys = this.localKeys.concat(substate.localKeys),
      )

    /** localKeys filter by whether contracts have been consumed already.
      */
    def localActiveKeys: Map[GlobalKey, KeyMapping] =
      localKeys.view
        .mapValues((v: ContractId) => if (consumedBy.contains(v)) KeyInactive else KeyActive(v))
        .toMap

    /** Lookup in localActiveKeys.
      */
    def getLocalActiveKey(key: GlobalKey): Option[KeyMapping] =
      localKeys.get(key) match {
        case None => None
        case Some(cid) =>
          Some(if (consumedBy.contains(cid)) KeyInactive else KeyActive(cid))
      }
  }

  object ActiveLedgerState {
    def empty[Nid]: ActiveLedgerState[Nid] = ActiveLedgerState(Set.empty, Map.empty, Map.empty)
  }

}
