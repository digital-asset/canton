// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.transaction.{
  NextGenContractStateMachine => ContractStateMachine,
  Node, NodeId,
  TransactionError => TxErr,
}
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.speedy.SError.SErrorCrash

import scala.collection.immutable.HashMap

package object speedy {

  val Compiler = compiler.Compiler
  type Compiler = compiler.Compiler


  private[speedy] def convTxError(nodes: HashMap[NodeId, Node], context: => String, err: TxErr): IE = {
    err match {
      case TxErr.DuplicateContractId(contractId) =>
        // TODO(#30398) make these proper IE errors instead of crashing the engine.
        throw SErrorCrash(context, s"Unexpected duplicate contract ID $contractId")
      case TxErr.DuplicateContractKey(key) =>
        throw SErrorCrash(context, s"Unexpected duplicate key $key")
      case TxErr.InconsistentContractKey(key) =>
        IE.InconsistentContractKey(key)
      case TxErr.AlreadyConsumed(cid, _: Any) =>
        // TODO(#30398) make these proper IE errors instead of crashing the engine.
        throw SErrorCrash(context, s"Tried consuming Already consumed id ${cid}")
      case e: TxErr.EffectfulRollback =>
        convEffectfulRollbackError(nodes, e)
    }
  }

  private[speedy] def convEffectfulRollbackError(nodes: HashMap[NodeId, Node], err: TxErr.EffectfulRollback): IE.EffectfulRollback = {
    IE.EffectfulRollback(
      err.nodeIds
        .map(id => nodes(id))
        .collect {
          case n: Node.Exercise => IE.EffectfulRollback.Node.fromExercise(n)
          case n: Node.Create => IE.EffectfulRollback.Node.fromCreate(n)
        }
    )
  }

  // Continuation-passing style traverse. Defined as an implicit class to help type inference.
  private[speedy] implicit class IterableOps[X](val xs: Iterable[X]) extends AnyVal {
    def traverseK[Y, R](f: X => (Y => R) => R)(k: List[Y] => R): R = {
      val acc = List.newBuilder[Y]
      def loop(it: Iterator[X]): R =
        if (it.hasNext)
          f(it.next()) { y =>
            val _ = acc += y
            loop(it)
          }
        else
          k(acc.result())
      loop(xs.iterator)
    }
  }

  type CSMState = ContractStateMachine.LLState[NodeId]
}
