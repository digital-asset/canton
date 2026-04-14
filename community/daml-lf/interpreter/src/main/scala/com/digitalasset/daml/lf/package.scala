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
        // TODO(#23969) check if ww want a proper IE errors instead of crashing the engine.
        //  this will be required by ContractID V2
        throw SErrorCrash(context, s"Unexpected duplicate contract ID $contractId")
      case TxErr.InconsistentContractKey(key) =>
        IE.InconsistentContractKey(key)
      case TxErr.AlreadyConsumed(cid, tmplId, nid) =>
         IE.ContractNotActive(cid, tmplId, nid)
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

  type CSMState = ContractStateMachine.LLState
}
