// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.runner

import com.daml.ledger.javaapi
import com.digitalasset.canton.testing.modelbased.ast.Concrete.*
import com.digitalasset.canton.testing.modelbased.conversions.ConcreteToCommands.*
import com.digitalasset.daml.lf.script.IdeLedger
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value as V

import scala.jdk.CollectionConverters.*

/** Shared utilities for extracting `ContractIdMapping`s from transaction results.
  *
  * Provides overloaded methods for both the IDE ledger (LF values / nodes) and the Java API (used
  * when submitting against a real Canton participant).
  */
object ContractIdMappings {

  // -- LF value variants (used by ReferenceInterpreter) --

  /** Parses a contract ID mapping from an LF value returned by a universal model choice.
    *
    * The value is a `GenMap[Int64, SomeContractId]` where `SomeContractId` is either
    * `UniversalContractId(ContractId)` or `UniversalWithKeyContractId(ContractId)`.
    */
  def assertContractIdMapping(value: V): ContractIdMapping =
    value match {
      case V.ValueGenMap(entries) =>
        entries.iterator
          .map[(ContractId, SomeContractId)] {
            case (
                  V.ValueInt64(contractId),
                  V.ValueVariant(_, "UniversalContractId", V.ValueContractId(cid)),
                ) =>
              contractId.toInt -> UniversalContractId(cid)
            case (
                  V.ValueInt64(contractId),
                  V.ValueVariant(_, "UniversalWithKeyContractId", V.ValueContractId(cid)),
                ) =>
              contractId.toInt -> UniversalWithKeyContractId(cid)
            case entry =>
              throw new IllegalArgumentException(
                s"assertContractIdMapping: invalid map entry $entry"
              )
          }
          .toMap
      case _ =>
        throw new IllegalArgumentException(
          s"assertContractIdMapping: expected ValueGenMap, got $value"
        )
    }

  /** Extracts the contract ID mapping from a committed IDE ledger transaction by pairing the
    * abstract actions with the root nodes of the transaction.
    *
    * For Create actions, the contract ID comes from the Create node. For Exercise/ExerciseByKey
    * actions, the mapping is parsed from the exercise result value.
    */
  def commandResultsToContractIdMapping(
      actions: List[Action],
      commitResult: IdeLedger.CommitResult,
  ): ContractIdMapping = {
    val tx = commitResult.richTransaction.transaction
    val rootNodes: List[Node] = tx.roots.toList.map(tx.nodes)
    actions
      .zip(rootNodes)
      .map {
        case (c: Create, node: Node.Create) =>
          Map[ContractId, SomeContractId](c.contractId -> UniversalContractId(node.coid))
        case (c: CreateWithKey, node: Node.Create) =>
          Map[ContractId, SomeContractId](c.contractId -> UniversalWithKeyContractId(node.coid))
        case (_: Exercise, node: Node.Exercise) =>
          node.exerciseResult
            .map(assertContractIdMapping)
            .getOrElse(Map.empty[ContractId, SomeContractId])
        case (_: ExerciseByKey, node: Node.Exercise) =>
          node.exerciseResult
            .map(assertContractIdMapping)
            .getOrElse(Map.empty[ContractId, SomeContractId])
        case (action, node) =>
          throw new IllegalArgumentException(
            s"Unexpected action/node pair: $action / $node"
          )
      }
      .fold(Map.empty[ContractId, SomeContractId])(_ ++ _)
  }

  // -- Java API variants (used by CantonInterpreter) --

  /** Parses a contract ID mapping from a Java API value returned by a universal model choice.
    *
    * The value is a `GenMap[Int64, SomeContractId]` where `SomeContractId` is either
    * `UniversalContractId(ContractId)` or `UniversalWithKeyContractId(ContractId)`.
    */
  def assertContractIdMapping(value: javaapi.data.Value): ContractIdMapping =
    value match {
      case genMap: javaapi.data.DamlGenMap =>
        genMap
          .stream()
          .iterator()
          .asScala
          .map[(ContractId, SomeContractId)] { entry =>
            val key = entry.getKey match {
              case i: javaapi.data.Int64 => i.getValue.toInt
              case k => throw new IllegalArgumentException(s"Expected Int64 key, got $k")
            }
            val value: SomeContractId = entry.getValue match {
              case v: javaapi.data.Variant if v.getConstructor == "UniversalContractId" =>
                val cid = v.getValue match {
                  case c: javaapi.data.ContractId => c.getValue
                  case other =>
                    throw new IllegalArgumentException(
                      s"Expected ContractId inside UniversalContractId, got $other"
                    )
                }
                UniversalContractId(V.ContractId.assertFromString(cid))
              case v: javaapi.data.Variant if v.getConstructor == "UniversalWithKeyContractId" =>
                val cid = v.getValue match {
                  case c: javaapi.data.ContractId => c.getValue
                  case other =>
                    throw new IllegalArgumentException(
                      s"Expected ContractId inside UniversalWithKeyContractId, got $other"
                    )
                }
                UniversalWithKeyContractId(V.ContractId.assertFromString(cid))
              case v =>
                throw new IllegalArgumentException(
                  s"Expected UniversalContractId or UniversalWithKeyContractId variant, got $v"
                )
            }
            key -> value
          }
          .toMap
      case _ =>
        throw new IllegalArgumentException(
          s"assertContractIdMapping: expected DamlGenMap, got $value"
        )
    }

  /** Extracts the contract ID mapping from a submitted Java API transaction by pairing the abstract
    * actions with the events in the transaction.
    *
    * For Create actions, the contract ID comes from the CreatedEvent. For Exercise/ExerciseByKey
    * actions, the mapping is parsed from the exercise result value.
    */
  def commandResultsToContractIdMapping(
      actions: List[Action],
      transaction: javaapi.data.Transaction,
  ): ContractIdMapping = {
    val eventsById = transaction.getEventsById
    val rootNodeIds = transaction.getRootNodeIds.asScala.toList
    actions
      .zip(rootNodeIds)
      .map { case (action, nodeId) =>
        val event = eventsById.get(nodeId)
        action match {
          case c: Create =>
            event match {
              case created: javaapi.data.CreatedEvent =>
                Map[ContractId, SomeContractId](
                  c.contractId -> UniversalContractId(
                    V.ContractId.assertFromString(created.getContractId)
                  )
                )
              case _ =>
                throw new IllegalArgumentException(
                  s"Expected CreatedEvent for Create action, got $event"
                )
            }
          case c: CreateWithKey =>
            event match {
              case created: javaapi.data.CreatedEvent =>
                Map[ContractId, SomeContractId](
                  c.contractId -> UniversalWithKeyContractId(
                    V.ContractId.assertFromString(created.getContractId)
                  )
                )
              case _ =>
                throw new IllegalArgumentException(
                  s"Expected CreatedEvent for CreateWithKey action, got $event"
                )
            }
          case _: Exercise =>
            event match {
              case exercised: javaapi.data.ExercisedEvent =>
                assertContractIdMapping(exercised.getExerciseResult)
              case _ =>
                throw new IllegalArgumentException(
                  s"Expected ExercisedEvent for Exercise action, got $event"
                )
            }
          case _: ExerciseByKey =>
            event match {
              case exercised: javaapi.data.ExercisedEvent =>
                assertContractIdMapping(exercised.getExerciseResult)
              case _ =>
                throw new IllegalArgumentException(
                  s"Expected ExercisedEvent for ExerciseByKey action, got $event"
                )
            }
          case _ =>
            throw new IllegalArgumentException(s"Unexpected action at command level: $action")
        }
      }
      .fold(Map.empty[ContractId, SomeContractId])(_ ++ _)
  }
}
